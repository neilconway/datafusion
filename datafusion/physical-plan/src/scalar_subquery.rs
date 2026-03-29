// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution plan for uncorrelated scalar subqueries.
//!
//! [`ScalarSubqueryExec`] wraps a main input plan and a set of subquery plans.
//! At execution time, it runs each subquery exactly once, extracts the scalar
//! result, and populates the shared results container that
//! [`ScalarSubqueryExpr`] instances read from by index.
//!
//! [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ScalarSubqueryResults;
use datafusion_physical_expr::PhysicalExpr;

use crate::execution_plan::{CardinalityEffect, ExecutionPlan, PlanProperties};
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};

use futures::StreamExt;
use futures::TryStreamExt;

/// Links a scalar subquery's execution plan to its index in the shared results
/// container. The [`ScalarSubqueryExec`] that owns these links populates
/// `results[index]` at execution time, and [`ScalarSubqueryExpr`] instances
/// with the same index read from it.
///
/// [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr
#[derive(Debug, Clone)]
pub struct ScalarSubqueryLink {
    /// The physical plan for the subquery.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Index into the shared results container.
    pub index: usize,
}

/// Manages execution of uncorrelated scalar subqueries for a single plan
/// level.
///
/// This node has an asymmetric set of children: the first child is the
/// **main input plan**, whose batches are passed through unchanged. The
/// remaining children are **subquery plans**, each of which must produce
/// exactly zero or one row. Before any batches from the main input are
/// yielded, all subquery plans are executed and their scalar results are
/// stored in a shared results container ([`ScalarSubqueryResults`]).
/// [`ScalarSubqueryExpr`] nodes embedded in the main input's expressions
/// read from this container by index.
///
/// All subqueries are evaluated eagerly when the first output partition is
/// requested, before any rows from the main input are produced.
///
/// TODO: Consider overlapping computation of the subqueries with evaluating the
/// main query.
///
/// TODO: Subqueries are evaluated sequentially. Consider parallel evaluation in
/// the future.
///
/// [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr
#[derive(Debug)]
pub struct ScalarSubqueryExec {
    /// The main input plan whose output is passed through.
    input: Arc<dyn ExecutionPlan>,
    /// Subquery plans and their result indexes.
    subqueries: Vec<ScalarSubqueryLink>,
    /// Ensures subqueries are executed exactly once, even when multiple
    /// partitions call `execute()` concurrently.
    subquery_barrier: Arc<tokio::sync::OnceCell<()>>,
    /// Shared results container; same instance held by ScalarSubqueryExpr nodes.
    results: ScalarSubqueryResults,
    /// Cached plan properties (copied from input).
    cache: Arc<PlanProperties>,
}

impl ScalarSubqueryExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        subqueries: Vec<ScalarSubqueryLink>,
        results: ScalarSubqueryResults,
    ) -> Self {
        let cache = Arc::clone(input.properties());
        Self {
            input,
            subqueries,
            subquery_barrier: Arc::new(tokio::sync::OnceCell::new()),
            results,
            cache,
        }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn subqueries(&self) -> &[ScalarSubqueryLink] {
        &self.subqueries
    }

    pub fn results(&self) -> &ScalarSubqueryResults {
        &self.results
    }

    /// Returns a per-child bool vec that is `true` for the main input
    /// (child 0) and `false` for every subquery child.
    fn true_for_input_only(&self) -> Vec<bool> {
        std::iter::once(true)
            .chain(std::iter::repeat_n(false, self.subqueries.len()))
            .collect()
    }
}

impl DisplayAs for ScalarSubqueryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "ScalarSubqueryExec: subqueries={}",
                    self.subqueries.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for ScalarSubqueryExec {
    fn name(&self) -> &'static str {
        "ScalarSubqueryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        let mut children = vec![&self.input];
        for sq in &self.subqueries {
            children.push(&sq.plan);
        }
        children
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // First child is the main input, the rest are subquery plans.
        let input = children.remove(0);
        let subqueries = self
            .subqueries
            .iter()
            .zip(children)
            .map(|(sq, new_plan)| ScalarSubqueryLink {
                plan: new_plan,
                index: sq.index,
            })
            .collect();
        Ok(Arc::new(ScalarSubqueryExec::new(
            input,
            subqueries,
            Arc::clone(&self.results),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let subqueries = self.subqueries.clone();
        let results = Arc::clone(&self.results);
        let barrier = Arc::clone(&self.subquery_barrier);
        let input = Arc::clone(&self.input);
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                // Execute all subqueries exactly once
                barrier
                    .get_or_try_init(|| async {
                        execute_subqueries(subqueries, results, context.clone())
                            .await
                            .map(|_| ())
                    })
                    .await?;

                // Now that the subqueries have been computed, we can safely
                // start the main input
                input.execute(partition, context)
            })
            .try_flatten(),
        )))
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Only the main input (first child); subquery children don't
        // contribute to ordering.
        self.true_for_input_only()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Only the main input; subquery children produce at most one
        // row, so repartitioning them adds overhead with no benefit.
        self.true_for_input_only()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Execute all subquery plans, extract their scalar results, and populate
/// the shared results container.
async fn execute_subqueries(
    subqueries: Vec<ScalarSubqueryLink>,
    results: ScalarSubqueryResults,
    context: Arc<TaskContext>,
) -> Result<Vec<ScalarValue>> {
    let mut values = Vec::with_capacity(subqueries.len());
    for sq in &subqueries {
        let value =
            execute_scalar_subquery(Arc::clone(&sq.plan), Arc::clone(&context)).await?;
        let _ = results[sq.index].set(value.clone());
        values.push(value);
    }
    Ok(values)
}

/// Execute a single subquery plan and extract the scalar value.
/// Returns NULL for 0 rows, the scalar value for exactly 1 row,
/// or an error for >1 rows.
async fn execute_scalar_subquery(
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
) -> Result<ScalarValue> {
    let schema = plan.schema();
    if schema.fields().len() != 1 {
        return internal_err!(
            "Scalar subquery must return exactly one column, got {}",
            schema.fields().len()
        );
    }

    let mut stream = crate::execute_stream(plan, context)?;

    let mut total_rows = 0usize;
    let mut result_value: Option<ScalarValue> = None;
    while let Some(batch) = stream.next().await.transpose()? {
        total_rows += batch.num_rows();
        if total_rows > 1 {
            return exec_err!(
                "Scalar subquery returned more than one row (got at least {total_rows})"
            );
        }
        if batch.num_rows() == 1 {
            result_value = Some(ScalarValue::try_from_array(batch.column(0), 0)?);
        }
    }

    // 0 rows → NULL of the appropriate type
    Ok(result_value.unwrap_or_else(|| {
        ScalarValue::try_from(schema.field(0).data_type()).unwrap_or(ScalarValue::Null)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{self, TestMemoryExec};

    use std::sync::OnceLock;

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    fn make_subquery_plan(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches[0].schema();
        TestMemoryExec::try_new_exec(&[batches], schema, None).unwrap()
    }

    fn make_results(n: usize) -> ScalarSubqueryResults {
        Arc::new((0..n).map(|_| OnceLock::new()).collect())
    }

    #[tokio::test]
    async fn test_single_row_subquery() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![42]))],
        )?;

        let results = make_results(1);
        let subquery_plan = make_subquery_plan(vec![batch]);
        let sq = ScalarSubqueryLink {
            plan: subquery_plan,
            index: 0,
        };

        let main_input = Arc::new(crate::placeholder_row::PlaceholderRowExec::new(
            test::aggr_test_schema(),
        ));
        let exec = ScalarSubqueryExec::new(main_input, vec![sq], Arc::clone(&results));

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx)?;
        let _batches = crate::common::collect(stream).await?;

        assert_eq!(results[0].get(), Some(&ScalarValue::Int32(Some(42))));
        Ok(())
    }

    #[tokio::test]
    async fn test_zero_row_subquery_returns_null() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))],
        )?;

        let results = make_results(1);
        let subquery_plan = make_subquery_plan(vec![batch]);
        let sq = ScalarSubqueryLink {
            plan: subquery_plan,
            index: 0,
        };

        let main_input = Arc::new(crate::placeholder_row::PlaceholderRowExec::new(
            test::aggr_test_schema(),
        ));
        let exec = ScalarSubqueryExec::new(main_input, vec![sq], Arc::clone(&results));

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx)?;
        let _batches = crate::common::collect(stream).await?;

        assert_eq!(results[0].get(), Some(&ScalarValue::Int64(None)));
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_row_subquery_errors() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let results = make_results(1);
        let subquery_plan = make_subquery_plan(vec![batch]);
        let sq = ScalarSubqueryLink {
            plan: subquery_plan,
            index: 0,
        };

        let main_input = Arc::new(crate::placeholder_row::PlaceholderRowExec::new(
            test::aggr_test_schema(),
        ));
        let exec = ScalarSubqueryExec::new(main_input, vec![sq], Arc::clone(&results));

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx)?;
        let result = crate::common::collect(stream).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("more than one row"),
            "Expected 'more than one row' error, got: {err_msg}"
        );
        Ok(())
    }
}
