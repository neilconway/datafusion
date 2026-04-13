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
//! result, and populates a shared [`ScalarSubqueryResults`] container that
//! [`ScalarSubqueryExpr`] instances hold directly and read from by index.
//!
//! [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr

use std::fmt;
use std::sync::Arc;

use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, ScalarValue, Statistics, exec_err, internal_err};
use datafusion_execution::TaskContext;
use datafusion_expr::execution_props::ScalarSubqueryResults;
use datafusion_physical_expr::PhysicalExpr;

use crate::execution_plan::{CardinalityEffect, ExecutionPlan, PlanProperties};
use crate::joins::utils::{OnceAsync, OnceFut};
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
/// From a query-results perspective, this node is a pass-through: it yields
/// the same batches as its main input and exists only to populate scalar
/// subquery results as a side effect before those batches are produced.
///
/// The first child node is the **main input plan**, whose batches are passed
/// through unchanged. The remaining children are **subquery plans**, each of
/// which must produce exactly zero or one row. Before any batches from the main
/// input are yielded, all subquery plans are executed and their scalar results
/// are stored in a shared [`ScalarSubqueryResults`] container owned by this
/// node. [`ScalarSubqueryExpr`] nodes embedded in the main input's expressions
/// hold the same container and read from it by index.
///
/// All subqueries are evaluated eagerly when the first output partition is
/// requested, before any rows from the main input are produced.
///
/// TODO: Consider overlapping computation of the subqueries with evaluating the
/// main query.
///
/// [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr
#[derive(Debug)]
pub struct ScalarSubqueryExec {
    /// The main input plan whose output is passed through.
    input: Arc<dyn ExecutionPlan>,
    /// Subquery plans and their result indexes.
    subqueries: Vec<ScalarSubqueryLink>,
    /// Shared one-time async computation of subquery results.
    subquery_future: Arc<OnceAsync<()>>,
    /// Shared results container; the same `Arc` is also held by the
    /// corresponding [`ScalarSubqueryExpr`] nodes in the input plan.
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
            subquery_future: Arc::default(),
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
        let subquery_ctx = Arc::clone(&context);
        let mut subquery_future = self.subquery_future.try_once(move || {
            Ok(async move { execute_subqueries(subqueries, results, subquery_ctx).await })
        })?;
        let input = Arc::clone(&self.input);
        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move {
                // Execute all subqueries exactly once, even when multiple
                // partitions call execute() concurrently.
                wait_for_subqueries(&mut subquery_future).await?;

                // Now that the subqueries have finished execution, we can
                // safely execute the main input
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

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Execute all subquery plans, extract their scalar results, and populate
/// the shared results container.
async fn wait_for_subqueries(fut: &mut OnceFut<()>) -> Result<()> {
    std::future::poll_fn(|cx| fut.get_shared(cx)).await?;
    Ok(())
}

async fn execute_subqueries(
    subqueries: Vec<ScalarSubqueryLink>,
    results: ScalarSubqueryResults,
    context: Arc<TaskContext>,
) -> Result<()> {
    // Evaluate subqueries in parallel; wait for them all to finish evaluation
    // before returning.
    let futures = subqueries.iter().map(|sq| {
        let plan = Arc::clone(&sq.plan);
        let ctx = Arc::clone(&context);
        let results = Arc::clone(&results);
        let index = sq.index;
        async move {
            let value = execute_scalar_subquery(plan, ctx).await?;
            if results[index].set(value).is_err() {
                return internal_err!(
                    "ScalarSubqueryExec: result for index {index} was already populated"
                );
            }
            Ok(()) as Result<()>
        }
    });
    futures::future::try_join_all(futures).await?;
    Ok(())
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
        // Should be enforced by the physical planner.
        return internal_err!(
            "Scalar subquery must return exactly one column, got {}",
            schema.fields().len()
        );
    }

    let mut stream = crate::execute_stream(plan, context)?;
    let mut result: Option<ScalarValue> = None;

    while let Some(batch) = stream.next().await.transpose()? {
        if batch.num_rows() == 0 {
            continue;
        }
        if result.is_some() || batch.num_rows() > 1 {
            return exec_err!("Scalar subquery returned more than one row");
        }
        result = Some(ScalarValue::try_from_array(batch.column(0), 0)?);
    }

    // 0 rows → typed NULL per SQL semantics
    match result {
        Some(v) => Ok(v),
        None => ScalarValue::try_from(schema.field(0).data_type()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{self, TestMemoryExec};

    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::test::exec::ErrorExec;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[derive(Debug)]
    struct CountingExec {
        inner: Arc<dyn ExecutionPlan>,
        execute_calls: Arc<AtomicUsize>,
    }

    impl CountingExec {
        fn new(inner: Arc<dyn ExecutionPlan>, execute_calls: Arc<AtomicUsize>) -> Self {
            Self {
                inner,
                execute_calls,
            }
        }
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(f, "CountingExec")
                }
                DisplayFormatType::TreeRender => write!(f, ""),
            }
        }
    }

    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            mut children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self::new(
                children.remove(0),
                Arc::clone(&self.execute_calls),
            )))
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.execute_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.execute(partition, context)
        }
    }

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

    #[tokio::test]
    async fn test_failed_subquery_is_not_retried() -> Result<()> {
        let execute_calls = Arc::new(AtomicUsize::new(0));
        let subquery_plan = Arc::new(CountingExec::new(
            Arc::new(ErrorExec::new()),
            Arc::clone(&execute_calls),
        ));
        let results = make_results(1);
        let sq = ScalarSubqueryLink {
            plan: subquery_plan,
            index: 0,
        };

        let main_input = Arc::new(crate::placeholder_row::PlaceholderRowExec::new(
            test::aggr_test_schema(),
        ));
        let exec = ScalarSubqueryExec::new(main_input, vec![sq], results);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, Arc::clone(&ctx))?;
        assert!(crate::common::collect(stream).await.is_err());

        let stream = exec.execute(0, ctx)?;
        assert!(crate::common::collect(stream).await.is_err());

        assert_eq!(execute_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
