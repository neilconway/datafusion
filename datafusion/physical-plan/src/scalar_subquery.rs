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
//! At execution time, it runs each subquery exactly once (even across multiple
//! output partitions), extracts the scalar result, and populates the shared
//! [`OnceLock`] slots that [`ScalarSubqueryExpr`] instances read from.
//!
//! [`ScalarSubqueryExpr`]: datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr

use std::any::Any;
use std::fmt;
use std::sync::{Arc, OnceLock};

use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_execution::TaskContext;

use crate::execution_plan::{CardinalityEffect, ExecutionPlan, PlanProperties};
use crate::joins::utils::OnceAsync;
use crate::stream::RecordBatchStreamAdapter;
use crate::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};

use futures::stream::StreamExt;

/// A subquery plan paired with the shared slot it should populate.
#[derive(Debug, Clone)]
pub struct SubqueryPlan {
    /// The physical plan for the subquery.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Shared slot populated with the scalar result at execution time.
    pub value: Arc<OnceLock<ScalarValue>>,
}

/// Executes subquery plans once and populates shared value slots before
/// yielding batches from the main input.
#[derive(Debug)]
pub struct ScalarSubqueryExec {
    /// The main input plan whose output is passed through.
    input: Arc<dyn ExecutionPlan>,
    /// Subquery plans and their shared value slots.
    subqueries: Vec<SubqueryPlan>,
    /// Shared one-time async computation of subquery results.
    subquery_results: Arc<OnceAsync<Vec<ScalarValue>>>,
    /// Cached plan properties (copied from input).
    cache: PlanProperties,
}

impl ScalarSubqueryExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        subqueries: Vec<SubqueryPlan>,
    ) -> Self {
        let cache = input.properties().clone();
        Self {
            input,
            subqueries,
            subquery_results: Arc::default(),
            cache,
        }
    }

    /// Returns the main input plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Returns the subquery plans.
    pub fn subqueries(&self) -> &[SubqueryPlan] {
        &self.subqueries
    }
}

impl DisplayAs for ScalarSubqueryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ScalarSubqueryExec: subqueries={}", self.subqueries.len())
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

    fn properties(&self) -> &PlanProperties {
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
            .map(|(sq, new_plan)| SubqueryPlan {
                plan: new_plan,
                value: Arc::clone(&sq.value),
            })
            .collect();
        Ok(Arc::new(ScalarSubqueryExec::new(input, subqueries)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let subqueries = self.subqueries.clone();
        let ctx = Arc::clone(&context);

        // Use OnceAsync to ensure all subqueries are executed exactly once,
        // even when multiple partitions call execute() concurrently.
        let mut once_fut = self.subquery_results.try_once(move || {
            Ok(async move {
                execute_subqueries(subqueries, ctx).await
            })
        })?;

        let input = self.input.execute(partition, context)?;
        let schema = input.schema();

        // Create a stream that first waits for subquery results, then
        // delegates to the inner input stream.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(async move {
                // Wait for subquery execution to complete.
                let result: Result<()> = std::future::poll_fn(|cx| {
                    once_fut.get(cx).map(|r| r.map(|_| ()))
                })
                .await;
                result
            })
            .filter_map(|result| async move {
                match result {
                    Ok(()) => None, // Subqueries done, proceed to input
                    Err(e) => Some(Err(e)), // Propagate error
                }
            })
            .chain(input),
        )))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // The first child is the main input, whose order is maintained.
        // Subquery children don't contribute to ordering.
        let mut v = vec![true];
        v.extend(self.subqueries.iter().map(|_| false));
        v
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Execute all subquery plans, extract their scalar results, and populate
/// the shared `OnceLock` slots.
async fn execute_subqueries(
    subqueries: Vec<SubqueryPlan>,
    context: Arc<TaskContext>,
) -> Result<Vec<ScalarValue>> {
    let mut results = Vec::with_capacity(subqueries.len());
    for sq in &subqueries {
        let value = execute_scalar_subquery(Arc::clone(&sq.plan), Arc::clone(&context)).await?;
        // Populate the shared OnceLock. It's fine if another thread beat us
        // (the OnceAsync ensures this function runs only once, but this is
        // defensive).
        let _ = sq.value.set(value.clone());
        results.push(value);
    }
    Ok(results)
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

    let stream = crate::execute_stream(plan, context)?;
    let batches = crate::common::collect(stream).await?;

    let mut total_rows = 0usize;
    let mut result_value: Option<ScalarValue> = None;
    for batch in &batches {
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

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::OnceLock;

    fn make_subquery_plan(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
        let schema = batches[0].schema();
        TestMemoryExec::try_new_exec(&[batches], schema, None).unwrap()
    }

    #[tokio::test]
    async fn test_single_row_subquery() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![42]))],
        )?;

        let subquery_plan = make_subquery_plan(vec![batch]);
        let value = Arc::new(OnceLock::new());
        let sq = SubqueryPlan {
            plan: subquery_plan,
            value: Arc::clone(&value),
        };

        // Use a PlaceholderRowExec as the main input
        let main_input = Arc::new(
            crate::placeholder_row::PlaceholderRowExec::new(test::aggr_test_schema()),
        );
        let exec = ScalarSubqueryExec::new(main_input, vec![sq]);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx)?;
        let _batches = crate::common::collect(stream).await?;

        // The OnceLock should now be populated
        assert_eq!(value.get(), Some(&ScalarValue::Int32(Some(42))));
        Ok(())
    }

    #[tokio::test]
    async fn test_zero_row_subquery_returns_null() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(vec![] as Vec<i64>))])?;

        let subquery_plan = make_subquery_plan(vec![batch]);
        let value = Arc::new(OnceLock::new());
        let sq = SubqueryPlan {
            plan: subquery_plan,
            value: Arc::clone(&value),
        };

        let main_input = Arc::new(
            crate::placeholder_row::PlaceholderRowExec::new(test::aggr_test_schema()),
        );
        let exec = ScalarSubqueryExec::new(main_input, vec![sq]);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx)?;
        let _batches = crate::common::collect(stream).await?;

        assert_eq!(value.get(), Some(&ScalarValue::Int64(None)));
        Ok(())
    }

    #[tokio::test]
    async fn test_multi_row_subquery_errors() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;

        let subquery_plan = make_subquery_plan(vec![batch]);
        let value = Arc::new(OnceLock::new());
        let sq = SubqueryPlan {
            plan: subquery_plan,
            value: Arc::clone(&value),
        };

        let main_input = Arc::new(
            crate::placeholder_row::PlaceholderRowExec::new(test::aggr_test_schema()),
        );
        let exec = ScalarSubqueryExec::new(main_input, vec![sq]);

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
