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

//! Physical expression for uncorrelated scalar subqueries.
//!
//! [`ScalarSubqueryExpr`] reads a cached [`ScalarValue`] that is populated
//! at execution time by `ScalarSubqueryExec`.

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// A physical expression whose value is provided by a scalar subquery.
///
/// Subquery execution is handled by `ScalarSubqueryExec`, which stores the
/// result in a shared results container. This expression simply reads from the
/// shared results container at the appropriate index.
///
/// If the same subquery appears multiple times in a query, there will be
/// multiple `ScalarSubqueryExpr` with the same result index.
#[derive(Debug)]
pub struct ScalarSubqueryExpr {
    data_type: DataType,
    nullable: bool,
    /// Index of this subquery in the shared results container.
    index: usize,
    /// Shared results container populated by `ScalarSubqueryExec`.
    results: Arc<Vec<OnceLock<ScalarValue>>>,
}

impl ScalarSubqueryExpr {
    pub fn new(
        data_type: DataType,
        nullable: bool,
        index: usize,
        results: Arc<Vec<OnceLock<ScalarValue>>>,
    ) -> Self {
        Self {
            data_type,
            nullable,
            index,
            results,
        }
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    /// Returns the index of this subquery in the shared results container.
    pub fn index(&self) -> usize {
        self.index
    }

    pub fn results(&self) -> &Arc<Vec<OnceLock<ScalarValue>>> {
        &self.results
    }
}

impl fmt::Display for ScalarSubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.results.get(self.index).and_then(|slot| slot.get()) {
            Some(v) => write!(f, "scalar_subquery({v})"),
            None => write!(f, "scalar_subquery(<pending>)"),
        }
    }
}

// Two ScalarSubqueryExprs are the "same" if they share the same results
// container and have the same index.
impl Hash for ScalarSubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.results).hash(state);
        self.index.hash(state);
    }
}

impl PartialEq for ScalarSubqueryExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.results, &other.results) && self.index == other.index
    }
}

impl Eq for ScalarSubqueryExpr {}

impl PhysicalExpr for ScalarSubqueryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            format!("{self}"),
            self.data_type.clone(),
            self.nullable,
        )))
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self
            .results
            .get(self.index)
            .and_then(|slot| slot.get())
            .ok_or_else(|| {
                internal_datafusion_err!(
                    "ScalarSubqueryExpr evaluated before the subquery was executed"
                )
            })?;
        Ok(ColumnarValue::Scalar(value.clone()))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn get_properties(&self, _children: &[ExprProperties]) -> Result<ExprProperties> {
        Ok(ExprProperties::new_unknown().with_order(SortProperties::Singleton))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(scalar subquery)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::Field;

    fn make_results(values: Vec<Option<ScalarValue>>) -> Arc<Vec<OnceLock<ScalarValue>>> {
        Arc::new(
            values
                .into_iter()
                .map(|v| {
                    let lock = OnceLock::new();
                    if let Some(val) = v {
                        lock.set(val).unwrap();
                    }
                    lock
                })
                .collect(),
        )
    }

    #[test]
    fn test_evaluate_with_value() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        let results = make_results(vec![Some(ScalarValue::Int32(Some(42)))]);
        let expr = ScalarSubqueryExpr::new(DataType::Int32, false, 0, results);

        let result = expr.evaluate(&batch)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(42))) => {}
            other => panic!("Expected Scalar(Int32(42)), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_evaluate_before_populated() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![1]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        let results = Arc::new(vec![OnceLock::new()]);
        let expr = ScalarSubqueryExpr::new(DataType::Int32, false, 0, results);

        let result = expr.evaluate(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_identity_equality() {
        let results = make_results(vec![None, None]);

        let e1a =
            ScalarSubqueryExpr::new(DataType::Int32, false, 0, Arc::clone(&results));
        let e1b =
            ScalarSubqueryExpr::new(DataType::Int32, false, 0, Arc::clone(&results));
        let e2 = ScalarSubqueryExpr::new(DataType::Int32, false, 1, Arc::clone(&results));

        // Same container + same index → equal
        assert_eq!(e1a, e1b);
        // Same container, different index → not equal
        assert_ne!(e1a, e2);

        // Different container, same index → not equal
        let other_results = make_results(vec![None]);
        let e3 = ScalarSubqueryExpr::new(DataType::Int32, false, 0, other_results);
        assert_ne!(e1a, e3);
    }
}
