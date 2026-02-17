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
//! at execution time by [`ScalarSubqueryExec`] (in the `physical-plan` crate).
//! It always returns [`ColumnarValue::Scalar`], enabling downstream UDFs to
//! take their scalar fast paths.
//!
//! [`ScalarSubqueryExec`]: datafusion_physical_plan::scalar_subquery::ScalarSubqueryExec

use std::any::Any;
use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue, internal_datafusion_err};
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_expr_common::sort_properties::{ExprProperties, SortProperties};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// A physical expression whose value is provided by a scalar subquery.
///
/// The actual subquery execution and value population is handled by
/// `ScalarSubqueryExec`; this expression simply reads the cached result.
#[derive(Debug)]
pub struct ScalarSubqueryExpr {
    data_type: DataType,
    nullable: bool,
    /// Shared slot populated by `ScalarSubqueryExec` before any batch
    /// is evaluated.
    value: Arc<OnceLock<ScalarValue>>,
}

impl ScalarSubqueryExpr {
    pub fn new(
        data_type: DataType,
        nullable: bool,
        value: Arc<OnceLock<ScalarValue>>,
    ) -> Self {
        Self {
            data_type,
            nullable,
            value,
        }
    }
}

impl fmt::Display for ScalarSubqueryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.value.get() {
            Some(v) => write!(f, "scalar_subquery({v})"),
            None => write!(f, "scalar_subquery(<pending>)"),
        }
    }
}

// Identity-based hashing: two ScalarSubqueryExprs are the "same" if and only
// if they share the same OnceLock (i.e., they were created for the same
// subquery). This follows the DynamicFilterPhysicalExpr precedent.
impl Hash for ScalarSubqueryExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.value).hash(state);
    }
}

impl PartialEq for ScalarSubqueryExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.value, &other.value)
    }
}

impl Eq for ScalarSubqueryExpr {}

impl PhysicalExpr for ScalarSubqueryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.nullable)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.value.get().ok_or_else(|| {
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

    #[test]
    fn test_evaluate_with_value() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        let value = Arc::new(OnceLock::new());
        value.set(ScalarValue::Int32(Some(42))).unwrap();
        let expr = ScalarSubqueryExpr::new(DataType::Int32, false, value);

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
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap();

        let value = Arc::new(OnceLock::new());
        let expr = ScalarSubqueryExpr::new(DataType::Int32, false, value);

        let result = expr.evaluate(&batch);
        assert!(result.is_err());
    }

    #[test]
    fn test_identity_equality() {
        let v1 = Arc::new(OnceLock::new());
        let v2 = Arc::new(OnceLock::new());

        let e1a = ScalarSubqueryExpr::new(DataType::Int32, false, Arc::clone(&v1));
        let e1b = ScalarSubqueryExpr::new(DataType::Int32, false, Arc::clone(&v1));
        let e2 = ScalarSubqueryExpr::new(DataType::Int32, false, v2);

        assert_eq!(e1a, e1b);
        assert_ne!(e1a, e2);
    }
}
