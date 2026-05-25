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

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, PrimitiveArray};
use arrow::datatypes::{ArrowNativeTypeOp, DecimalType};
use arrow::error::ArrowError;
use arrow_buffer::ArrowNativeType;
use datafusion_common::{DataFusionError, Result};

pub(super) fn apply_decimal_to_integral_op<T, F>(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    fn_name: &str,
    op: F,
) -> Result<ArrayRef>
where
    T: DecimalType,
    T::Native: ArrowNativeType + ArrowNativeTypeOp,
    F: Fn(T::Native, T::Native, &str) -> std::result::Result<T::Native, ArrowError>,
{
    if scale <= 0 {
        return Ok(Arc::clone(array));
    }

    let factor = decimal_scale_factor::<T>(scale, fn_name)?;
    let decimal = array.as_primitive::<T>();
    let output_scale = 0;
    let data_type = T::TYPE_CONSTRUCTOR(precision, output_scale);

    let result: PrimitiveArray<T> = decimal.try_unary(|value| {
        let new_value = op(value, factor, fn_name)?;
        T::validate_decimal_precision(new_value, precision, output_scale).map_err(
            |_| {
                ArrowError::ComputeError(format!(
                    "Decimal overflow while applying {fn_name}"
                ))
            },
        )?;
        Ok::<_, ArrowError>(new_value)
    })?;

    let result = result.with_data_type(data_type);

    Ok(Arc::new(result))
}

fn decimal_scale_factor<T>(scale: i8, fn_name: &str) -> Result<T::Native>
where
    T: DecimalType,
    T::Native: ArrowNativeType + ArrowNativeTypeOp,
{
    let base = <T::Native as ArrowNativeType>::from_usize(10).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Cannot get 10_{} from usize: {:?}",
            std::any::type_name::<T::Native>(),
            10_usize
        ))
    })?;

    base.pow_checked(scale as u32).map_err(|_| {
        DataFusionError::Execution(format!("Decimal overflow while applying {fn_name}"))
    })
}

pub(super) fn ceil_decimal_value<T>(
    value: T,
    factor: T,
    fn_name: &str,
) -> std::result::Result<T, ArrowError>
where
    T: ArrowNativeTypeOp,
{
    let quotient = value.div_wrapping(factor);
    let remainder = value.mod_wrapping(factor);

    if remainder == T::ZERO {
        return Ok(quotient);
    }

    if value >= T::ZERO {
        quotient.add_checked(T::ONE).map_err(|_| {
            ArrowError::ComputeError(format!("Decimal overflow while applying {fn_name}"))
        })
    } else {
        Ok(quotient)
    }
}

pub(super) fn floor_decimal_value<T>(
    value: T,
    factor: T,
    fn_name: &str,
) -> std::result::Result<T, ArrowError>
where
    T: ArrowNativeTypeOp,
{
    let quotient = value.div_wrapping(factor);
    let remainder = value.mod_wrapping(factor);

    if remainder == T::ZERO {
        return Ok(quotient);
    }

    if value >= T::ZERO {
        Ok(quotient)
    } else {
        quotient.sub_checked(T::ONE).map_err(|_| {
            ArrowError::ComputeError(format!("Decimal overflow while applying {fn_name}"))
        })
    }
}
