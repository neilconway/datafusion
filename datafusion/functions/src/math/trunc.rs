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

use crate::utils::{calculate_binary_decimal_math, calculate_binary_math};

use arrow::array::ArrayRef;
use arrow::datatypes::DataType::{
    Decimal32, Decimal64, Decimal128, Decimal256, Float32, Float64,
};
use arrow::datatypes::{
    ArrowNativeTypeOp, DataType, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, DecimalType, Field, FieldRef, Float32Type, Float64Type, Int64Type,
};
use arrow::error::ArrowError;
use datafusion_common::types::{
    NativeType, logical_float32, logical_float64, logical_int64,
};
use datafusion_common::{Result, ScalarValue, exec_err, internal_err};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

fn output_scale_for_decimal(precision: u8, input_scale: i8, decimal_places: i64) -> i8 {
    if input_scale < 0 {
        let min_scale = -i64::from(precision);
        let new_scale = i64::from(input_scale).min(decimal_places).max(min_scale);
        return new_scale as i8;
    }

    i64::from(input_scale).min(decimal_places.max(0)) as i8
}

fn normalize_decimal_places_for_decimal(
    decimal_places: i64,
    precision: u8,
    scale: i8,
) -> Option<i64> {
    if decimal_places >= 0 {
        return Some(decimal_places);
    }

    let max_truncating_pow10 = i64::from(precision) - i64::from(scale);
    if max_truncating_pow10 <= 0 {
        return None;
    }

    let abs_decimal_places = decimal_places.unsigned_abs();
    (abs_decimal_places <= max_truncating_pow10 as u64).then_some(decimal_places)
}

fn calculate_new_precision_scale<T: DecimalType>(
    precision: u8,
    scale: i8,
    decimal_places: Option<i64>,
) -> Result<DataType> {
    let new_scale = decimal_places
        .map(|dp| output_scale_for_decimal(precision, scale, dp))
        .unwrap_or(scale);
    Ok(T::TYPE_CONSTRUCTOR(precision, new_scale))
}

fn decimal_places_from_scalar(scalar: &ScalarValue) -> Result<i64> {
    let out_of_range = |value: String| {
        datafusion_common::DataFusionError::Execution(format!(
            "trunc decimal_places {value} is out of supported i64 range"
        ))
    };
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::UInt64(Some(v)) => {
            i64::try_from(*v).map_err(|_| out_of_range(v.to_string()))
        }
        other => exec_err!(
            "Unexpected datatype for decimal_places: {}",
            other.data_type()
        ),
    }
}

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Truncates a number to a whole number or truncated to the specified decimal places.",
    syntax_example = "trunc(numeric_expression[, decimal_places])",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    argument(
        name = "decimal_places",
        description = r#"Optional. The number of decimal places to
  truncate to. Defaults to 0 (truncate to a whole number). If
  `decimal_places` is a positive integer, truncates digits to the
  right of the decimal point. If `decimal_places` is a negative
  integer, replaces digits to the left of the decimal point with `0`."#
    ),
    sql_example = r#"
  ```sql
  > SELECT trunc(42.738);
  +----------------+
  | trunc(42.738)  |
  +----------------+
  | 42             |
  +----------------+
  ```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TruncFunc {
    signature: Signature,
}

impl Default for TruncFunc {
    fn default() -> Self {
        TruncFunc::new()
    }
}

impl TruncFunc {
    pub fn new() -> Self {
        let decimal = Coercion::new_exact(TypeSignatureClass::Decimal);
        let decimal_places = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Integer],
            NativeType::Int64,
        );
        let float32 = Coercion::new_exact(TypeSignatureClass::Native(logical_float32()));
        let float64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        decimal.clone(),
                        decimal_places.clone(),
                    ]),
                    TypeSignature::Coercible(vec![decimal]),
                    TypeSignature::Coercible(vec![
                        float32.clone(),
                        decimal_places.clone(),
                    ]),
                    TypeSignature::Coercible(vec![float32]),
                    TypeSignature::Coercible(vec![float64.clone(), decimal_places]),
                    TypeSignature::Coercible(vec![float64]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TruncFunc {
    fn name(&self) -> &str {
        "trunc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let decimal_places = (arg_types.len() == 1).then_some(0);
        return_type_for_trunc(&arg_types[0], decimal_places)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let input_type = args.arg_fields[0].data_type();

        let decimal_places = match args.scalar_arguments.get(1) {
            None => Some(0),
            Some(None) => None,
            Some(Some(scalar)) if scalar.is_null() => None,
            Some(Some(scalar)) => Some(decimal_places_from_scalar(scalar)?),
        };

        let return_type = return_type_for_trunc(input_type, decimal_places)?;
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new(self.name(), return_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.arg_fields.iter().any(|a| a.data_type().is_null()) {
            return ColumnarValue::Scalar(ScalarValue::Null)
                .cast_to(args.return_type(), None);
        }

        let default_decimal_places = ColumnarValue::Scalar(ScalarValue::Int64(Some(0)));
        let decimal_places = if args.args.len() == 2 {
            &args.args[1]
        } else {
            &default_decimal_places
        };

        if let (ColumnarValue::Scalar(value_scalar), ColumnarValue::Scalar(dp_scalar)) =
            (&args.args[0], decimal_places)
        {
            if value_scalar.is_null() || dp_scalar.is_null() {
                return ColumnarValue::Scalar(ScalarValue::Null)
                    .cast_to(args.return_type(), None);
            }

            let dp = if let ScalarValue::Int64(Some(dp)) = dp_scalar {
                *dp
            } else {
                return internal_err!(
                    "Unexpected datatype for decimal_places: {}",
                    dp_scalar.data_type()
                );
            };

            match (value_scalar, args.return_type()) {
                (ScalarValue::Float32(Some(v)), _) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float32(Some(compute_truncate32(*v, dp))),
                )),
                (ScalarValue::Float64(Some(v)), _) => Ok(ColumnarValue::Scalar(
                    ScalarValue::Float64(Some(compute_truncate64(*v, dp))),
                )),
                (
                    ScalarValue::Decimal32(Some(v), in_precision, scale),
                    Decimal32(out_precision, out_scale),
                ) => {
                    let truncated =
                        trunc_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let scalar = ScalarValue::Decimal32(
                        Some(truncated),
                        *out_precision,
                        *out_scale,
                    );
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (
                    ScalarValue::Decimal64(Some(v), in_precision, scale),
                    Decimal64(out_precision, out_scale),
                ) => {
                    let truncated =
                        trunc_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let scalar = ScalarValue::Decimal64(
                        Some(truncated),
                        *out_precision,
                        *out_scale,
                    );
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (
                    ScalarValue::Decimal128(Some(v), in_precision, scale),
                    Decimal128(out_precision, out_scale),
                ) => {
                    let truncated =
                        trunc_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let scalar = ScalarValue::Decimal128(
                        Some(truncated),
                        *out_precision,
                        *out_scale,
                    );
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (
                    ScalarValue::Decimal256(Some(v), in_precision, scale),
                    Decimal256(out_precision, out_scale),
                ) => {
                    let truncated =
                        trunc_decimal_or_zero(*v, *in_precision, *scale, *out_scale, dp)?;
                    let scalar = ScalarValue::Decimal256(
                        Some(truncated),
                        *out_precision,
                        *out_scale,
                    );
                    Ok(ColumnarValue::Scalar(scalar))
                }
                (ScalarValue::Null, _) => ColumnarValue::Scalar(ScalarValue::Null)
                    .cast_to(args.return_type(), None),
                (value_scalar, return_type) => {
                    internal_err!(
                        "Unexpected datatype for trunc(value, decimal_places): value {}, return type {}",
                        value_scalar.data_type(),
                        return_type
                    )
                }
            }
        } else {
            trunc_columnar(
                &args.args[0],
                decimal_places,
                args.number_rows,
                args.return_type(),
            )
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // trunc preserves the order of the first argument
        let value = &input[0];
        let precision = input.get(1);

        if precision
            .map(|r| r.sort_properties.eq(&SortProperties::Singleton))
            .unwrap_or(true)
        {
            Ok(value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn return_type_for_trunc(
    input_type: &DataType,
    decimal_places: Option<i64>,
) -> Result<DataType> {
    match input_type {
        Float32 => Ok(Float32),
        Decimal32(precision, scale) => calculate_new_precision_scale::<Decimal32Type>(
            *precision,
            *scale,
            decimal_places,
        ),
        Decimal64(precision, scale) => calculate_new_precision_scale::<Decimal64Type>(
            *precision,
            *scale,
            decimal_places,
        ),
        Decimal128(precision, scale) => calculate_new_precision_scale::<Decimal128Type>(
            *precision,
            *scale,
            decimal_places,
        ),
        Decimal256(precision, scale) => calculate_new_precision_scale::<Decimal256Type>(
            *precision,
            *scale,
            decimal_places,
        ),
        _ => Ok(Float64),
    }
}

/// Truncate(numeric, decimalPrecision) and trunc(numeric) SQL function
#[cfg(test)]
fn trunc(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 1 && args.len() != 2 {
        return exec_err!(
            "truncate function requires one or two arguments, got {}",
            args.len()
        );
    }

    // If only one arg then invoke toolchain trunc(num) and precision = 0 by default
    // or then invoke the compute_truncate method to process precision
    let num = &args[0];
    let decimal_places = if args.len() == 1 {
        Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(0))))
    } else {
        Some(ColumnarValue::Array(Arc::clone(&args[1])))
    };
    let decimal_places_ref = decimal_places.as_ref().unwrap();
    let return_type =
        return_type_for_trunc(num.data_type(), (args.len() == 1).then_some(0))?;

    trunc_columnar(
        &ColumnarValue::Array(Arc::clone(num)),
        decimal_places_ref,
        num.len(),
        &return_type,
    )?
    .to_array(num.len())
}

fn trunc_columnar(
    value: &ColumnarValue,
    decimal_places: &ColumnarValue,
    number_rows: usize,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    let value_array = value.to_array(number_rows)?;
    let both_scalars = matches!(value, ColumnarValue::Scalar(_))
        && matches!(decimal_places, ColumnarValue::Scalar(_));

    let arr: ArrayRef = match (value_array.data_type(), return_type) {
        (Float64, _) => {
            let result = calculate_binary_math::<Float64Type, Int64Type, Float64Type, _>(
                value_array.as_ref(),
                decimal_places,
                |x, y| Ok(compute_truncate64(x, y)),
            )?;
            result as _
        }
        (Float32, _) => {
            let result = calculate_binary_math::<Float32Type, Int64Type, Float32Type, _>(
                value_array.as_ref(),
                decimal_places,
                |x, y| Ok(compute_truncate32(x, y)),
            )?;
            result as _
        }
        (Decimal32(input_precision, scale), Decimal32(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal32Type,
                Int64Type,
                Decimal32Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    trunc_decimal_or_zero(v, *input_precision, *scale, *new_scale, dp)
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (Decimal64(input_precision, scale), Decimal64(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal64Type,
                Int64Type,
                Decimal64Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    trunc_decimal_or_zero(v, *input_precision, *scale, *new_scale, dp)
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (Decimal128(input_precision, scale), Decimal128(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal128Type,
                Int64Type,
                Decimal128Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    trunc_decimal_or_zero(v, *input_precision, *scale, *new_scale, dp)
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (Decimal256(input_precision, scale), Decimal256(precision, new_scale)) => {
            let result = calculate_binary_decimal_math::<
                Decimal256Type,
                Int64Type,
                Decimal256Type,
                _,
            >(
                value_array.as_ref(),
                decimal_places,
                |v, dp| {
                    trunc_decimal_or_zero(v, *input_precision, *scale, *new_scale, dp)
                },
                *precision,
                *new_scale,
            )?;
            result as _
        }
        (other, _) => exec_err!("Unsupported data type {other:?} for function trunc")?,
    };

    if both_scalars {
        ScalarValue::try_from_array(&arr, 0).map(ColumnarValue::Scalar)
    } else {
        Ok(ColumnarValue::Array(arr))
    }
}

fn compute_truncate32(x: f32, y: i64) -> f32 {
    if x == 0_f32 {
        return 0_f32;
    }
    let factor = 10.0_f32.powi(y as i32);
    (x * factor).trunc() / factor
}

fn compute_truncate64(x: f64, y: i64) -> f64 {
    if x == 0_f64 {
        return 0_f64;
    }
    let factor = 10.0_f64.powi(y as i32);
    (x * factor).trunc() / factor
}

fn trunc_decimal<V: ArrowNativeTypeOp>(
    value: V,
    input_scale: i8,
    output_scale: i8,
    decimal_places: i64,
) -> Result<V, ArrowError> {
    let diff = i64::from(input_scale) - decimal_places;
    if diff <= 0 {
        return Ok(value);
    }

    debug_assert!(diff <= i64::from(u32::MAX));
    let diff = diff as u32;

    let ten = V::from_usize(10).ok_or_else(|| {
        ArrowError::ComputeError("Internal error: could not create constant 10".into())
    })?;

    let factor = ten.pow_checked(diff).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Overflow while truncating decimal with scale {input_scale} and decimal places {decimal_places}"
        ))
    })?;

    let quotient = value.div_wrapping(factor);

    let scale_shift = i64::from(output_scale) - decimal_places;
    if scale_shift == 0 {
        return Ok(quotient);
    }

    debug_assert!(scale_shift > 0);
    debug_assert!(scale_shift <= i64::from(u32::MAX));
    let scale_shift = scale_shift as u32;
    let shift_factor = ten.pow_checked(scale_shift).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Overflow while truncating decimal with scale {input_scale} and decimal places {decimal_places}"
        ))
    })?;
    quotient
        .mul_checked(shift_factor)
        .map_err(|_| ArrowError::ComputeError("Overflow while truncating decimal".into()))
}

fn trunc_decimal_or_zero<V: ArrowNativeTypeOp>(
    value: V,
    precision: u8,
    input_scale: i8,
    output_scale: i8,
    decimal_places: i64,
) -> Result<V, ArrowError> {
    if let Some(dp) =
        normalize_decimal_places_for_decimal(decimal_places, precision, input_scale)
    {
        trunc_decimal(value, input_scale, output_scale, dp)
    } else {
        V::from_usize(0).ok_or_else(|| {
            ArrowError::ComputeError("Internal error: could not create constant 0".into())
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::trunc::trunc;

    use arrow::array::{
        ArrayRef, AsArray, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
        Int64Array,
    };
    use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type};
    use arrow_buffer::i256;
    use datafusion_common::cast::{as_float32_array, as_float64_array};

    #[test]
    fn test_truncate_32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                15.0,
                1_234.267_8,
                1_233.123_4,
                3.312_979_2,
                -21.123_4,
            ])),
            Arc::new(Int64Array::from(vec![0, 3, 2, 5, 6])),
        ];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float32_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 15.0);
        assert_eq!(floats.value(1), 1_234.267);
        assert_eq!(floats.value(2), 1_233.12);
        assert_eq!(floats.value(3), 3.312_97);
        assert_eq!(floats.value(4), -21.123_4);
    }

    #[test]
    fn test_truncate_64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![
                5.0,
                234.267_812_176,
                123.123_456_789,
                123.312_979_313_2,
                -321.123_1,
            ])),
            Arc::new(Int64Array::from(vec![0, 3, 2, 5, 6])),
        ];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float64_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 5.0);
        assert_eq!(floats.value(1), 234.267);
        assert_eq!(floats.value(2), 123.12);
        assert_eq!(floats.value(3), 123.312_97);
        assert_eq!(floats.value(4), -321.123_1);
    }

    #[test]
    fn test_truncate_64_one_arg() {
        let args: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            5.0,
            234.267_812,
            123.123_45,
            123.312_979_313_2,
            -321.123,
        ]))];

        let result = trunc(&args).expect("failed to initialize function truncate");
        let floats =
            as_float64_array(&result).expect("failed to initialize function truncate");

        assert_eq!(floats.len(), 5);
        assert_eq!(floats.value(0), 5.0);
        assert_eq!(floats.value(1), 234.0);
        assert_eq!(floats.value(2), 123.0);
        assert_eq!(floats.value(3), 123.0);
        assert_eq!(floats.value(4), -321.0);
    }

    #[test]
    fn test_truncate_decimal128_exact_large_integer() {
        let values = Decimal128Array::from(vec![
            Some(9_007_199_254_740_993_i128),
            Some(-9_007_199_254_740_993_i128),
        ])
        .with_precision_and_scale(20, 0)
        .unwrap();
        let args: Vec<ArrayRef> = vec![Arc::new(values)];

        let result = trunc(&args).expect("failed to initialize function truncate");
        assert_eq!(result.data_type(), &DataType::Decimal128(20, 0));

        let decimals = result.as_primitive::<Decimal128Type>();
        assert_eq!(decimals.value(0), 9_007_199_254_740_993_i128);
        assert_eq!(decimals.value(1), -9_007_199_254_740_993_i128);
    }

    #[test]
    fn test_truncate_decimal256_one_arg() {
        let values = Decimal256Array::from(vec![
            Some(i256::from(1_234_567)),
            Some(i256::from(-1_234_567)),
        ])
        .with_precision_and_scale(50, 4)
        .unwrap();
        let args: Vec<ArrayRef> = vec![Arc::new(values)];

        let result = trunc(&args).expect("failed to initialize function truncate");
        assert_eq!(result.data_type(), &DataType::Decimal256(50, 0));

        let decimals = result.as_primitive::<Decimal256Type>();
        assert_eq!(decimals.value(0), i256::from(123));
        assert_eq!(decimals.value(1), i256::from(-123));
    }
}
