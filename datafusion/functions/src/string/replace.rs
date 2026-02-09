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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, GenericStringBuilder, OffsetSizeTrait, StringArrayType};
use arrow::datatypes::DataType;

use crate::utils::{make_scalar_function, utf8_to_str_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;
#[user_doc(
    doc_section(label = "String Functions"),
    description = "Replaces all occurrences of a specified substring in a string with a new substring.",
    syntax_example = "replace(str, substr, replacement)",
    sql_example = r#"```sql
> select replace('ABabbaBA', 'ab', 'cd');
+-------------------------------------------------+
| replace(Utf8("ABabbaBA"),Utf8("ab"),Utf8("cd")) |
+-------------------------------------------------+
| ABcdbaBA                                        |
+-------------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(
        name = "substr",
        prefix = "Substring expression to replace in the input string. Substring"
    ),
    standard_argument(name = "replacement", prefix = "Replacement substring")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReplaceFunc {
    signature: Signature,
}

impl Default for ReplaceFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReplaceFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "replace"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .and_then(|dt| string_coercion(&dt, &arg_types[2]))
            .or_else(|| {
                binary_to_string_coercion(&arg_types[0], &arg_types[1])
                    .and_then(|dt| binary_to_string_coercion(&dt, &arg_types[2]))
            })
        {
            utf8_to_str_type(&coercion_data_type, "replace")
        } else {
            exec_err!(
                "Unsupported data types for replace. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let number_rows = args.number_rows;
        let return_type = args.return_type().clone();
        let [string_arg, from_arg, to_arg] =
            take_function_args(self.name(), args.args)?;

        // Try to extract scalar from/to strings (the common case: literal arguments)
        let from_scalar = extract_scalar_str(&from_arg);
        let to_scalar = extract_scalar_str(&to_arg);

        match (&string_arg, from_scalar, to_scalar) {
            // All three are scalar
            (ColumnarValue::Scalar(string_sv), Some(from_opt), Some(to_opt)) => {
                match (string_sv.try_as_str(), from_opt, to_opt) {
                    (Some(Some(s)), Some(from), Some(to)) => {
                        let mut buf = String::new();
                        replace_into_string(&mut buf, s, &from, &to);
                        Ok(ColumnarValue::Scalar(make_scalar_value(
                            buf,
                            &return_type,
                        )?))
                    }
                    // Any null scalar → null result
                    _ => Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                        &return_type,
                    )?)),
                }
            }

            // String is array, from/to are scalar non-null → fast path
            (ColumnarValue::Array(string_arr), Some(Some(from)), Some(Some(to))) => {
                replace_scalar_from_to(string_arr, &from, &to, &return_type)
            }

            // from or to is a null scalar → entire result is null
            (_, Some(None), _) | (_, _, Some(None)) => {
                let string_arr = string_arg.to_array(number_rows)?;
                let null_scalar = ScalarValue::try_from(&return_type)?;
                Ok(ColumnarValue::Array(null_scalar.to_array_of_size(
                    string_arr.len(),
                )?))
            }

            // Fallback: from and/or to are arrays; use existing array-based impl
            _ => {
                let converted_args =
                    coerce_args(&[string_arg, from_arg, to_arg])?;
                let coercion_type = converted_args[0].data_type();
                match coercion_type {
                    DataType::Utf8 => make_scalar_function(
                        replace::<i32>,
                        vec![],
                    )(&converted_args),
                    DataType::LargeUtf8 => make_scalar_function(
                        replace::<i64>,
                        vec![],
                    )(&converted_args),
                    DataType::Utf8View => {
                        make_scalar_function(replace_view, vec![])(&converted_args)
                    }
                    other => exec_err!(
                        "Unsupported coercion data type {other:?} for function replace"
                    ),
                }
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Extract a scalar string value from a ColumnarValue.
/// Returns `Some(Some(string))` for non-null scalar strings,
/// `Some(None)` for null scalar strings, and `None` for arrays.
fn extract_scalar_str(cv: &ColumnarValue) -> Option<Option<String>> {
    match cv {
        ColumnarValue::Scalar(s) => match s.try_as_str() {
            Some(Some(v)) => Some(Some(v.to_owned())),
            Some(None) => Some(None),
            None => None,
        },
        ColumnarValue::Array(_) => None,
    }
}

/// Create a ScalarValue of the appropriate string type.
fn make_scalar_value(s: String, data_type: &DataType) -> Result<ScalarValue> {
    Ok(match data_type {
        DataType::Utf8 => ScalarValue::Utf8(Some(s)),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(Some(s)),
        other => {
            return exec_err!(
                "Unsupported return type {other:?} for function replace"
            )
        }
    })
}

/// Coerce arguments to a common string type for the fallback array path.
fn coerce_args(args: &[ColumnarValue]) -> Result<Vec<ColumnarValue>> {
    let data_types: Vec<_> = args.iter().map(|a| a.data_type()).collect();
    let coercion_type = string_coercion(&data_types[0], &data_types[1])
        .and_then(|dt| string_coercion(&dt, &data_types[2]))
        .or_else(|| {
            binary_to_string_coercion(&data_types[0], &data_types[1])
                .and_then(|dt| binary_to_string_coercion(&dt, &data_types[2]))
        });

    match coercion_type {
        Some(ct) => {
            let mut converted = Vec::with_capacity(args.len());
            for arg in args {
                if arg.data_type() == ct {
                    converted.push(arg.clone());
                } else {
                    converted.push(arg.cast_to(&ct, None)?);
                }
            }
            Ok(converted)
        }
        None => exec_err!(
            "Unsupported data type {}, {:?}, {:?} for function replace.",
            data_types[0],
            data_types[1],
            data_types[2]
        ),
    }
}

/// Fast path for `replace(string_array, scalar_from, scalar_to)`.
/// Iterates only the string array, avoiding expansion of scalar from/to into arrays.
fn replace_scalar_from_to(
    string_arr: &ArrayRef,
    from: &str,
    to: &str,
    return_type: &DataType,
) -> Result<ColumnarValue> {
    match string_arr.data_type() {
        DataType::Utf8 => {
            let arr = as_generic_string_array::<i32>(string_arr)?;
            Ok(ColumnarValue::Array(replace_scalar_from_to_iter::<_, i32>(
                arr, from, to,
            )))
        }
        DataType::LargeUtf8 => {
            let arr = as_generic_string_array::<i64>(string_arr)?;
            Ok(ColumnarValue::Array(replace_scalar_from_to_iter::<_, i64>(
                arr, from, to,
            )))
        }
        DataType::Utf8View => {
            // Utf8View input → Utf8 output (i32 offsets)
            let arr = as_string_view_array(string_arr)?;
            Ok(ColumnarValue::Array(replace_scalar_from_to_iter::<_, i32>(
                arr, from, to,
            )))
        }
        other => {
            exec_err!(
                "Unsupported string array type {other:?} for function replace; expected {return_type:?}"
            )
        }
    }
}

/// Core loop for scalar from/to replacement over any StringArrayType,
/// parameterized on the output offset size (i32 for Utf8, i64 for LargeUtf8).
#[expect(clippy::needless_pass_by_value)] // S is instantiated as a reference type
fn replace_scalar_from_to_iter<'a, S: StringArrayType<'a>, O: OffsetSizeTrait>(
    string_array: S,
    from: &str,
    to: &str,
) -> ArrayRef {
    let mut builder = GenericStringBuilder::<O>::new();
    let mut buffer = String::new();

    for val in string_array.iter() {
        match val {
            Some(s) => {
                buffer.clear();
                replace_into_string(&mut buffer, s, from, to);
                builder.append_value(&buffer);
            }
            None => builder.append_null(),
        }
    }

    Arc::new(builder.finish())
}

fn replace_view(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_string_view_array(&args[0])?;
    let from_array = as_string_view_array(&args[1])?;
    let to_array = as_string_view_array(&args[2])?;

    let mut builder = GenericStringBuilder::<i32>::new();
    let mut buffer = String::new();

    for ((string, from), to) in string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
    {
        match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                buffer.clear();
                replace_into_string(&mut buffer, string, from, to);
                builder.append_value(&buffer);
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Replaces all occurrences in string of substring from with substring to.
/// replace('abcdefabcdef', 'cd', 'XX') = 'abXXefabXXef'
fn replace<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;
    let from_array = as_generic_string_array::<T>(&args[1])?;
    let to_array = as_generic_string_array::<T>(&args[2])?;

    let mut builder = GenericStringBuilder::<T>::new();
    let mut buffer = String::new();

    for ((string, from), to) in string_array
        .iter()
        .zip(from_array.iter())
        .zip(to_array.iter())
    {
        match (string, from, to) {
            (Some(string), Some(from), Some(to)) => {
                buffer.clear();
                replace_into_string(&mut buffer, string, from, to);
                builder.append_value(&buffer);
            }
            _ => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

/// Helper function to perform string replacement into a reusable String buffer
#[inline]
fn replace_into_string(buffer: &mut String, string: &str, from: &str, to: &str) {
    if from.is_empty() {
        // When from is empty, insert 'to' at the beginning, between each character, and at the end
        // This matches the behavior of str::replace()
        buffer.push_str(to);
        for ch in string.chars() {
            buffer.push(ch);
            buffer.push_str(to);
        }
        return;
    }

    // Fast path for replacing a single ASCII character with another single ASCII character
    // This matches Rust's str::replace() optimization and enables vectorization
    if let ([from_byte], [to_byte]) = (from.as_bytes(), to.as_bytes())
        && from_byte.is_ascii()
        && to_byte.is_ascii()
    {
        // SAFETY: We're replacing ASCII with ASCII, which preserves UTF-8 validity
        let replaced: Vec<u8> = string
            .as_bytes()
            .iter()
            .map(|b| if *b == *from_byte { *to_byte } else { *b })
            .collect();
        buffer.push_str(unsafe { std::str::from_utf8_unchecked(&replaced) });
        return;
    }

    let mut last_end = 0;
    for (start, _part) in string.match_indices(from) {
        buffer.push_str(&string[last_end..start]);
        buffer.push_str(to);
        last_end = start + from.len();
    }
    buffer.push_str(&string[last_end..]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::test::test_function;
    use arrow::array::Array;
    use arrow::array::LargeStringArray;
    use arrow::array::StringArray;
    use arrow::datatypes::DataType::{LargeUtf8, Utf8};
    use datafusion_common::ScalarValue;
    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("aabbdqcbb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("bb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("ccc")))),
            ],
            Ok(Some("aacccdqcccc")),
            &str,
            Utf8,
            StringArray
        );

        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from(
                    "aabbb"
                )))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("bbb")))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from("cc")))),
            ],
            Ok(Some("aacc")),
            &str,
            LargeUtf8,
            LargeStringArray
        );

        test_function!(
            ReplaceFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "aabbbcw"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("bb")))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("cc")))),
            ],
            Ok(Some("aaccbcw")),
            &str,
            Utf8,
            StringArray
        );

        Ok(())
    }
}
