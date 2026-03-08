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

use crate::strings::make_and_append_view;
use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayIter, ArrayRef, AsArray, NullBufferBuilder, StringArrayType,
    StringViewArray, StringViewBuilder,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::DataType;
use datafusion_common::cast::as_int64_array;
use datafusion_common::types::{
    NativeType, logical_int32, logical_int64, logical_string,
};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarUDFImpl, Signature, TypeSignature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Extracts a substring of a specified number of characters from a specific starting position in a string.",
    syntax_example = "substr(str, start_pos[, length])",
    alternative_syntax = "substring(str from start_pos for length)",
    sql_example = r#"```sql
> select substr('datafusion', 5, 3);
+----------------------------------------------+
| substr(Utf8("datafusion"),Int64(5),Int64(3)) |
+----------------------------------------------+
| fus                                          |
+----------------------------------------------+
```"#,
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "start_pos",
        description = "Character position to start the substring at. The first character in the string has a position of 1. If the start position is less than 1, it is treated as if it is before the start of the string and the (absolute) number of characters before position 1 is subtracted from `length` (if given). For example, `substr('abc', -3, 6)` returns `'ab'`."
    ),
    argument(
        name = "length",
        description = "Number of characters to extract. If not specified, returns the rest of the string after the start position."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SubstrFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SubstrFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl SubstrFunc {
    pub fn new() -> Self {
        let string = Coercion::new_exact(TypeSignatureClass::Native(logical_string()));
        let int64 = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_int64()),
            vec![TypeSignatureClass::Native(logical_int32())],
            NativeType::Int64,
        );
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![string.clone(), int64.clone()]),
                    TypeSignature::Coercible(vec![
                        string.clone(),
                        int64.clone(),
                        int64.clone(),
                    ]),
                ],
                Volatility::Immutable,
            )
            .with_parameter_names(vec![
                "str".to_string(),
                "start_pos".to_string(),
                "length".to_string(),
            ])
            .expect("valid parameter names"),
            aliases: vec![String::from("substring")],
        }
    }
}

impl ScalarUDFImpl for SubstrFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "substr"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    // `SubstrFunc` always generates `Utf8View` output for its efficiency.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        // Fast path for array string with scalar start/count
        if let Some(result) = try_substr_scalar_args(&args.args) {
            return result.map(ColumnarValue::Array);
        }
        make_scalar_function(substr, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Fallback substr implementation for when start and/or count are arrays.
fn substr(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            string_substr::<_>(string_array, &args[1..])
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            string_substr::<_>(string_array, &args[1..])
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            string_view_substr(string_array, &args[1..])
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function substr,\
            expected Utf8View, Utf8 or LargeUtf8."
        ),
    }
}

// Convert the given `start` and `count` to valid byte indices within `input` string
//
// Input `start` and `count` are equivalent to PostgreSQL's `substr(s, start, count)`
// `start` is 1-based, if `count` is not provided count to the end of the string.
// Input indices are character-based, and return values are byte indices.
// The input bounds can be outside string bounds, this function will return
// the intersection between input bounds and valid string bounds.
// `input_ascii_only` is used to optimize this function if `input` is ASCII-only.
//
// * Example
// 'Hi🌏' in-mem (`[]` for one char, `x` for one byte): [x][x][xxxx]
// `get_true_start_end('Hi🌏', 1, None) -> (0, 6)`
// `get_true_start_end('Hi🌏', 1, 1) -> (0, 1)`
// `get_true_start_end('Hi🌏', -10, 2) -> (0, 0)`
pub fn get_true_start_end(
    input: &str,
    start: i64,
    count: Option<i64>,
    is_input_ascii_only: bool,
) -> Result<(usize, usize)> {
    if let Some(count) = count {
        if count < 0 {
            return exec_err!(
                "negative count not allowed: {count}"
            );
        }
    }

    let start = start.checked_sub(1).unwrap_or(start);

    let end = match count {
        Some(count) => start.saturating_add(count),
        None => input.len() as i64,
    };
    let count_to_end = count.is_some();

    let start = start.clamp(0, input.len() as i64) as usize;
    let end = end.clamp(0, input.len() as i64) as usize;
    let count = end - start;

    // If input is ASCII-only, byte-based indices equals to char-based indices
    if is_input_ascii_only {
        return Ok((start, end));
    }

    // Otherwise, calculate byte indices from char indices
    // Note this decoding is relatively expensive for this simple `substr` function,
    // so the implementation attempts to decode in one pass (and caused the complexity)
    let (mut st, mut ed) = (input.len(), input.len());
    let mut start_counting = false;
    let mut cnt = 0;
    for (char_cnt, (byte_cnt, _)) in input.char_indices().enumerate() {
        if char_cnt == start {
            st = byte_cnt;
            if count_to_end {
                start_counting = true;
            } else {
                break;
            }
        }
        if start_counting {
            if cnt == count {
                ed = byte_cnt;
                break;
            }
            cnt += 1;
        }
    }
    Ok((st, ed))
}

// The decoding process refs the trait at: arrow/arrow-data/src/byte_view.rs:44
// From<u128> for ByteView
fn string_view_substr(
    string_view_array: &StringViewArray,
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let start_array = as_int64_array(&args[0])?;
    let is_ascii = string_view_array.is_ascii();
    let mut views_buf = Vec::with_capacity(string_view_array.len());
    let mut null_builder = NullBufferBuilder::new(string_view_array.len());

    // In either case of `substr(s, i)` or `substr(s, i, cnt)`
    // If any of input argument is `NULL`, the result is `NULL`
    match args.len() {
        1 => {
            for ((str_opt, raw_view), start_opt) in string_view_array
                .iter()
                .zip(string_view_array.views().iter())
                .zip(start_array.iter())
            {
                if let (Some(str), Some(start)) = (str_opt, start_opt) {
                    let (start, end) = get_true_start_end(str, start, None, is_ascii)?;
                    let substr = &str[start..end];

                    make_and_append_view(
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                        substr,
                        start as u32,
                    );
                } else {
                    null_builder.append_null();
                    views_buf.push(0);
                }
            }
        }
        2 => {
            let count_array = as_int64_array(&args[1])?;
            for (((str_opt, raw_view), start_opt), count_opt) in string_view_array
                .iter()
                .zip(string_view_array.views().iter())
                .zip(start_array.iter())
                .zip(count_array.iter())
            {
                if let (Some(str), Some(start), Some(count)) =
                    (str_opt, start_opt, count_opt)
                {
                    let (start, end) =
                        get_true_start_end(str, start, Some(count), is_ascii)?;
                    let substr = &str[start..end];

                    make_and_append_view(
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                        substr,
                        start as u32,
                    );
                } else {
                    null_builder.append_null();
                    views_buf.push(0);
                }
            }
        }
        other => {
            return exec_err!(
                "substr was called with {other} arguments. It requires 2 or 3."
            );
        }
    }

    let views_buf = ScalarBuffer::from(views_buf);
    let nulls_buf = null_builder.finish();

    // Safety:
    // (1) The blocks of the given views are all provided
    // (2) Each of the range `view.offset+start..end` of view in views_buf is within
    // the bounds of each of the blocks
    unsafe {
        let array = StringViewArray::new_unchecked(
            views_buf,
            string_view_array.data_buffers().to_vec(),
            nulls_buf,
        );
        Ok(Arc::new(array) as ArrayRef)
    }
}

fn string_substr<'a, V>(string_array: V, args: &[ArrayRef]) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
{
    let start_array = as_int64_array(&args[0])?;
    let is_ascii = string_array.is_ascii();
    let iter = ArrayIter::new(string_array);
    let mut result_builder = StringViewBuilder::new();

    match args.len() {
        1 => {
            for (string, start) in iter.zip(start_array.iter()) {
                if let (Some(string), Some(start)) = (string, start) {
                    let (start, end) = get_true_start_end(string, start, None, is_ascii)?;
                    result_builder.append_value(&string[start..end]);
                } else {
                    result_builder.append_null();
                }
            }
        }
        2 => {
            let count_array = as_int64_array(&args[1])?;

            for ((string, start), count) in
                iter.zip(start_array.iter()).zip(count_array.iter())
            {
                if let (Some(string), Some(start), Some(count)) = (string, start, count) {
                    let (start, end) =
                        get_true_start_end(string, start, Some(count), is_ascii)?;
                    result_builder.append_value(&string[start..end]);
                } else {
                    result_builder.append_null();
                }
            }
        }
        other => {
            return exec_err!(
                "substr was called with {other} arguments. It requires 2 or 3."
            );
        }
    }

    Ok(Arc::new(result_builder.finish()) as ArrayRef)
}

/// Fast path: handle substr(array_str, scalar_start[, scalar_count]).
/// Returns None if the args don't match this pattern.
fn try_substr_scalar_args(args: &[ColumnarValue]) -> Option<Result<ArrayRef>> {
    let ColumnarValue::Array(string_array) = &args[0] else {
        return None;
    };
    let ColumnarValue::Scalar(ScalarValue::Int64(Some(start))) = &args[1] else {
        return None;
    };
    let count = match args.get(2) {
        Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(c)))) => Some(*c),
        Some(_) => return None,
        None => None,
    };
    Some(substr_scalar_args(string_array, *start, count))
}

fn substr_scalar_args(
    string_array: &ArrayRef,
    start: i64,
    count: Option<i64>,
) -> Result<ArrayRef> {
    match string_array.data_type() {
        DataType::Utf8 => {
            let string_array = string_array.as_string::<i32>();
            string_substr_scalar_args(string_array, start, count)
        }
        DataType::LargeUtf8 => {
            let string_array = string_array.as_string::<i64>();
            string_substr_scalar_args(string_array, start, count)
        }
        DataType::Utf8View => {
            let string_array = string_array.as_string_view();
            string_view_substr_scalar_args(string_array, start, count)
        }
        other => exec_err!(
            "Unsupported data type {other:?} for function substr,\
            expected Utf8View, Utf8 or LargeUtf8."
        ),
    }
}

/// Decide whether to use the ASCII fast path when start/count are scalar.
/// When taking a short prefix (start + count <= 32), the char-based decoding
/// is cheaper than scanning the entire array for ASCII, so skip the check.
fn is_ascii_for_scalar_args<'a, V: StringArrayType<'a>>(
    string_array: &V,
    start: i64,
    count: Option<i64>,
) -> bool {
    let is_short_prefix = match count {
        Some(c) => start.saturating_add(c) <= 32,
        None => false,
    };
    if is_short_prefix {
        false
    } else {
        string_array.is_ascii()
    }
}

fn string_view_substr_scalar_args(
    string_view_array: &StringViewArray,
    start: i64,
    count: Option<i64>,
) -> Result<ArrayRef> {
    let mut views_buf = Vec::with_capacity(string_view_array.len());
    let mut null_builder = NullBufferBuilder::new(string_view_array.len());

    let is_ascii = is_ascii_for_scalar_args(&string_view_array, start, count);

    for (str_opt, raw_view) in string_view_array
        .iter()
        .zip(string_view_array.views().iter())
    {
        if let Some(s) = str_opt {
            let (st, ed) = get_true_start_end(s, start, count, is_ascii)?;
            let substr = &s[st..ed];
            make_and_append_view(
                &mut views_buf,
                &mut null_builder,
                raw_view,
                substr,
                st as u32,
            );
        } else {
            null_builder.append_null();
            views_buf.push(0);
        }
    }

    let views_buf = ScalarBuffer::from(views_buf);
    let nulls_buf = null_builder.finish();

    // Safety:
    // (1) The blocks of the given views are all provided
    // (2) Each of the range `view.offset+start..end` of view in views_buf is within
    // the bounds of each of the blocks
    unsafe {
        let array = StringViewArray::new_unchecked(
            views_buf,
            string_view_array.data_buffers().to_vec(),
            nulls_buf,
        );
        Ok(Arc::new(array) as ArrayRef)
    }
}

fn string_substr_scalar_args<'a, V>(
    string_array: V,
    start: i64,
    count: Option<i64>,
) -> Result<ArrayRef>
where
    V: StringArrayType<'a>,
{
    let is_ascii = is_ascii_for_scalar_args(&string_array, start, count);

    let iter = ArrayIter::new(string_array);
    let mut result_builder = StringViewBuilder::new();

    for string in iter {
        if let Some(s) = string {
            let (st, ed) = get_true_start_end(s, start, count, is_ascii)?;
            result_builder.append_value(&s[st..ed]);
        } else {
            result_builder.append_null();
        }
    }

    Ok(Arc::new(result_builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, StringViewArray};
    use arrow::datatypes::DataType::Utf8View;

    use datafusion_common::{Result, ScalarValue, exec_err};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::substr::SubstrFunc;
    use crate::utils::test::test_function;

    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(None)),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "this és longer than 12B"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some(" é")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "this is longer than 12B"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some(" is longer than 12B")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "joséésoj"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ph")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "alphabet"
                )))),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("ésoj")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
            ],
            Ok(Some("joséésoj")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("lphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-3i64)),
            ],
            Ok(Some("alphabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(30i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("ph")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(Some("phabet")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("alph")),
            &str,
            Utf8View,
            StringViewArray
        );
        // starting from 5 (10 + -5)
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(10i64)),
            ],
            Ok(Some("alph")),
            &str,
            Utf8View,
            StringViewArray
        );
        // starting from -1 (4 + -5)
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(4i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        // starting from 0 (5 + -5)
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(-5i64)),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
                ColumnarValue::Scalar(ScalarValue::from(20i64)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(3i64)),
                ColumnarValue::Scalar(ScalarValue::Int64(None)),
            ],
            Ok(None),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
                ColumnarValue::Scalar(ScalarValue::from(-1i64)),
            ],
            exec_err!("negative count not allowed: -1"),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("joséésoj")),
                ColumnarValue::Scalar(ScalarValue::from(5i64)),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
            ],
            Ok(Some("és")),
            &str,
            Utf8View,
            StringViewArray
        );
        #[cfg(not(feature = "unicode_expressions"))]
        test_function!(
            SubstrFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from(0i64)),
            ],
            internal_err!(
                "function substr requires compilation with feature flag: unicode_expressions."
            ),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("abc")),
                ColumnarValue::Scalar(ScalarValue::from(i64::MIN)),
            ],
            Ok(Some("abc")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("overflow")),
                ColumnarValue::Scalar(ScalarValue::from(i64::MIN)),
                ColumnarValue::Scalar(ScalarValue::from(1i64)),
            ],
            Ok(Some("")),
            &str,
            Utf8View,
            StringViewArray
        );
        test_function!(
            SubstrFunc::new(),
            vec![
                ColumnarValue::Scalar(ScalarValue::from("large count")),
                ColumnarValue::Scalar(ScalarValue::from(2i64)),
                ColumnarValue::Scalar(ScalarValue::from(i64::MAX)),
            ],
            Ok(Some("arge count")),
            &str,
            Utf8View,
            StringViewArray
        );

        Ok(())
    }
}
