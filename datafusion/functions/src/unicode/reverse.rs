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

use crate::utils::{make_scalar_function, utf8_to_str_type};
use DataType::{LargeUtf8, Utf8, Utf8View};
use arrow::array::{
    Array, ArrayRef, AsArray, GenericStringArray, GenericStringBuilder, OffsetSizeTrait,
    StringArrayType, StringViewArray,
};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::DataType;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Reverses the character order of a string.",
    syntax_example = "reverse(str)",
    sql_example = r#"```sql
> select reverse('datafusion');
+-----------------------------+
| reverse(Utf8("datafusion")) |
+-----------------------------+
| noisufatad                  |
+-----------------------------+
```"#,
    standard_argument(name = "str", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReverseFunc {
    signature: Signature,
}

impl Default for ReverseFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ReverseFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Utf8View, Utf8, LargeUtf8],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ReverseFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "reverse"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        utf8_to_str_type(&arg_types[0], "reverse")
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;
        match args[0].data_type() {
            Utf8 | Utf8View => make_scalar_function(reverse::<i32>, vec![])(args),
            LargeUtf8 => make_scalar_function(reverse::<i64>, vec![])(args),
            other => {
                exec_err!("Unsupported data type {other:?} for function reverse")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Reverses the order of the characters in the string `reverse('abcde') = 'edcba'`.
/// The implementation uses UTF-8 code points as characters
fn reverse<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args[0].data_type() == &Utf8View {
        let string_view_array = args[0].as_string_view();
        if string_view_array.is_ascii() {
            reverse_ascii_string_view(string_view_array)
        } else {
            reverse_row_by_row::<T, _>(&string_view_array)
        }
    } else {
        let string_array = args[0].as_string::<T>();
        if string_array.value_data().is_ascii() {
            reverse_ascii_array(string_array)
        } else {
            reverse_row_by_row::<T, _>(&string_array)
        }
    }
}

/// Fast path for all-ASCII `GenericStringArray`: copy the contiguous value
/// buffer then reverse bytes in-place within each string's offset span. We can
/// also avoid recomputing the input offsets and nulls buffers.
fn reverse_ascii_array<T: OffsetSizeTrait>(
    string_array: &GenericStringArray<T>,
) -> Result<ArrayRef> {
    let value_data = string_array.value_data();
    let mut reversed = value_data.to_vec();

    let offsets = string_array.offsets();
    for i in 0..string_array.len() {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();
        reversed[start..end].reverse();
    }

    let values = Buffer::from_vec(reversed);
    // SAFETY: We only reversed bytes within each string's offset span.
    // Since all bytes are ASCII, reversing preserves valid UTF-8.
    // Offsets and nulls are unchanged from the input array.
    Ok(Arc::new(unsafe {
        GenericStringArray::<T>::new_unchecked(
            offsets.clone(),
            values,
            string_array.nulls().cloned(),
        )
    }))
}

/// Fast path for all-ASCII `StringViewArray`: pre-allocate the exact output
/// size and copy+reverse each string, bypassing `GenericStringBuilder`.
fn reverse_ascii_string_view(string_view_array: &StringViewArray) -> Result<ArrayRef> {
    let len = string_view_array.len();

    // Compute total output byte size from the views.
    let total_bytes: usize = (0..len)
        .map(|i| string_view_array.value(i).len())
        .sum();

    let mut values = Vec::<u8>::with_capacity(total_bytes);
    let mut offsets = Vec::<i32>::with_capacity(len + 1);
    offsets.push(0);

    for i in 0..len {
        if string_view_array.is_null(i) {
            offsets.push(*offsets.last().unwrap());
        } else {
            let s = string_view_array.value(i);
            let start = values.len();
            values.extend_from_slice(s.as_bytes());
            values[start..].reverse();
            offsets.push(values.len() as i32);
        }
    }

    let values = Buffer::from_vec(values);
    // SAFETY: offsets are monotonically increasing and within bounds of values.
    let offsets =
        unsafe { OffsetBuffer::new_unchecked(Buffer::from_vec(offsets).into()) };
    let nulls = string_view_array.nulls().cloned();

    // SAFETY: reversed ASCII bytes are valid UTF-8. Offsets and nulls are correct.
    Ok(Arc::new(unsafe {
        GenericStringArray::<i32>::new_unchecked(offsets, values, nulls)
    }))
}

fn reverse_row_by_row<'a, T: OffsetSizeTrait, V: StringArrayType<'a>>(
    string_array: &V,
) -> Result<ArrayRef> {
    let mut builder = GenericStringBuilder::<T>::with_capacity(string_array.len(), 1024);

    let mut string_buf = String::new();
    let mut byte_buf = Vec::<u8>::new();
    for string in string_array.iter() {
        if let Some(s) = string {
            if s.is_ascii() {
                byte_buf.extend(s.as_bytes());
                byte_buf.reverse();
                // SAFETY: Since the original string was ASCII, reversing the bytes still results in valid UTF-8.
                let reversed = unsafe { std::str::from_utf8_unchecked(&byte_buf) };
                builder.append_value(reversed);
                byte_buf.clear();
            } else {
                string_buf.extend(s.chars().rev());
                builder.append_value(&string_buf);
                string_buf.clear();
            }
        } else {
            builder.append_null();
        }
    }

    Ok(Arc::new(builder.finish()) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, LargeStringArray, StringArray};
    use arrow::datatypes::DataType::{LargeUtf8, Utf8};

    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use crate::unicode::reverse::ReverseFunc;
    use crate::utils::test::test_function;

    macro_rules! test_reverse {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_function!(
                ReverseFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );

            test_function!(
                ReverseFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                &str,
                LargeUtf8,
                LargeStringArray
            );

            test_function!(
                ReverseFunc::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                &str,
                Utf8,
                StringArray
            );
        };
    }

    #[test]
    fn test_functions() -> Result<()> {
        test_reverse!(Some("abcde".into()), Ok(Some("edcba")));
        test_reverse!(Some("loẅks".into()), Ok(Some("sk̈wol")));
        test_reverse!(Some("loẅks".into()), Ok(Some("sk̈wol")));
        test_reverse!(None, Ok(None));
        #[cfg(not(feature = "unicode_expressions"))]
        test_reverse!(
            Some("abcde".into()),
            internal_err!(
                "function reverse requires compilation with feature flag: unicode_expressions."
            ),
        );

        Ok(())
    }
}
