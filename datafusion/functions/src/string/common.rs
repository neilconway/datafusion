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

//! Common utilities for implementing string functions

use std::sync::Arc;

use crate::strings::make_and_append_view;
use arrow::array::{
    Array, ArrayRef, GenericStringArray, GenericStringBuilder, NullBufferBuilder,
    OffsetSizeTrait, StringViewArray, StringViewBuilder, make_view, new_null_array,
};
use arrow::buffer::{Buffer, ScalarBuffer};
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::{ScalarValue, exec_err};
use datafusion_expr::ColumnarValue;

/// Trait for trim operations, allowing compile-time dispatch instead of runtime matching.
///
/// Each implementation performs its specific trim operation and returns
/// (trimmed_str, start_offset) where start_offset is the byte offset
/// from the beginning of the input string where the trimmed result starts.
pub(crate) trait Trimmer {
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32);

    /// Optimized trim for a single ASCII byte.
    /// Uses byte-level scanning instead of char-level iteration.
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32);
}

/// Returns the number of leading bytes matching `byte`
#[inline]
fn leading_bytes(bytes: &[u8], byte: u8) -> usize {
    bytes.iter().take_while(|&&b| b == byte).count()
}

/// Returns the number of trailing bytes matching `byte`
#[inline]
fn trailing_bytes(bytes: &[u8], byte: u8) -> usize {
    bytes.iter().rev().take_while(|&&b| b == byte).count()
}

/// Left trim - removes leading characters
pub(crate) struct TrimLeft;

impl Trimmer for TrimLeft {
    #[inline]
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32) {
        if pattern.len() == 1 && pattern[0].is_ascii() {
            return Self::trim_ascii_char(input, pattern[0] as u8);
        }
        let trimmed = input.trim_start_matches(pattern);
        let offset = (input.len() - trimmed.len()) as u32;
        (trimmed, offset)
    }

    #[inline]
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32) {
        let start = leading_bytes(input.as_bytes(), byte);
        (&input[start..], start as u32)
    }
}

/// Right trim - removes trailing characters
pub(crate) struct TrimRight;

impl Trimmer for TrimRight {
    #[inline]
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32) {
        if pattern.len() == 1 && pattern[0].is_ascii() {
            return Self::trim_ascii_char(input, pattern[0] as u8);
        }
        let trimmed = input.trim_end_matches(pattern);
        (trimmed, 0)
    }

    #[inline]
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32) {
        let bytes = input.as_bytes();
        let end = bytes.len() - trailing_bytes(bytes, byte);
        (&input[..end], 0)
    }
}

/// Both trim - removes both leading and trailing characters
pub(crate) struct TrimBoth;

impl Trimmer for TrimBoth {
    #[inline]
    fn trim<'a>(input: &'a str, pattern: &[char]) -> (&'a str, u32) {
        if pattern.len() == 1 && pattern[0].is_ascii() {
            return Self::trim_ascii_char(input, pattern[0] as u8);
        }
        let left_trimmed = input.trim_start_matches(pattern);
        let offset = (input.len() - left_trimmed.len()) as u32;
        let trimmed = left_trimmed.trim_end_matches(pattern);
        (trimmed, offset)
    }

    #[inline]
    fn trim_ascii_char(input: &str, byte: u8) -> (&str, u32) {
        let bytes = input.as_bytes();
        let start = leading_bytes(bytes, byte);
        let end = bytes.len() - trailing_bytes(&bytes[start..], byte);
        (&input[start..end], start as u32)
    }
}

pub(crate) fn general_trim<T: OffsetSizeTrait, Tr: Trimmer>(
    args: &[ArrayRef],
    use_string_view: bool,
) -> Result<ArrayRef> {
    if use_string_view {
        string_view_trim::<Tr>(args)
    } else {
        generic_string_trim::<T, Tr>(args)
    }
}

/// Returns true if the values buffer of the given string array fits in a u32
/// offset, which is required for StringView.
#[expect(dead_code)]
fn values_fit_in_u32<T: OffsetSizeTrait>(string_array: &GenericStringArray<T>) -> bool {
    string_array
        .offsets()
        .last()
        .map(|offset| offset.as_usize() <= u32::MAX as usize)
        .unwrap_or(true)
}

/// Creates a new StringView and appends it to the views buffer.
///
/// Returns true if the view is out-of-line (string length > 12 bytes).
#[inline]
fn append_new_view(
    views_buf: &mut Vec<u128>,
    null_builder: &mut NullBufferBuilder,
    substr: &str,
    byte_offset: usize,
) -> bool {
    let is_out_of_line = substr.len() > 12;
    let view = if is_out_of_line {
        let byte_offset =
            u32::try_from(byte_offset).expect("validated string buffer offset fits in u32");
        make_view(substr.as_bytes(), 0, byte_offset)
    } else {
        make_view(substr.as_bytes(), 0, 0)
    };

    views_buf.push(view);
    null_builder.append_non_null();
    is_out_of_line
}

/// Applies the trim function to the given Utf8/LargeUtf8 string array(s)
/// and returns a StringViewArray.
///
/// Uses zero-copy (views into the original values buffer) when the average
/// string length exceeds 12 bytes, since those results will be stored
/// out-of-line in the StringView and benefit from avoiding the copy.
/// For shorter strings, uses `collect::<StringViewArray>()` which has
/// lower per-element overhead via Arrow's optimized `FromIterator`.
///
/// Falls back to the copy-based path for arrays whose values buffer
/// exceeds 4GB (cannot be addressed by u32 StringView offsets).
fn generic_string_trim<T: OffsetSizeTrait, Tr: Trimmer>(
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let string_array = as_generic_string_array::<T>(&args[0])?;

    generic_string_trim_collect::<T, Tr>(string_array, args)
}

/// Trim via `collect::<StringViewArray>()`. Copies data but uses Arrow's
/// optimized `FromIterator` which has low per-element overhead.
fn generic_string_trim_collect<T: OffsetSizeTrait, Tr: Trimmer>(
    string_array: &GenericStringArray<T>,
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    match args.len() {
        1 => {
            let result = string_array
                .iter()
                .map(|string| string.map(|s| Tr::trim_ascii_char(s, b' ').0))
                .collect::<StringViewArray>();
            Ok(Arc::new(result) as ArrayRef)
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            if characters_array.len() == 1 {
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        &DataType::Utf8View,
                        string_array.len(),
                    ));
                }
                let pattern: Vec<char> = characters_array.value(0).chars().collect();
                let result = string_array
                    .iter()
                    .map(|item| item.map(|s| Tr::trim(s, &pattern).0))
                    .collect::<StringViewArray>();
                return Ok(Arc::new(result) as ArrayRef);
            }

            let mut pattern: Vec<char> = Vec::new();
            let result = string_array
                .iter()
                .zip(characters_array.iter())
                .map(|(string, characters)| match (string, characters) {
                    (Some(s), Some(c)) => {
                        pattern.clear();
                        pattern.extend(c.chars());
                        Some(Tr::trim(s, &pattern).0)
                    }
                    _ => None,
                })
                .collect::<StringViewArray>();
            Ok(Arc::new(result) as ArrayRef)
        }
        other => {
            exec_err!(
                "Function TRIM was called with {other} arguments. It requires at least 1 and at most 2."
            )
        }
    }
}

/// Trim via zero-copy: constructs StringView views pointing into the
/// original GenericStringArray values buffer, avoiding data copies
/// for out-of-line strings (> 12 bytes).
#[expect(dead_code)]
#[expect(clippy::needless_range_loop)]
fn generic_string_trim_zerocopy<T: OffsetSizeTrait, Tr: Trimmer>(
    string_array: &GenericStringArray<T>,
    args: &[ArrayRef],
) -> Result<ArrayRef> {
    let offsets = string_array.value_offsets();
    let mut views_buf = Vec::with_capacity(string_array.len());
    let mut null_builder = NullBufferBuilder::new(string_array.len());
    let mut has_out_of_line = false;

    match args.len() {
        1 => {
            for i in 0..string_array.len() {
                if string_array.is_null(i) {
                    null_builder.append_null();
                    views_buf.push(0);
                    continue;
                }
                let src_str = string_array.value(i);
                let source_offset = offsets[i].as_usize();
                let (trimmed, trim_offset) = Tr::trim_ascii_char(src_str, b' ');
                has_out_of_line |= append_new_view(
                    &mut views_buf,
                    &mut null_builder,
                    trimmed,
                    source_offset + trim_offset as usize,
                );
            }
        }
        2 => {
            let characters_array = as_generic_string_array::<T>(&args[1])?;

            if characters_array.len() == 1 {
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        &DataType::Utf8View,
                        string_array.len(),
                    ));
                }

                let pattern: Vec<char> = characters_array.value(0).chars().collect();
                for i in 0..string_array.len() {
                    if string_array.is_null(i) {
                        null_builder.append_null();
                        views_buf.push(0);
                        continue;
                    }
                    let src_str = string_array.value(i);
                    let source_offset = offsets[i].as_usize();
                    let (trimmed, trim_offset) = Tr::trim(src_str, &pattern);
                    has_out_of_line |= append_new_view(
                        &mut views_buf,
                        &mut null_builder,
                        trimmed,
                        source_offset + trim_offset as usize,
                    );
                }
            } else {
                let mut pattern: Vec<char> = Vec::new();
                for i in 0..string_array.len() {
                    if string_array.is_null(i) || characters_array.is_null(i) {
                        null_builder.append_null();
                        views_buf.push(0);
                        continue;
                    }
                    let src_str = string_array.value(i);
                    let source_offset = offsets[i].as_usize();
                    let characters = characters_array.value(i);
                    pattern.clear();
                    pattern.extend(characters.chars());
                    let (trimmed, trim_offset) = Tr::trim(src_str, &pattern);
                    has_out_of_line |= append_new_view(
                        &mut views_buf,
                        &mut null_builder,
                        trimmed,
                        source_offset + trim_offset as usize,
                    );
                }
            }
        }
        other => {
            return exec_err!(
                "Function TRIM was called with {other} arguments. It requires at least 1 and at most 2."
            );
        }
    }

    let views_buf = ScalarBuffer::from(views_buf);
    let nulls_buf = null_builder.finish();

    // If all results are inline (≤ 12 bytes), no need to retain the
    // input buffer.
    let data_buffers = if has_out_of_line {
        vec![string_array.values().clone()]
    } else {
        vec![]
    };

    // Safety:
    // (1) The data buffers referenced by the views are provided
    // (2) Each view's offset+length range is within the bounds of the
    //     source values buffer (trimmed substrings are subsets of original strings)
    unsafe {
        let array =
            StringViewArray::new_unchecked(views_buf, data_buffers, nulls_buf);
        Ok(Arc::new(array) as ArrayRef)
    }
}

/// Applies the trim function to the given string view array(s)
/// and returns a new string view array with the trimmed values.
///
/// Pre-computes the pattern characters once for scalar patterns to avoid
/// repeated allocations per row.
fn string_view_trim<Tr: Trimmer>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let string_view_array = as_string_view_array(&args[0])?;
    let mut views_buf = Vec::with_capacity(string_view_array.len());
    let mut null_builder = NullBufferBuilder::new(string_view_array.len());

    match args.len() {
        1 => {
            // Trim spaces by default
            for (src_str_opt, raw_view) in string_view_array
                .iter()
                .zip(string_view_array.views().iter())
            {
                if let Some(src_str) = src_str_opt {
                    let (trimmed, offset) = Tr::trim_ascii_char(src_str, b' ');
                    make_and_append_view(
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                        trimmed,
                        offset,
                    );
                } else {
                    null_builder.append_null();
                    views_buf.push(0);
                }
            }
        }
        2 => {
            let characters_array = as_string_view_array(&args[1])?;

            if characters_array.len() == 1 {
                // Scalar pattern - pre-compute pattern chars once
                if characters_array.is_null(0) {
                    return Ok(new_null_array(
                        &DataType::Utf8View,
                        string_view_array.len(),
                    ));
                }

                let pattern: Vec<char> = characters_array.value(0).chars().collect();
                for (src_str_opt, raw_view) in string_view_array
                    .iter()
                    .zip(string_view_array.views().iter())
                {
                    trim_and_append_view::<Tr>(
                        src_str_opt,
                        &pattern,
                        &mut views_buf,
                        &mut null_builder,
                        raw_view,
                    );
                }
            } else {
                // Per-row pattern - must compute pattern chars for each row
                let mut pattern: Vec<char> = Vec::new();
                for ((src_str_opt, raw_view), characters_opt) in string_view_array
                    .iter()
                    .zip(string_view_array.views().iter())
                    .zip(characters_array.iter())
                {
                    if let (Some(src_str), Some(characters)) =
                        (src_str_opt, characters_opt)
                    {
                        pattern.clear();
                        pattern.extend(characters.chars());
                        let (trimmed, offset) = Tr::trim(src_str, &pattern);
                        make_and_append_view(
                            &mut views_buf,
                            &mut null_builder,
                            raw_view,
                            trimmed,
                            offset,
                        );
                    } else {
                        null_builder.append_null();
                        views_buf.push(0);
                    }
                }
            }
        }
        other => {
            return exec_err!(
                "Function TRIM was called with {other} arguments. It requires at least 1 and at most 2."
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

/// Trims the given string and appends the trimmed string to the views buffer
/// and the null buffer.
///
/// Arguments
/// - `src_str_opt`: The original string value (represented by the view)
/// - `pattern`: Pre-computed character pattern to trim
/// - `views_buf`: The buffer to append the updated views to
/// - `null_builder`: The buffer to append the null values to
/// - `original_view`: The original view value (that contains src_str_opt)
#[inline]
fn trim_and_append_view<Tr: Trimmer>(
    src_str_opt: Option<&str>,
    pattern: &[char],
    views_buf: &mut Vec<u128>,
    null_builder: &mut NullBufferBuilder,
    original_view: &u128,
) {
    if let Some(src_str) = src_str_opt {
        let (trimmed, offset) = Tr::trim(src_str, pattern);
        make_and_append_view(views_buf, null_builder, original_view, trimmed, offset);
    } else {
        null_builder.append_null();
        views_buf.push(0);
    }
}

pub(crate) fn to_lower(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    case_conversion(args, |string| string.to_lowercase(), name)
}

pub(crate) fn to_upper(args: &[ColumnarValue], name: &str) -> Result<ColumnarValue> {
    case_conversion(args, |string| string.to_uppercase(), name)
}

fn case_conversion<'a, F>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    F: Fn(&'a str) -> String,
{
    match &args[0] {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(case_conversion_array::<i32, _>(
                array, op,
            )?)),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(case_conversion_array::<
                i64,
                _,
            >(array, op)?)),
            DataType::Utf8View => {
                let string_array = as_string_view_array(array)?;
                let mut string_builder =
                    StringViewBuilder::with_capacity(string_array.len());

                for str in string_array.iter() {
                    if let Some(str) = str {
                        string_builder.append_value(op(str));
                    } else {
                        string_builder.append_null();
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(string_builder.finish())))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::LargeUtf8(result)))
            }
            ScalarValue::Utf8View(a) => {
                let result = a.as_ref().map(|x| op(x));
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8View(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

fn case_conversion_array<'a, O, F>(array: &'a ArrayRef, op: F) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    const PRE_ALLOC_BYTES: usize = 8;

    let string_array = as_generic_string_array::<O>(array)?;
    let value_data = string_array.value_data();

    // All values are ASCII.
    if value_data.is_ascii() {
        return case_conversion_ascii_array::<O, _>(string_array, op);
    }

    // Values contain non-ASCII.
    let item_len = string_array.len();
    let capacity = string_array.value_data().len() + PRE_ALLOC_BYTES;
    let mut builder = GenericStringBuilder::<O>::with_capacity(item_len, capacity);

    if string_array.null_count() == 0 {
        let iter =
            (0..item_len).map(|i| Some(op(unsafe { string_array.value_unchecked(i) })));
        builder.extend(iter);
    } else {
        let iter = string_array.iter().map(|string| string.map(&op));
        builder.extend(iter);
    }
    Ok(Arc::new(builder.finish()))
}

/// All values of string_array are ASCII, and when converting case, there is no changes in the byte
/// array length. Therefore, the StringArray can be treated as a complete ASCII string for
/// case conversion, and we can reuse the offsets buffer and the nulls buffer.
fn case_conversion_ascii_array<'a, O, F>(
    string_array: &'a GenericStringArray<O>,
    op: F,
) -> Result<ArrayRef>
where
    O: OffsetSizeTrait,
    F: Fn(&'a str) -> String,
{
    let value_data = string_array.value_data();
    // SAFETY: all items stored in value_data satisfy UTF8.
    // ref: impl ByteArrayNativeType for str {...}
    let str_values = unsafe { std::str::from_utf8_unchecked(value_data) };

    // conversion
    let converted_values = op(str_values);
    assert_eq!(converted_values.len(), str_values.len());
    let bytes = converted_values.into_bytes();

    // build result
    let values = Buffer::from_vec(bytes);
    let offsets = string_array.offsets().clone();
    let nulls = string_array.nulls().cloned();
    // SAFETY: offsets and nulls are consistent with the input array.
    Ok(Arc::new(unsafe {
        GenericStringArray::<O>::new_unchecked(offsets, values, nulls)
    }))
}
