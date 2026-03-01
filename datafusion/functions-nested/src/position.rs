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

//! [`ScalarUDFImpl`] definitions for array_position and array_positions functions.

use arrow::array::Scalar;
use arrow::datatypes::DataType;
use arrow::datatypes::{
    DataType::{LargeList, List, UInt64},
    Field,
};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_macros::user_doc;

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, GenericListArray, ListArray, OffsetSizeTrait, UInt64Array,
    types::UInt64Type,
};
use datafusion_common::cast::{
    as_generic_list_array, as_int64_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, exec_err, utils::take_function_args};
use itertools::Itertools;

use crate::utils::{compare_element_to_list, make_scalar_function};

make_udf_expr_and_func!(
    ArrayPosition,
    array_position,
    array element index,
    "searches for an element in the array, returns first occurrence.",
    array_position_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns the position of the first occurrence of the specified element in the array, or NULL if not found. Comparisons are done using `IS DISTINCT FROM` semantics, so NULL is considered to match NULL.",
    syntax_example = "array_position(array, element)\narray_position(array, element, index)",
    sql_example = r#"```sql
> select array_position([1, 2, 2, 3, 1, 4], 2);
+----------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2)) |
+----------------------------------------------+
| 2                                            |
+----------------------------------------------+
> select array_position([1, 2, 2, 3, 1, 4], 2, 3);
+----------------------------------------------------+
| array_position(List([1,2,2,3,1,4]),Int64(2), Int64(3)) |
+----------------------------------------------------+
| 3                                                  |
+----------------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(name = "element", description = "Element to search for in the array."),
    argument(
        name = "index",
        description = "Index at which to start searching (1-indexed)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrayPosition {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArrayPosition {
    fn default() -> Self {
        Self::new()
    }
}
impl ArrayPosition {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element_and_optional_index(
                Volatility::Immutable,
            ),
            aliases: vec![
                String::from("list_position"),
                String::from("array_indexof"),
                String::from("list_indexof"),
            ],
        }
    }
}

impl ScalarUDFImpl for ArrayPosition {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_position"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt64)
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let [first_arg, second_arg, third_arg @ ..] = args.args.as_slice() else {
            return exec_err!("array_position expects two or three arguments");
        };

        match second_arg {
            ColumnarValue::Scalar(scalar_element) => {
                // Nested element types (List, Struct) can't use the fast path
                // (because Arrow's `non_distinct` does not support them).
                if scalar_element.data_type().is_nested() {
                    return make_scalar_function(array_position_inner)(&args.args);
                }

                // Determine batch length from whichever argument is columnar;
                // if all inputs are scalar, batch length is 1.
                let (num_rows, all_inputs_scalar) = match (first_arg, third_arg.first()) {
                    (ColumnarValue::Array(a), _) => (a.len(), false),
                    (_, Some(ColumnarValue::Array(a))) => (a.len(), false),
                    _ => (1, true),
                };

                let element_arr = scalar_element.to_array_of_size(1)?;
                let haystack = first_arg.to_array(num_rows)?;
                let arr_from = resolve_start_from(third_arg.first(), num_rows)?;

                let result = match haystack.data_type() {
                    List(_) => {
                        let list = as_generic_list_array::<i32>(&haystack)?;
                        array_position_scalar::<i32>(list, &element_arr, &arr_from)
                    }
                    LargeList(_) => {
                        let list = as_generic_list_array::<i64>(&haystack)?;
                        array_position_scalar::<i64>(list, &element_arr, &arr_from)
                    }
                    t => exec_err!("array_position does not support type '{t}'."),
                }?;

                if all_inputs_scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        &result, 0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(result))
                }
            }
            ColumnarValue::Array(_) => {
                make_scalar_function(array_position_inner)(&args.args)
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_position_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() < 2 || args.len() > 3 {
        return exec_err!("array_position expects two or three arguments");
    }
    match &args[0].data_type() {
        List(_) => general_position_dispatch::<i32>(args),
        LargeList(_) => general_position_dispatch::<i64>(args),
        array_type => exec_err!("array_position does not support type '{array_type}'."),
    }
}

/// Resolves the optional `start_from` argument into a `Vec<i64>` of
/// 0-indexed starting positions.
fn resolve_start_from(
    third_arg: Option<&ColumnarValue>,
    num_rows: usize,
) -> Result<Vec<i64>> {
    match third_arg {
        None => Ok(vec![0i64; num_rows]),
        Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(v)))) => {
            Ok(vec![v - 1; num_rows])
        }
        Some(ColumnarValue::Scalar(s)) => {
            exec_err!("array_position expected Int64 for start_from, got {s}")
        }
        Some(ColumnarValue::Array(a)) => {
            Ok(as_int64_array(a)?.values().iter().map(|&x| x - 1).collect())
        }
    }
}

/// Fast path for `array_position` when the element is a scalar.
///
/// Performs a single bulk `not_distinct` comparison of the scalar element
/// against the entire flattened values buffer, then walks the result bitmap
/// using offsets to find per-row first-match positions.
fn array_position_scalar<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    element_array: &ArrayRef,
    arr_from: &[i64], // 0-indexed
) -> Result<ArrayRef> {
    crate::utils::check_datatypes(
        "array_position",
        &[list_array.values(), element_array],
    )?;
    let element_datum = Scalar::new(Arc::clone(element_array));

    let offsets = list_array.offsets();
    let validity = list_array.nulls();

    if list_array.len() == 0 {
        return Ok(Arc::new(UInt64Array::new_null(0)));
    }

    // `not_distinct` treats NULL=NULL as true, matching the semantics of
    // `array_position`
    let eq_array = arrow_ord::cmp::not_distinct(list_array.values(), &element_datum)?;
    let eq_bits = eq_array.values();
    let bit_data = eq_bits.values();
    let bit_offset = eq_bits.offset();

    let mut result: Vec<Option<u64>> = Vec::with_capacity(list_array.len());

    for i in 0..list_array.len() {
        let start = offsets[i].as_usize();
        let end = offsets[i + 1].as_usize();

        if validity.is_some_and(|v| v.is_null(i)) {
            result.push(None);
            continue;
        }

        let from = arr_from[i];
        let row_len = end - start;
        if !(from >= 0 && (from as usize) <= row_len) {
            return exec_err!("start_from out of bounds: {}", from + 1);
        }
        let search_start = start + from as usize;

        let found = first_set_bit_in_range(bit_data, bit_offset, search_start, end)
            .map(|pos| (pos - start + 1) as u64);
        result.push(found);
    }

    debug_assert_eq!(result.len(), list_array.len());
    Ok(Arc::new(UInt64Array::from(result)))
}

/// Finds the first set bit in the range [start, end) of a bit-packed buffer.
///
/// `data` is the raw byte buffer, `bit_offset` is the global bit offset
/// (from `BooleanBuffer::offset()`), and `start`/`end` are positions
/// relative to that offset.
///
/// Uses u64 word-level scanning with `trailing_zeros()` so that zero-heavy
/// regions (no matches) are skipped ~64 bits at a time, while still
/// stopping immediately on the first match.
#[inline]
fn first_set_bit_in_range(
    data: &[u8],
    bit_offset: usize,
    start: usize,
    end: usize,
) -> Option<usize> {
    if start >= end {
        return None;
    }
    let abs_start = bit_offset + start;
    let abs_end = bit_offset + end;

    let first_byte = abs_start / 8;
    let last_byte = (abs_end - 1) / 8;
    let start_bit = (abs_start % 8) as u32;

    // All bits within a single byte
    if first_byte == last_byte {
        let end_bit = (abs_end % 8) as u32;
        let mask = if end_bit == 0 {
            0xFFu8
        } else {
            (1u8 << end_bit) - 1
        };
        let byte = data[first_byte] & mask & (0xFFu8 << start_bit);
        return if byte != 0 {
            Some(first_byte * 8 + byte.trailing_zeros() as usize - bit_offset)
        } else {
            None
        };
    }

    // First partial byte
    let byte = data[first_byte] & (0xFFu8 << start_bit);
    if byte != 0 {
        return Some(first_byte * 8 + byte.trailing_zeros() as usize - bit_offset);
    }

    // Middle bytes: scan in u64 words where possible
    let mut idx = first_byte + 1;
    while idx + 8 <= last_byte {
        let word = u64::from_le_bytes(
            data[idx..idx + 8].try_into().expect("slice is 8 bytes"),
        );
        if word != 0 {
            return Some(idx * 8 + word.trailing_zeros() as usize - bit_offset);
        }
        idx += 8;
    }

    // Remaining middle bytes (< 8)
    while idx < last_byte {
        if data[idx] != 0 {
            return Some(idx * 8 + data[idx].trailing_zeros() as usize - bit_offset);
        }
        idx += 1;
    }

    // Last partial byte
    let end_bit = (abs_end % 8) as u32;
    let mask = if end_bit == 0 {
        0xFFu8
    } else {
        (1u8 << end_bit) - 1
    };
    let byte = data[last_byte] & mask;
    if byte != 0 {
        Some(last_byte * 8 + byte.trailing_zeros() as usize - bit_offset)
    } else {
        None
    }
}

fn general_position_dispatch<O: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let list_array = as_generic_list_array::<O>(&args[0])?;
    let element_array = &args[1];

    crate::utils::check_datatypes(
        "array_position",
        &[list_array.values(), element_array],
    )?;

    let arr_from = if args.len() == 3 {
        as_int64_array(&args[2])?
            .values()
            .iter()
            .map(|&x| x - 1)
            .collect::<Vec<_>>()
    } else {
        vec![0; list_array.len()]
    };

    for (arr, &from) in list_array.iter().zip(arr_from.iter()) {
        // If `arr` is `None`: we will get null if we got null in the array, so we don't need to check
        if !arr.is_none_or(|arr| from >= 0 && (from as usize) <= arr.len()) {
            return exec_err!("start_from out of bounds: {}", from + 1);
        }
    }

    generic_position::<O>(list_array, element_array, &arr_from)
}

fn generic_position<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
    arr_from: &[i64], // 0-indexed
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, (list_array_row, &from)) in
        list_array.iter().zip(arr_from.iter()).enumerate()
    {
        let from = from as usize;

        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let index = eq_array
                .iter()
                .skip(from)
                .position(|e| e == Some(true))
                .map(|index| (from + index + 1) as u64);

            data.push(index);
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(UInt64Array::from(data)))
}

make_udf_expr_and_func!(
    ArrayPositions,
    array_positions,
    array element, // arg name
    "searches for an element in the array, returns all occurrences.", // doc
    array_positions_udf // internal function name
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Searches for an element in the array, returns all occurrences.",
    syntax_example = "array_positions(array, element)",
    sql_example = r#"```sql
> select array_positions([1, 2, 2, 3, 1, 4], 2);
+-----------------------------------------------+
| array_positions(List([1,2,2,3,1,4]),Int64(2)) |
+-----------------------------------------------+
| [2, 3]                                        |
+-----------------------------------------------+
```"#,
    argument(
        name = "array",
        description = "Array expression. Can be a constant, column, or function, and any combination of array operators."
    ),
    argument(
        name = "element",
        description = "Element to search for position in the array."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct ArrayPositions {
    signature: Signature,
    aliases: Vec<String>,
}

impl ArrayPositions {
    pub fn new() -> Self {
        Self {
            signature: Signature::array_and_element(Volatility::Immutable),
            aliases: vec![String::from("list_positions")],
        }
    }
}

impl ScalarUDFImpl for ArrayPositions {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "array_positions"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(List(Arc::new(Field::new_list_field(UInt64, true))))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(array_positions_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn array_positions_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [array, element] = take_function_args("array_positions", args)?;

    match &array.data_type() {
        List(_) => {
            let arr = as_list_array(&array)?;
            crate::utils::check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i32>(arr, element)
        }
        LargeList(_) => {
            let arr = as_large_list_array(&array)?;
            crate::utils::check_datatypes("array_positions", &[arr.values(), element])?;
            general_positions::<i64>(arr, element)
        }
        array_type => {
            exec_err!("array_positions does not support type '{array_type}'.")
        }
    }
}

fn general_positions<OffsetSize: OffsetSizeTrait>(
    list_array: &GenericListArray<OffsetSize>,
    element_array: &ArrayRef,
) -> Result<ArrayRef> {
    let mut data = Vec::with_capacity(list_array.len());

    for (row_index, list_array_row) in list_array.iter().enumerate() {
        if let Some(list_array_row) = list_array_row {
            let eq_array =
                compare_element_to_list(&list_array_row, element_array, row_index, true)?;

            // Collect `true`s in 1-indexed positions
            let indexes = eq_array
                .iter()
                .positions(|e| e == Some(true))
                .map(|index| Some(index as u64 + 1))
                .collect::<Vec<_>>();

            data.push(Some(indexes));
        } else {
            data.push(None);
        }
    }

    Ok(Arc::new(
        ListArray::from_iter_primitive::<UInt64Type, _, _>(data),
    ))
}
