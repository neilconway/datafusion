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

//! [`ScalarUDFImpl`] definitions for arrays_zip function.

use crate::utils::make_scalar_function;
use arrow::array::{
    Array, ArrayRef, Capacities, ListArray, MutableArrayData, NullBufferBuilder,
    StructArray, new_null_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::DataType::{FixedSizeList, LargeList, List, Null};
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::cast::{
    as_fixed_size_list_array, as_large_list_array, as_list_array,
};
use datafusion_common::{Result, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

/// Type-erased view of a list column (works for both List and LargeList).
/// Stores the information needed to iterate rows without re-downcasting.
struct ListColumnView {
    /// The flat values array backing this list column.
    values: ArrayRef,
    /// Pre-computed per-row start offsets (length = num_rows + 1).
    offsets: Vec<usize>,
    /// Null bitmap from the input array (None means no nulls).
    nulls: Option<NullBuffer>,
}

impl ListColumnView {
    fn is_null(&self, idx: usize) -> bool {
        self.nulls.as_ref().is_some_and(|n| n.is_null(idx))
    }
}

make_udf_expr_and_func!(
    ArraysZip,
    arrays_zip,
    "combines one or multiple arrays into a single array of structs.",
    arrays_zip_udf
);

#[user_doc(
    doc_section(label = "Array Functions"),
    description = "Returns an array of structs created by combining the elements of each input array at the same index. If the arrays have different lengths, shorter arrays are padded with NULLs.",
    syntax_example = "arrays_zip(array1[, ..., array_n])",
    sql_example = r#"```sql
> select arrays_zip([1, 2, 3]);
+---------------------------------------------------+
| arrays_zip([1, 2, 3])                             |
+---------------------------------------------------+
| [{1: 1}, {1: 2}, {1: 3}]                          |
+---------------------------------------------------+
> select arrays_zip([1, 2], [3, 4, 5]);
+---------------------------------------------------+
| arrays_zip([1, 2], [3, 4, 5])                     |
+---------------------------------------------------+
| [{1: 1, 2: 3}, {1: 2, 2: 4}, {1: NULL, 2: 5}]     |
+---------------------------------------------------+
```"#,
    argument(name = "array1", description = "First array expression."),
    argument(
        name = "array_n",
        description = "Optional additional array expressions."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArraysZip {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for ArraysZip {
    fn default() -> Self {
        Self::new()
    }
}

impl ArraysZip {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            aliases: vec![String::from("list_zip")],
        }
    }
}

impl ScalarUDFImpl for ArraysZip {
    fn name(&self) -> &str {
        "arrays_zip"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() {
            return exec_err!("arrays_zip requires at least one argument");
        }

        let mut fields = Vec::with_capacity(arg_types.len());
        for (i, arg_type) in arg_types.iter().enumerate() {
            let element_type = match arg_type {
                List(field) | LargeList(field) | FixedSizeList(field, _) => {
                    field.data_type().clone()
                }
                Null => Null,
                dt => {
                    return exec_err!("arrays_zip expects array arguments, got {dt}");
                }
            };
            fields.push(Field::new(format!("{}", i + 1), element_type, true));
        }

        Ok(List(Arc::new(Field::new_list_field(
            DataType::Struct(Fields::from(fields)),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(arrays_zip_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Core implementation for arrays_zip.
///
/// Takes N list arrays and produces a list of structs where each struct
/// has one field per input array. If arrays within a row have different
/// lengths, shorter arrays are padded with NULLs.
/// Supports List, LargeList, and Null input types.
fn arrays_zip_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.is_empty() {
        return exec_err!("arrays_zip requires at least one argument");
    }

    let num_rows = args[0].len();

    // Build a type-erased ListColumnView for each argument.
    // None means the argument is Null-typed (all nulls, no backing data).
    let mut views: Vec<Option<ListColumnView>> = Vec::with_capacity(args.len());
    let mut element_types: Vec<DataType> = Vec::with_capacity(args.len());

    for (i, arg) in args.iter().enumerate() {
        match arg.data_type() {
            List(field) => {
                let arr = as_list_array(arg)?;
                let raw_offsets = arr.value_offsets();
                let offsets: Vec<usize> =
                    raw_offsets.iter().map(|&o| o as usize).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    nulls: arr.nulls().cloned(),
                }));
            }
            LargeList(field) => {
                let arr = as_large_list_array(arg)?;
                let raw_offsets = arr.value_offsets();
                let offsets: Vec<usize> =
                    raw_offsets.iter().map(|&o| o as usize).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    nulls: arr.nulls().cloned(),
                }));
            }
            FixedSizeList(field, size) => {
                let arr = as_fixed_size_list_array(arg)?;
                let size = *size as usize;
                let offsets: Vec<usize> = (0..=num_rows).map(|row| row * size).collect();
                element_types.push(field.data_type().clone());
                views.push(Some(ListColumnView {
                    values: Arc::clone(arr.values()),
                    offsets,
                    nulls: arr.nulls().cloned(),
                }));
            }
            Null => {
                element_types.push(Null);
                views.push(None);
            }
            dt => {
                return exec_err!("arrays_zip argument {i} expected list type, got {dt}");
            }
        }
    }

    // Collect per-column values data for MutableArrayData builders.
    let values_data: Vec<_> = views
        .iter()
        .map(|v| v.as_ref().map(|view| view.values.to_data()))
        .collect();

    let struct_fields: Fields = element_types
        .iter()
        .enumerate()
        .map(|(i, dt)| Field::new(format!("{}", i + 1), dt.clone(), true))
        .collect::<Vec<_>>()
        .into();

    // Create a MutableArrayData builder per column. For None (Null-typed)
    // args we only need extend_nulls, so we track them separately.
    let mut builders: Vec<Option<MutableArrayData>> = values_data
        .iter()
        .map(|vd| {
            vd.as_ref().map(|data| {
                MutableArrayData::with_capacities(vec![data], true, Capacities::Array(0))
            })
        })
        .collect();

    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut null_builder = NullBufferBuilder::new(num_rows);
    let mut total_values: usize = 0;

    // Process each row: compute per-array lengths, then copy values
    // and pad shorter arrays with NULLs.
    for row_idx in 0..num_rows {
        let mut max_len: usize = 0;
        let mut all_null = true;

        for view in views.iter().flatten() {
            if !view.is_null(row_idx) {
                all_null = false;
                let len = view.offsets[row_idx + 1] - view.offsets[row_idx];
                max_len = max_len.max(len);
            }
        }

        if all_null {
            null_builder.append_null();
            offsets.push(*offsets.last().unwrap());
            continue;
        }
        null_builder.append_non_null();

        // Extend each column builder for this row.
        for (col_idx, view) in views.iter().enumerate() {
            match view {
                Some(v) if !v.is_null(row_idx) => {
                    let start = v.offsets[row_idx];
                    let end = v.offsets[row_idx + 1];
                    let len = end - start;
                    let builder = builders[col_idx].as_mut().unwrap();
                    builder.extend(0, start, end);
                    if len < max_len {
                        builder.extend_nulls(max_len - len);
                    }
                }
                _ => {
                    // Null list entry or None (Null-typed) arg — all nulls.
                    if let Some(builder) = builders[col_idx].as_mut() {
                        builder.extend_nulls(max_len);
                    }
                }
            }
        }

        total_values += max_len;
        let last = *offsets.last().unwrap();
        offsets.push(last + max_len as i32);
    }

    // Assemble struct columns from builders.
    let struct_columns: Vec<ArrayRef> = builders
        .into_iter()
        .zip(element_types.iter())
        .map(|(builder, elem_type)| match builder {
            Some(b) => arrow::array::make_array(b.freeze()),
            None => new_null_array(
                if elem_type.is_null() {
                    &Null
                } else {
                    elem_type
                },
                total_values,
            ),
        })
        .collect();

    let struct_array = StructArray::try_new(struct_fields, struct_columns, None)?;

    let null_buffer = null_builder.finish();

    let result = ListArray::try_new(
        Arc::new(Field::new_list_field(
            struct_array.data_type().clone(),
            true,
        )),
        OffsetBuffer::new(offsets.into()),
        Arc::new(struct_array),
        null_buffer,
    )?;

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::arrays_zip_inner;
    use arrow::array::{
        Array, ArrayRef, AsArray, FixedSizeListArray, Int64Array, ListArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Int64Type};
    use std::sync::Arc;

    /// Helper: from a `ListArray<Struct<...>>` output row, return the per-field
    /// vec of `Option<i64>` for the named struct field index.
    fn row_struct_field(
        list: &ListArray,
        row: usize,
        field_idx: usize,
    ) -> Vec<Option<i64>> {
        let row_array = list.value(row);
        let row_struct = row_array.as_any().downcast_ref::<StructArray>().unwrap();
        let col = row_struct
            .column(field_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..col.len())
            .map(|i| {
                if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                }
            })
            .collect()
    }

    #[test]
    fn test_sliced_list_input_uniform() {
        // Full data, then slice off the first and last rows.
        let a_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(10), Some(20), Some(30)]),
            Some(vec![Some(100), Some(200), Some(300)]),
            Some(vec![Some(1000), Some(2000), Some(3000)]),
        ]);
        let b_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(4), Some(5), Some(6)]),
            Some(vec![Some(40), Some(50), Some(60)]),
            Some(vec![Some(400), Some(500), Some(600)]),
            Some(vec![Some(4000), Some(5000), Some(6000)]),
        ]);
        let a: ArrayRef = Arc::new(a_full.slice(1, 2));
        let b: ArrayRef = Arc::new(b_full.slice(1, 2));

        let out = arrays_zip_inner(&[a, b]).unwrap();
        let list = out.as_list::<i32>();
        assert_eq!(list.len(), 2);
        assert_eq!(list.null_count(), 0);

        assert_eq!(
            row_struct_field(list, 0, 0),
            vec![Some(10), Some(20), Some(30)]
        );
        assert_eq!(
            row_struct_field(list, 0, 1),
            vec![Some(40), Some(50), Some(60)]
        );
        assert_eq!(
            row_struct_field(list, 1, 0),
            vec![Some(100), Some(200), Some(300)]
        );
        assert_eq!(
            row_struct_field(list, 1, 1),
            vec![Some(400), Some(500), Some(600)]
        );
    }

    #[test]
    fn test_sliced_list_input_with_padding() {
        // Mismatched lengths within the slice window so extend_nulls fires.
        let a_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(10), Some(20)]), // visible row 0
            Some(vec![Some(100)]),          // visible row 1
            Some(vec![Some(1000), Some(2000)]),
        ]);
        let b_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(2)]),
            Some(vec![Some(40)]), // visible row 0: shorter than a
            Some(vec![Some(400), Some(500), Some(600)]), // visible row 1: longer than a
            Some(vec![Some(4000)]),
        ]);
        let a: ArrayRef = Arc::new(a_full.slice(1, 2));
        let b: ArrayRef = Arc::new(b_full.slice(1, 2));

        let out = arrays_zip_inner(&[a, b]).unwrap();
        let list = out.as_list::<i32>();
        assert_eq!(list.len(), 2);

        // row 0: a=[10,20], b=[40] → max_len=2, expect [(10,40),(20,NULL)]
        assert_eq!(row_struct_field(list, 0, 0), vec![Some(10), Some(20)]);
        assert_eq!(row_struct_field(list, 0, 1), vec![Some(40), None]);

        // row 1: a=[100], b=[400,500,600] → max_len=3, expect [(100,400),(NULL,500),(NULL,600)]
        assert_eq!(row_struct_field(list, 1, 0), vec![Some(100), None, None]);
        assert_eq!(
            row_struct_field(list, 1, 1),
            vec![Some(400), Some(500), Some(600)]
        );
    }

    #[test]
    fn test_sliced_list_with_list_level_nulls() {
        // Slice contains both list-level null rows and a non-null row.
        let a_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),          // hidden
            None,                         // visible row 0: a null, b non-null
            Some(vec![Some(5), Some(6)]), // visible row 1: a non-null, b null
            Some(vec![Some(9)]),          // hidden
        ]);
        let b_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(2)]),
            Some(vec![Some(30), Some(40)]),
            None,
            Some(vec![Some(99)]),
        ]);
        let a: ArrayRef = Arc::new(a_full.slice(1, 2));
        let b: ArrayRef = Arc::new(b_full.slice(1, 2));

        let out = arrays_zip_inner(&[a, b]).unwrap();
        let list = out.as_list::<i32>();
        assert_eq!(list.len(), 2);
        assert_eq!(list.null_count(), 0);

        // row 0: a NULL, b=[30,40] → max_len=2, a side all NULL
        assert_eq!(row_struct_field(list, 0, 0), vec![None, None]);
        assert_eq!(row_struct_field(list, 0, 1), vec![Some(30), Some(40)]);

        // row 1: a=[5,6], b NULL → max_len=2, b side all NULL
        assert_eq!(row_struct_field(list, 1, 0), vec![Some(5), Some(6)]);
        assert_eq!(row_struct_field(list, 1, 1), vec![None, None]);
    }

    #[test]
    fn test_sliced_list_with_all_null_row() {
        // Slice contains one all-null row sandwiched between non-null rows.
        let a_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(10)]),  // visible row 0
            None,                  // visible row 1: ALL NULL
            Some(vec![Some(100)]), // visible row 2
            Some(vec![Some(1000)]),
        ]);
        let b_full = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(2)]),
            Some(vec![Some(20)]),
            None,
            Some(vec![Some(200)]),
            Some(vec![Some(2000)]),
        ]);
        let a: ArrayRef = Arc::new(a_full.slice(1, 3));
        let b: ArrayRef = Arc::new(b_full.slice(1, 3));

        let out = arrays_zip_inner(&[a, b]).unwrap();
        let list = out.as_list::<i32>();
        assert_eq!(list.len(), 3);
        // The middle row's outer list is NULL.
        assert!(!list.is_null(0));
        assert!(list.is_null(1));
        assert!(!list.is_null(2));

        assert_eq!(row_struct_field(list, 0, 0), vec![Some(10)]);
        assert_eq!(row_struct_field(list, 0, 1), vec![Some(20)]);
        assert_eq!(row_struct_field(list, 2, 0), vec![Some(100)]);
        assert_eq!(row_struct_field(list, 2, 1), vec![Some(200)]);
    }

    #[test]
    fn test_sliced_fixed_size_list_input() {
        // FixedSizeList: each row has exactly `size` elements; slice cuts rows.
        let a_full_values = Int64Array::from(vec![1, 2, 10, 20, 100, 200, 1000, 2000]);
        let a_full = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            2,
            Arc::new(a_full_values),
            None,
        );
        let b_full_values = Int64Array::from(vec![3, 4, 30, 40, 300, 400, 3000, 4000]);
        let b_full = FixedSizeListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            2,
            Arc::new(b_full_values),
            None,
        );
        let a: ArrayRef = Arc::new(a_full.slice(1, 2));
        let b: ArrayRef = Arc::new(b_full.slice(1, 2));

        let out = arrays_zip_inner(&[a, b]).unwrap();
        let list = out.as_list::<i32>();
        assert_eq!(list.len(), 2);

        assert_eq!(row_struct_field(list, 0, 0), vec![Some(10), Some(20)]);
        assert_eq!(row_struct_field(list, 0, 1), vec![Some(30), Some(40)]);
        assert_eq!(row_struct_field(list, 1, 0), vec![Some(100), Some(200)]);
        assert_eq!(row_struct_field(list, 1, 1), vec![Some(300), Some(400)]);
    }
}
