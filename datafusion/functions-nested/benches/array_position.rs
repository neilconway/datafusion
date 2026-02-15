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

use arrow::array::{ArrayRef, Int64Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::position::ArrayPosition;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::Rng;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 10000;
const ARRAY_SIZES: &[usize] = &[10, 100, 500];
const SEED: u64 = 42;
const NULL_DENSITY: f64 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    bench_array_position_int64(c);
    bench_array_position_strings(c);
}

fn bench_array_position_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_position_int64");

    for &array_size in ARRAY_SIZES {
        let list_array = create_int64_list_array(NUM_ROWS, array_size, NULL_DENSITY);

        // Search for element value 1, which exists in most rows
        let element = ScalarValue::Int64(Some(1));
        let args = vec![
            ColumnarValue::Array(list_array.clone()),
            ColumnarValue::Scalar(element),
        ];

        group.bench_with_input(
            BenchmarkId::new("scalar_element", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayPosition::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Int64, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new("result", DataType::UInt64, true)
                                .into(),
                            config_options: Arc::new(ConfigOptions::default()),
                        })
                        .unwrap(),
                    )
                })
            },
        );

        // Search for element that doesn't exist (worst case: scans all elements)
        let element_missing = ScalarValue::Int64(Some(-999));
        let args_missing = vec![
            ColumnarValue::Array(list_array.clone()),
            ColumnarValue::Scalar(element_missing),
        ];

        group.bench_with_input(
            BenchmarkId::new("scalar_element_not_found", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayPosition::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args_missing.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Int64, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new("result", DataType::UInt64, true)
                                .into(),
                            config_options: Arc::new(ConfigOptions::default()),
                        })
                        .unwrap(),
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_array_position_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_position_strings");

    for &array_size in ARRAY_SIZES {
        let list_array = create_string_list_array(NUM_ROWS, array_size, NULL_DENSITY);
        let element = ScalarValue::Utf8(Some("value_1".to_string()));
        let args = vec![
            ColumnarValue::Array(list_array.clone()),
            ColumnarValue::Scalar(element),
        ];

        group.bench_with_input(
            BenchmarkId::new("scalar_element", array_size),
            &array_size,
            |b, _| {
                let udf = ArrayPosition::new();
                b.iter(|| {
                    black_box(
                        udf.invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: vec![
                                Field::new("arr", list_array.data_type().clone(), false)
                                    .into(),
                                Field::new("el", DataType::Utf8, false).into(),
                            ],
                            number_rows: NUM_ROWS,
                            return_field: Field::new("result", DataType::UInt64, true)
                                .into(),
                            config_options: Arc::new(ConfigOptions::default()),
                        })
                        .unwrap(),
                    )
                })
            },
        );
    }

    group.finish();
}

fn create_int64_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows * array_size)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                Some(rng.random_range(0..array_size as i64))
            }
        })
        .collect::<Int64Array>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

fn create_string_list_array(
    num_rows: usize,
    array_size: usize,
    null_density: f64,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows * array_size)
        .map(|_| {
            if rng.random::<f64>() < null_density {
                None
            } else {
                let idx = rng.random_range(0..array_size);
                Some(format!("value_{idx}"))
            }
        })
        .collect::<StringArray>();
    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
