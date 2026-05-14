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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::arrays_zip::ArraysZip;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NUM_ROWS: usize = 8192;
const LIST_SIZE: usize = 10;
const SEED: u64 = 42;

/// Build a ListArray of Int64 with one row per entry in `list_sizes`.
/// If `null_density > 0`, that fraction of rows will be null at the
/// list level.
fn make_list_array(
    rng: &mut StdRng,
    list_sizes: &[usize],
    null_density: f64,
) -> ArrayRef {
    let num_rows = list_sizes.len();
    let total: usize = list_sizes.iter().sum();
    let values: Vec<i64> = (0..total).map(|_| rng.random_range(0..1000i64)).collect();
    let values_array = Arc::new(Int64Array::from(values)) as ArrayRef;

    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    offsets.push(0);
    let mut acc: i32 = 0;
    for &size in list_sizes {
        acc += size as i32;
        offsets.push(acc);
    }

    let nulls = if null_density > 0.0 {
        let valid: Vec<bool> = (0..num_rows)
            .map(|_| rng.random::<f64>() >= null_density)
            .collect();
        Some(NullBuffer::from(valid))
    } else {
        None
    };

    Arc::new(
        ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int64, true)),
            OffsetBuffer::new(offsets.into()),
            values_array,
            nulls,
        )
        .unwrap(),
    )
}

/// Three columns of `NUM_ROWS` rows where every row in every column
/// has exactly `LIST_SIZE` elements (the padding path is never taken).
fn uniform_arrays(null_density: f64) -> [ArrayRef; 3] {
    let mut rng = StdRng::seed_from_u64(SEED);
    let sizes = vec![LIST_SIZE; NUM_ROWS];
    [
        make_list_array(&mut rng, &sizes, null_density),
        make_list_array(&mut rng, &sizes, null_density),
        make_list_array(&mut rng, &sizes, null_density),
    ]
}

/// Three columns where each row's list length is drawn independently
/// from 5..=15 per column. Mean length stays at LIST_SIZE = 10, but
/// columns disagree on most rows so `extend_nulls` padding runs
/// frequently and per-row `max_len` varies.
fn varying_arrays(null_density: f64) -> [ArrayRef; 3] {
    let mut rng = StdRng::seed_from_u64(SEED);
    let sizes: Vec<Vec<usize>> = (0..3)
        .map(|_| (0..NUM_ROWS).map(|_| rng.random_range(5..=15)).collect())
        .collect();
    [
        make_list_array(&mut rng, &sizes[0], null_density),
        make_list_array(&mut rng, &sizes[1], null_density),
        make_list_array(&mut rng, &sizes[2], null_density),
    ]
}

fn bench_arrays_zip(c: &mut Criterion, name: &str, arrays: &[ArrayRef; 3]) {
    let udf = ArraysZip::new();
    let args_vec = vec![
        ColumnarValue::Array(Arc::clone(&arrays[0])),
        ColumnarValue::Array(Arc::clone(&arrays[1])),
        ColumnarValue::Array(Arc::clone(&arrays[2])),
    ];
    let return_type = udf
        .return_type(&[
            arrays[0].data_type().clone(),
            arrays[1].data_type().clone(),
            arrays[2].data_type().clone(),
        ])
        .unwrap();
    let return_field = Arc::new(Field::new("f", return_type, true));
    let arg_fields: Vec<_> = arrays
        .iter()
        .map(|a| Arc::new(Field::new("a", a.data_type().clone(), true)))
        .collect();
    let config_options = Arc::new(ConfigOptions::default());
    let num_rows = arrays[0].len();

    c.bench_function(name, |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: args_vec.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: num_rows,
                    return_field: Arc::clone(&return_field),
                    config_options: Arc::clone(&config_options),
                })
                .expect("arrays_zip should work"),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    bench_arrays_zip(c, "arrays_zip_no_nulls_8192", &uniform_arrays(0.0));
    bench_arrays_zip(c, "arrays_zip_10pct_nulls_8192", &uniform_arrays(0.1));
    bench_arrays_zip(c, "arrays_zip_varying_no_nulls_8192", &varying_arrays(0.0));
    bench_arrays_zip(
        c,
        "arrays_zip_varying_10pct_nulls_8192",
        &varying_arrays(0.1),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
