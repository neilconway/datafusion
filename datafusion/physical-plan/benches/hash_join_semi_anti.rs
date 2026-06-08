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

//! Criterion benchmarks for Hash Join with RightSemi/RightAnti joins with Int32 keys.
//!
//! ## Key Benchmark Axes
//!
//! - **Density**: How tightly distinct keys pack into their numeric range.
//!   `density = num_distinct_keys / (max_key - min_key + 1)`.
//!   Examples for 5 distinct keys:
//!     - `[0, 1, 2, 3, 4]`      → 5/5  = 100% (fully packed)
//!     - `[0, 2, 4, 6, 8]`      → 5/9  ≈  55% (every 2nd slot)
//!     - `[0, 10, 20, 30, 40]`  → 5/41 ≈  12% (every 10th slot)
//!
//!   Why it matters for this workload: future potential semi/anti-join
//!   fast paths could exploit densely packed build keys to outperform the
//!   general hash-table path, which is largely insensitive to density.
//!   Varying density across benchmarks helps surface those potential gains
//!   under different key distributions. Density describes only the
//!   build-side key layout; the per-probe match count is tracked
//!   separately as fanout.
//!
//! - **Hit Rate**: The percentage of probe rows that find a match in the build side.
//!   This controls how often the join produces output rows.
//!
//! Semi/anti joins can short-circuit after finding the first match, so these
//! benchmarks help evaluate optimization strategies for existence checks.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use datafusion_common::{JoinType, NullEquality};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, utils::JoinOn};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tokio::runtime::Runtime;

const ARRAY_MAP_CREATED_COUNT_METRIC_NAME: &str = "array_map_created_count";

/// Build RecordBatches with Int32 keys.
///
/// Schema: (key: Int32, data: Int32, payload: Utf8)
///
/// `key_mod` controls distinct key count: key = row_index % key_mod.
/// `key_offset` shifts keys to control hit rate.
fn build_batches(
    num_rows: usize,
    key_mod: usize,
    key_offset: i32,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    build_batches_from_key_fn(num_rows, schema, |i| ((i % key_mod) as i32) + key_offset)
}

fn make_exec(batches: &[RecordBatch], schema: &SchemaRef) -> Arc<dyn ExecutionPlan> {
    TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(schema), None).unwrap()
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("data", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

fn do_hash_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: JoinType,
    rt: &Runtime,
) -> HashJoinRun {
    let on: JoinOn = vec![(
        col("key", &left.schema()).unwrap(),
        col("key", &right.schema()).unwrap(),
    )];
    let join = Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            on,
            None,
            &join_type,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );

    let task_ctx = Arc::new(TaskContext::default());
    let output_rows = rt.block_on(async {
        let batches = collect(join.clone(), task_ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    });
    let metrics = join.metrics().unwrap();
    let array_map_created_count = metrics
        .sum_by_name(ARRAY_MAP_CREATED_COUNT_METRIC_NAME)
        .map(|v| v.as_usize())
        .unwrap_or(0);

    HashJoinRun {
        output_rows,
        array_map_created_count,
    }
}

struct HashJoinRun {
    output_rows: usize,
    array_map_created_count: usize,
}

#[derive(Clone, Copy)]
struct CaseExpectation {
    output_rows: usize,
    uses_array_map: bool,
}

struct BenchCase {
    name: &'static str,
    probe_rows: usize,
    left_batches: Vec<RecordBatch>,
    right_batches: Vec<RecordBatch>,
    join_type: JoinType,
    expected: CaseExpectation,
}

fn validate_hash_join_case(
    name: &str,
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    schema: &SchemaRef,
    join_type: JoinType,
    expected: CaseExpectation,
    rt: &Runtime,
) {
    let left = make_exec(left_batches, schema);
    let right = make_exec(right_batches, schema);
    let run = do_hash_join(left, right, join_type, rt);
    assert_eq!(
        run.output_rows, expected.output_rows,
        "unexpected output row count for {name}"
    );
    assert_eq!(
        run.array_map_created_count > 0,
        expected.uses_array_map,
        "unexpected map strategy for {name}"
    );
}

fn bench_hash_join_case(
    group: &mut BenchmarkGroup<'_, WallTime>,
    schema: &SchemaRef,
    rt: &Runtime,
    case: BenchCase,
) {
    let BenchCase {
        name,
        probe_rows,
        left_batches,
        right_batches,
        join_type,
        expected,
    } = case;

    validate_hash_join_case(
        name,
        &left_batches,
        &right_batches,
        schema,
        join_type,
        expected,
        rt,
    );

    group.bench_function(BenchmarkId::new(name, probe_rows), |b| {
        b.iter(|| {
            let left = make_exec(&left_batches, schema);
            let right = make_exec(&right_batches, schema);
            do_hash_join(left, right, join_type, rt).output_rows
        })
    });
}

/// Build batches with sparse keys (key = row_index % key_mod * multiplier + key_offset).
/// The `multiplier` controls density: 1 = 100%, 2 = 50%, 10 = 10%.
fn build_batches_sparse(
    num_rows: usize,
    key_mod: usize,
    key_offset: i32,
    multiplier: i32,
    schema: &SchemaRef,
) -> Vec<RecordBatch> {
    build_batches_from_key_fn(num_rows, schema, |i| {
        ((i % key_mod) as i32) * multiplier + key_offset
    })
}

/// Build batches from an arbitrary deterministic key generator.
fn build_batches_from_key_fn<F>(
    num_rows: usize,
    schema: &SchemaRef,
    mut key_fn: F,
) -> Vec<RecordBatch>
where
    F: FnMut(usize) -> i32,
{
    let keys: Vec<i32> = (0..num_rows).map(&mut key_fn).collect();
    let data: Vec<i32> = (0..num_rows).map(|i| i as i32).collect();
    let payload: Vec<String> = data.iter().map(|d| format!("val_{d}")).collect();

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(data)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .unwrap();

    let batch_size = 8192;
    let mut batches = Vec::new();
    let mut offset = 0;
    while offset < batch.num_rows() {
        let len = (batch.num_rows() - offset).min(batch_size);
        batches.push(batch.slice(offset, len));
        offset += len;
    }
    batches
}

/// Skewed 50%-hit key generator for a sparse (HashMap) build side whose keys are
/// spaced by 10. Matching keys land on multiples of 10; misses are offset by 1.
fn skewed_h50_hashmap_key(row: usize, build_rows: usize, hot_keys: usize) -> i32 {
    let logical_row = row / 2;
    if row.is_multiple_of(2) {
        let key = if logical_row % 10 < 8 {
            logical_row % hot_keys
        } else {
            hot_keys + logical_row % (build_rows - hot_keys)
        };
        (key as i32) * 10
    } else {
        ((logical_row % build_rows) as i32) * 10 + 1
    }
}

fn bench_hash_join_semi_anti(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let s = schema();

    let mut group = c.benchmark_group("hash_join_semi_anti");

    // Build side: 100K rows, Probe side: 1M rows
    // Matching ratio: 1:1 (build keys are unique, each probe matches at most 1 build row)
    let build_rows = 100_000;
    let probe_rows = 1_000_000;

    // =========================================================================
    // RightSemi Join benchmarks
    // =========================================================================

    // RightSemi - 100% Density, 100% hit rate
    // Keys: 0..100K contiguous, all probe rows find a match
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d100_h100",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - 100% Density, 10% hit rate
    // Keys: 0..100K contiguous, only 10% of probe rows find a match
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows * 10, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d100_h10",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: build_rows,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - 100% Density, 50% hit rate
    // Keys: 0..100K contiguous, half of probe rows find a match.
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows * 2, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d100_h50",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - 50% Density, 100% hit rate
    // Keys: 0, 2, 4, ... (sparse, multiplier=2), all probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 2, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d50_h100",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - 50% Density, 10% hit rate
    // Keys: 0, 2, 4, ... (sparse), only 10% of probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 2, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d50_h10",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: build_rows,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - 10% Density, 100% hit rate
    // Keys: 0, 10, 20, ... (very sparse, multiplier=10), all probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 10, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d10_h100",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightSemi - 10% Density, 10% hit rate
    // Keys: 0, 10, 20, ... (very sparse), only 10% of probe rows find a match
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 10, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d10_h10",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: build_rows,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightSemi - 10% Density, 50% hit rate
    // Sparse build keys force the HashMap path while avoiding all-match/no-match extremes.
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 2, 0, 10, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_d10_h50",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightSemi - 100% Density, ~1% hit rate, fanout ~100
    // Build keys are duplicated: 100K rows over 1K distinct keys. Matching
    // probe rows produce many duplicate probe indices before RightSemi
    // deduplication.
    {
        let fanout_keys = 1_000;
        let left_batches = build_batches(build_rows, fanout_keys, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_fanout100_h1",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows / 100,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - 100% Density, 50% hit rate, fanout ~10
    // This is a moderate duplicate-heavy case: enough fanout to stress pair
    // materialization, without relying on a tiny match rate.
    {
        let fanout_keys = 10_000;
        let left_batches = build_batches(build_rows, fanout_keys, 0, &s);
        let right_batches = build_batches(probe_rows, fanout_keys * 2, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_fanout10_h50",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightSemi - HashMap path, 50% hit rate, fanout ~10
    // A large sparse key range prevents ArrayMap selection while preserving the
    // same moderate fanout and hit-rate shape.
    {
        let fanout_keys = 10_000;
        let left_batches = build_batches_sparse(build_rows, fanout_keys, 0, 1000, &s);
        let right_batches =
            build_batches_sparse(probe_rows, fanout_keys * 2, 0, 1000, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_fanout10_h50_hashmap",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightSemi - HashMap path, skewed 50% hit rate
    {
        let hot_keys = 1_000;
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_from_key_fn(probe_rows, &s, |row| {
            skewed_h50_hashmap_key(row, build_rows, hot_keys)
        });
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_semi_skewed_h50_hashmap",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightSemi,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: false,
                },
            },
        );
    }

    // =========================================================================
    // RightAnti Join benchmarks
    // =========================================================================

    // RightAnti - 100% Density, 100% hit rate (no output)
    // Keys: 0..100K contiguous, all probe rows find a match -> no output
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d100_h100",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: 0,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - 100% Density, 10% hit rate (90% output)
    // Keys: 0..100K contiguous, only 10% of probe rows find a match -> 90% output
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows * 10, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d100_h10",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows - build_rows,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - 100% Density, 50% hit rate
    // Keys: 0..100K contiguous, half of probe rows find a match.
    {
        let left_batches = build_batches(build_rows, build_rows, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows * 2, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d100_h50",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - 50% Density, 100% hit rate (no output)
    // Keys: 0, 2, 4, ... (sparse), all probe rows find a match -> no output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 2, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d50_h100",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: 0,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - 50% Density, 10% hit rate (90% output)
    // Keys: 0, 2, 4, ... (sparse), only 10% of probe rows find a match -> 90% output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 2, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 2, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d50_h10",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows - build_rows,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - 10% Density, 100% hit rate (no output)
    // Keys: 0, 10, 20, ... (very sparse), all probe rows find a match -> no output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows, 0, 10, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d10_h100",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: 0,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightAnti - 10% Density, 10% hit rate (90% output)
    // Keys: 0, 10, 20, ... (very sparse), only 10% of probe rows find a match -> 90% output
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 10, 0, 10, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d10_h10",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows - build_rows,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightAnti - 10% Density, 50% hit rate
    // Sparse build keys force the HashMap path while avoiding all-match/no-match extremes.
    {
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_sparse(probe_rows, build_rows * 2, 0, 10, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_d10_h50",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightAnti - 100% Density, ~1% hit rate, fanout ~100
    // Build keys are duplicated: 100K rows over 1K distinct keys. Matching
    // probe rows produce many duplicate probe indices before RightAnti
    // computes the unmatched probe rows.
    {
        let fanout_keys = 1_000;
        let left_batches = build_batches(build_rows, fanout_keys, 0, &s);
        let right_batches = build_batches(probe_rows, build_rows, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_fanout100_h1",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows - (probe_rows / 100),
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - 100% Density, 50% hit rate, fanout ~10
    {
        let fanout_keys = 10_000;
        let left_batches = build_batches(build_rows, fanout_keys, 0, &s);
        let right_batches = build_batches(probe_rows, fanout_keys * 2, 0, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_fanout10_h50",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: true,
                },
            },
        );
    }

    // RightAnti - HashMap path, 50% hit rate, fanout ~10
    {
        let fanout_keys = 10_000;
        let left_batches = build_batches_sparse(build_rows, fanout_keys, 0, 1000, &s);
        let right_batches =
            build_batches_sparse(probe_rows, fanout_keys * 2, 0, 1000, &s);
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_fanout10_h50_hashmap",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: false,
                },
            },
        );
    }

    // RightAnti - HashMap path, skewed 50% hit rate
    {
        let hot_keys = 1_000;
        let left_batches = build_batches_sparse(build_rows, build_rows, 0, 10, &s);
        let right_batches = build_batches_from_key_fn(probe_rows, &s, |row| {
            skewed_h50_hashmap_key(row, build_rows, hot_keys)
        });
        bench_hash_join_case(
            &mut group,
            &s,
            &rt,
            BenchCase {
                name: "right_anti_skewed_h50_hashmap",
                probe_rows,
                left_batches,
                right_batches,
                join_type: JoinType::RightAnti,
                expected: CaseExpectation {
                    output_rows: probe_rows / 2,
                    uses_array_map: false,
                },
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_hash_join_semi_anti);
criterion_main!(benches);
