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

//! Benchmarks for uncorrelated scalar subquery evaluation.
//!
//! Measures the overhead of subquery execution machinery by using simple
//! arithmetic and comparison operators that don't have specialized scalar
//! fast paths, keeping the comparison between the old (join-based) and new
//! (ScalarSubqueryExec-based) approaches apples-to-apples.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn query(ctx: &SessionContext, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(num_rows: usize) -> Result<SessionContext> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));

    let batch_size = 4096;
    let batches = (0..num_rows / batch_size)
        .map(|i| {
            let values: Vec<i64> =
                ((i * batch_size) as i64..((i + 1) * batch_size) as i64).collect();
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                .unwrap()
        })
        .collect::<Vec<_>>();

    // Small lookup table for the subquery to read from.
    let sq_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let sq_batch = RecordBatch::try_new(
        sq_schema.clone(),
        vec![Arc::new(Int64Array::from(vec![10, 20, 30]))],
    )?;

    let ctx = SessionContext::new();
    ctx.register_table(
        "main_t",
        Arc::new(MemTable::try_new(schema, vec![batches])?),
    )?;
    ctx.register_table(
        "lookup",
        Arc::new(MemTable::try_new(sq_schema, vec![vec![sq_batch]])?),
    )?;

    Ok(ctx)
}

fn criterion_benchmark(c: &mut Criterion) {
    let num_rows = 1_048_576; // 2^20
    let rt = Runtime::new().unwrap();

    // Scalar subquery in a filter (WHERE clause).
    c.bench_function("scalar_subquery_filter", |b| {
        let ctx = create_context(num_rows).unwrap();
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "SELECT x FROM main_t WHERE x > (SELECT max(v) FROM lookup)",
            )
        })
    });

    // Scalar subquery in a projection (SELECT expression).
    c.bench_function("scalar_subquery_projection", |b| {
        let ctx = create_context(num_rows).unwrap();
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "SELECT x + (SELECT max(v) FROM lookup) AS y FROM main_t",
            )
        })
    });

    // Two scalar subqueries in one query.
    c.bench_function("scalar_subquery_two_subqueries", |b| {
        let ctx = create_context(num_rows).unwrap();
        b.iter(|| {
            query(
                &ctx,
                &rt,
                "SELECT x FROM main_t \
                 WHERE x > (SELECT min(v) FROM lookup) \
                   AND x < (SELECT max(v) FROM lookup) + 1000000",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
