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

//! Debug-only assertions for statistics propagation contracts.

#[cfg(debug_assertions)]
use arrow::datatypes::Schema;
#[cfg(debug_assertions)]
use datafusion_common::stats::Precision;
#[cfg(debug_assertions)]
use datafusion_common::{ColumnStatistics, JoinType, Result, ScalarValue, Statistics};
#[cfg(debug_assertions)]
use datafusion_common::{plan_err, stats::Precision::Absent};

#[cfg(debug_assertions)]
pub(crate) fn assert_filter_statistics(
    input: &Statistics,
    output: &Statistics,
    schema: &Schema,
) {
    output.debug_assert_valid_for_schema(schema, "FilterExec statistics");
    if let Err(e) = validate_filter_statistics(input, output) {
        panic!("Invalid filter statistics: {e}\nInput: {input}\nOutput: {output}");
    }
}

#[cfg(debug_assertions)]
fn validate_filter_statistics(input: &Statistics, output: &Statistics) -> Result<()> {
    if let (Some(input_rows), Some(output_rows)) =
        (input.num_rows.get_value(), output.num_rows.get_value())
        && output_rows > input_rows
    {
        return plan_err!(
            "filter increased row count from {input_rows} to {output_rows}"
        );
    }

    for (idx, (input_col, output_col)) in input
        .column_statistics
        .iter()
        .zip(&output.column_statistics)
        .enumerate()
    {
        validate_not_increased(
            idx,
            "null_count",
            input_col.null_count,
            output_col.null_count,
        )?;
        validate_not_increased(
            idx,
            "distinct_count",
            input_col.distinct_count,
            output_col.distinct_count,
        )?;
        validate_min_not_widened(idx, input_col, output_col)?;
        validate_max_not_widened(idx, input_col, output_col)?;
    }

    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) fn assert_union_statistics(
    inputs: &[Statistics],
    output: &Statistics,
    schema: &Schema,
) {
    output.debug_assert_valid_for_schema(schema, "UnionExec statistics");
    if let Err(e) = validate_union_statistics(inputs, output) {
        panic!("Invalid union statistics: {e}\nOutput: {output}");
    }
}

#[cfg(debug_assertions)]
fn validate_union_statistics(inputs: &[Statistics], output: &Statistics) -> Result<()> {
    let expected_rows = inputs
        .iter()
        .map(|stats| stats.num_rows)
        .reduce(|acc, rows| acc.add(&rows))
        .unwrap_or(Absent);
    if expected_rows != Absent && output.num_rows != expected_rows {
        return plan_err!(
            "union row count {} does not equal input sum {}",
            output.num_rows,
            expected_rows
        );
    }

    for (idx, output_col) in output.column_statistics.iter().enumerate() {
        let expected_nulls = inputs
            .iter()
            .map(|stats| stats.column_statistics[idx].null_count)
            .reduce(|acc, nulls| acc.add(&nulls))
            .unwrap_or(Absent);
        if expected_nulls != Absent && output_col.null_count != expected_nulls {
            return plan_err!(
                "union column {idx} null_count {} does not equal input sum {}",
                output_col.null_count,
                expected_nulls
            );
        }

        for input in inputs {
            let input_col = &input.column_statistics[idx];
            validate_output_min_contains_input_min(idx, output_col, input_col)?;
            validate_output_max_contains_input_max(idx, output_col, input_col)?;
        }
    }

    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) fn assert_join_statistics(
    left: &Statistics,
    right: &Statistics,
    join_type: &JoinType,
    output: &Statistics,
    schema: &Schema,
) {
    output.debug_assert_valid_for_schema(schema, "join statistics");
    if let Err(e) = validate_join_statistics(left, right, join_type, output) {
        panic!(
            "Invalid join statistics: {e}\nLeft: {left}\nRight: {right}\nOutput: {output}"
        );
    }
}

#[cfg(debug_assertions)]
fn validate_join_statistics(
    left: &Statistics,
    right: &Statistics,
    join_type: &JoinType,
    output: &Statistics,
) -> Result<()> {
    let Some(&output_rows) = output.num_rows.get_value() else {
        return Ok(());
    };

    match join_type {
        JoinType::Inner => {
            if let Some(product) = row_product(left, right)
                && output_rows > product
            {
                return plan_err!(
                    "inner join row count {output_rows} exceeds cartesian product {product}"
                );
            }
        }
        JoinType::Left => validate_at_least("left outer join", output_rows, left)?,
        JoinType::Right => validate_at_least("right outer join", output_rows, right)?,
        JoinType::Full => {
            validate_at_least("full outer join", output_rows, left)?;
            validate_at_least("full outer join", output_rows, right)?;
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            validate_at_most("left semi/anti join", output_rows, left)?
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            validate_at_most("right semi/anti join", output_rows, right)?
        }
        JoinType::LeftMark => validate_equal("left mark join", output_rows, left)?,
        JoinType::RightMark => validate_equal("right mark join", output_rows, right)?,
    }

    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) fn assert_cross_join_statistics(
    left: &Statistics,
    right: &Statistics,
    output: &Statistics,
    schema: &Schema,
) {
    output.debug_assert_valid_for_schema(schema, "CrossJoinExec statistics");
    if let Err(e) = validate_cross_join_statistics(left, right, output) {
        panic!(
            "Invalid cross join statistics: {e}\nLeft: {left}\nRight: {right}\nOutput: {output}"
        );
    }
}

#[cfg(debug_assertions)]
fn validate_cross_join_statistics(
    left: &Statistics,
    right: &Statistics,
    output: &Statistics,
) -> Result<()> {
    let expected_rows = left.num_rows.multiply(&right.num_rows);
    if expected_rows != Absent && output.num_rows != expected_rows {
        return plan_err!(
            "cross join row count {} does not equal product {}",
            output.num_rows,
            expected_rows
        );
    }
    if output.num_rows == Precision::Exact(0) {
        return Ok(());
    }

    let left_col_count = left.column_statistics.len();
    for (idx, input_col) in left.column_statistics.iter().enumerate() {
        let output_col = &output.column_statistics[idx];
        validate_scaled_null_count(idx, input_col, output_col, right.num_rows)?;
        validate_preserved_scalar(
            idx,
            "min_value",
            &input_col.min_value,
            &output_col.min_value,
        )?;
        validate_preserved_scalar(
            idx,
            "max_value",
            &input_col.max_value,
            &output_col.max_value,
        )?;
        validate_preserved_usize(
            idx,
            "distinct_count",
            input_col.distinct_count,
            output_col.distinct_count,
        )?;
    }

    for (right_idx, input_col) in right.column_statistics.iter().enumerate() {
        let output_idx = left_col_count + right_idx;
        let output_col = &output.column_statistics[output_idx];
        validate_scaled_null_count(output_idx, input_col, output_col, left.num_rows)?;
        validate_preserved_scalar(
            output_idx,
            "min_value",
            &input_col.min_value,
            &output_col.min_value,
        )?;
        validate_preserved_scalar(
            output_idx,
            "max_value",
            &input_col.max_value,
            &output_col.max_value,
        )?;
        validate_preserved_usize(
            output_idx,
            "distinct_count",
            input_col.distinct_count,
            output_col.distinct_count,
        )?;
    }

    Ok(())
}

#[cfg(debug_assertions)]
fn validate_not_increased(
    idx: usize,
    stat_name: &str,
    input: Precision<usize>,
    output: Precision<usize>,
) -> Result<()> {
    if let (Some(input), Some(output)) = (input.get_value(), output.get_value())
        && output > input
    {
        return plan_err!("column {idx} {stat_name} increased from {input} to {output}");
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_min_not_widened(
    idx: usize,
    input: &ColumnStatistics,
    output: &ColumnStatistics,
) -> Result<()> {
    if let (Some(input_min), Some(output_min)) =
        (input.min_value.get_value(), output.min_value.get_value())
        && comparable(input_min, output_min)
        && output_min < input_min
    {
        return plan_err!(
            "column {idx} min_value widened from {input_min:?} to {output_min:?}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_max_not_widened(
    idx: usize,
    input: &ColumnStatistics,
    output: &ColumnStatistics,
) -> Result<()> {
    if let (Some(input_max), Some(output_max)) =
        (input.max_value.get_value(), output.max_value.get_value())
        && comparable(input_max, output_max)
        && output_max > input_max
    {
        return plan_err!(
            "column {idx} max_value widened from {input_max:?} to {output_max:?}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_output_min_contains_input_min(
    idx: usize,
    output: &ColumnStatistics,
    input: &ColumnStatistics,
) -> Result<()> {
    if let (Some(output_min), Some(input_min)) =
        (output.min_value.get_value(), input.min_value.get_value())
        && comparable(output_min, input_min)
        && output_min > input_min
    {
        return plan_err!(
            "union column {idx} output min {output_min:?} does not contain input min {input_min:?}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_output_max_contains_input_max(
    idx: usize,
    output: &ColumnStatistics,
    input: &ColumnStatistics,
) -> Result<()> {
    if let (Some(output_max), Some(input_max)) =
        (output.max_value.get_value(), input.max_value.get_value())
        && comparable(output_max, input_max)
        && output_max < input_max
    {
        return plan_err!(
            "union column {idx} output max {output_max:?} does not contain input max {input_max:?}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_scaled_null_count(
    idx: usize,
    input: &ColumnStatistics,
    output: &ColumnStatistics,
    multiplier: Precision<usize>,
) -> Result<()> {
    let expected = input.null_count.multiply(&multiplier);
    if expected != Absent && output.null_count != expected {
        return plan_err!(
            "cross join column {idx} null_count {} does not equal scaled input null_count {}",
            output.null_count,
            expected
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_preserved_usize(
    idx: usize,
    stat_name: &str,
    input: Precision<usize>,
    output: Precision<usize>,
) -> Result<()> {
    if input != output {
        return plan_err!(
            "cross join column {idx} {stat_name} changed from {input} to {output}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_preserved_scalar(
    idx: usize,
    stat_name: &str,
    input: &Precision<ScalarValue>,
    output: &Precision<ScalarValue>,
) -> Result<()> {
    if input != output {
        return plan_err!(
            "cross join column {idx} {stat_name} changed from {input} to {output}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn comparable(left: &ScalarValue, right: &ScalarValue) -> bool {
    !left.is_null() && !right.is_null() && left.data_type() == right.data_type()
}

#[cfg(debug_assertions)]
fn row_product(left: &Statistics, right: &Statistics) -> Option<usize> {
    Some(
        left.num_rows
            .get_value()?
            .saturating_mul(*right.num_rows.get_value()?),
    )
}

#[cfg(debug_assertions)]
fn validate_at_least(
    context: &str,
    output_rows: usize,
    input: &Statistics,
) -> Result<()> {
    if let Some(&input_rows) = input.num_rows.get_value()
        && output_rows < input_rows
    {
        return plan_err!(
            "{context} row count {output_rows} is less than preserved input row count {input_rows}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_at_most(context: &str, output_rows: usize, input: &Statistics) -> Result<()> {
    if let Some(&input_rows) = input.num_rows.get_value()
        && output_rows > input_rows
    {
        return plan_err!(
            "{context} row count {output_rows} exceeds preserved input row count {input_rows}"
        );
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn validate_equal(context: &str, output_rows: usize, input: &Statistics) -> Result<()> {
    if let Some(&input_rows) = input.num_rows.get_value()
        && output_rows != input_rows
    {
        return plan_err!(
            "{context} row count {output_rows} does not equal input row count {input_rows}"
        );
    }
    Ok(())
}
