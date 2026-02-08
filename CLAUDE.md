# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Overview

Apache DataFusion is an extensible query engine written in Rust that uses Apache Arrow as its in-memory format. It is a Cargo workspace with 40+ crates organized under the `datafusion/` directory.

## Building and Testing

### Build Commands

```bash
# Build entire workspace
cargo build

# Build with release optimizations (slow compile, fast runtime)
cargo build --release

# Build with release-nonlto profile (faster compile than release, nearly same performance)
cargo build --profile release-nonlto

# Build with profiling profile (release + debug symbols for flamegraphs)
cargo build --profile profiling

# Build specific package
cargo build -p datafusion

# Check code compiles (faster than full build)
cargo check --workspace --all-targets
```

### Test Commands

```bash
# Before running the tests, git submodules must be fetched:
git submodule init
git submodule update --init --remote --recursive

# Run all tests in workspace
cargo test --workspace

# Run tests for a specific package
cargo test -p datafusion-expr

# Run a specific test by name
cargo test test_name

# Run tests in a specific file
cargo test --test integration_test
```

### SqlLogicTests (SLTs)

DataFusion uses sqllogictest files (`.slt`) for integration testing. Test files are located in `datafusion/sqllogictest/test_files/`.

```bash
# Run all tests
cargo test --test sqllogictests

# Run only the tests in `information_schema.slt`
cargo test --test sqllogictests -- information_schema
```

To add a new SLT test, first add the setup and queries you want to run to a .slt file (`my_awesome_test.slt` in this example) using the following format:

```
query
CREATE TABLE foo AS VALUES (1);

query
SELECT * from foo;
```

Running the following command will update `my_awesome_test.slt` with the expected output:

```bash
cargo test --test sqllogictests -- my_awesome_test --complete
```

### Linting and Formatting

```bash
# Format code
cargo fmt --all

# Run clippy (required before commit)
cargo clippy --workspace --all-targets --all-features

# Check typos
ci/scripts/typos_check.sh
```

## Architecture

DataFusion is organized as a modular query engine with the following key components:

### Core Crates

- **datafusion/core**: Main entry point, `SessionContext`, query execution
- **datafusion/sql**: SQL parser and query planner
- **datafusion/expr**: Logical expressions and logical plan nodes
- **datafusion/optimizer**: Logical plan optimization rules
- **datafusion/physical-plan**: Physical execution plan operators
- **datafusion/physical-expr**: Physical expression evaluation
- **datafusion/physical-optimizer**: Physical plan optimization
- **datafusion/execution**: Runtime execution environment and task scheduling

### Function Crates

- **datafusion/functions**: Scalar functions (string, math, etc.)
- **datafusion/functions-aggregate**: Aggregate functions (SUM, AVG, etc.)
- **datafusion/functions-window**: Window functions
- **datafusion/functions-nested**: Array and struct manipulation functions
- **datafusion/functions-table**: Table-valued functions

### Data Source Crates

- **datafusion/datasource**: Core TableProvider trait and file-based sources
- **datafusion/datasource-csv**: CSV file format support
- **datafusion/datasource-json**: JSON file format support
- **datafusion/datasource-parquet**: Parquet file format support
- **datafusion/datasource-avro**: Avro file format support (optional feature)
- **datafusion/datasource-arrow**: Arrow IPC support

### Other Important Crates

- **datafusion/common**: Shared utilities, error types, configuration
- **datafusion/catalog**: Catalog and schema management
- **datafusion/proto**: Serialization/deserialization of plans via Protocol Buffers
- **datafusion/substrait**: Substrait plan conversion
- **datafusion-cli**: Interactive SQL command-line interface

## Query Execution Flow

1. **SQL Parsing** (datafusion/sql): Parse SQL string into AST using sqlparser
2. **Logical Planning** (datafusion/sql): Convert AST to LogicalPlan
3. **Analysis** (datafusion/optimizer): Resolve types, validate schema
4. **Logical Optimization** (datafusion/optimizer): Apply optimization rules (predicate pushdown, constant folding, etc.)
5. **Physical Planning** (datafusion/core): Convert LogicalPlan to ExecutionPlan
6. **Physical Optimization** (datafusion/physical-optimizer): Optimize physical plan (join ordering, etc.)
7. **Execution** (datafusion/physical-plan): Execute plan as streaming batches of Arrow RecordBatches

## Development Practices

### Feature Flags

DataFusion has several optional features:
- `avro`: Apache Avro format support
- `backtrace`: Include backtraces in errors
- `parquet_encryption`: Parquet Modular Encryption
- `nested_expressions`: Array/struct functions (default)
- `compression`: File compression (xz2, bzip2, flate2, zstd) (default)

### Testing Conventions

- Unit tests live alongside code in each crate
- Integration tests use sqllogictest format in `datafusion/sqllogictest/test_files/`
- Examples serve as both documentation and integration tests
- All tests must pass before merging to main

### Performance Testing

Benchmarks are in the `benchmarks/` directory using TPC-H, TPC-DS, and other datasets:

```bash
cd benchmarks
./bench.sh data tpch    # Download TPC-H data
./bench.sh run tpch     # Run TPC-H benchmarks
```

### Micro-Benchmarks

Micro-benchmarks for individual functions and components often live in the `benches` directory of the component crate. For example, micro-benchmarks for built-in UDFs are in `datafusion/functions/benches/` (and `datafusion/functions-nested/benches/`).

```bash
# Run a specific benchmark
cargo bench --bench trim -p datafusion-functions

# Filter to specific benchmark cases
cargo bench --bench trim -p datafusion-functions -- "trim spaces"

# Run with baseline comparison (run before changes, then after)
cargo bench --bench trim -p datafusion-functions -- --save-baseline before
# ... make changes ...
cargo bench --bench trim -p datafusion-functions -- --baseline before
```

When adding or modifying a UDF optimization, ensure the benchmark exercises the actual code path being changed. A common pitfall is that benchmarks pass scalar arguments (which take an optimized dispatch path) while the change targets the array-argument path, or vice versa.

### Cargo.lock

This repository commits `Cargo.lock` and CI uses it. Dependencies are updated via Dependabot PRs.

## Common Workflows

### Adding a New Function

1. Add implementation in appropriate functions crate (datafusion/functions, datafusion/functions-aggregate, etc.)
2. Register function in the crate's registration module
3. Add tests in the same file
4. Add sqllogictest cases in `datafusion/sqllogictest/test_files/`
5. Documentation is auto-generated from code (`dev/update_function_docs.sh`)

### Debugging Test Failures

For sqllogictest failures:
- Test files contain expected output inline
- Compare actual vs expected output
- Use `RUST_LOG=debug` for detailed execution logs
- Check physical plan with EXPLAIN statements

### Fixing Clippy Issues

```bash
# Fix automatically where possible
cargo clippy --workspace --all-targets --fix --allow-dirty
```

### Command-Line Interface (CLI)

DataFusion provides a simple CLI that can be useful for testing new features:

```
cargo run --bin datafusion-cli -- -f ~/my_test.sql
```

### Working with Physical Plans

Physical plans implement the `ExecutionPlan` trait (datafusion/physical-plan). Key methods:
- `execute()`: Returns a stream of RecordBatches
- `schema()`: Returns output schema
- `children()`: Returns child plans
- `with_new_children()`: Creates plan with replaced children

### Working with Logical Plans

Logical plans use the `LogicalPlan` enum (datafusion/expr). Common variants:
- `Projection`, `Filter`, `Aggregate`, `Join`, `TableScan`
- Use `LogicalPlanBuilder` for constructing plans
- Optimizer rules traverse and transform LogicalPlans

### Working with Scalar Functions (UDFs)

Scalar UDFs implement `ScalarUDFImpl` (datafusion/expr/src/udf.rs). Key methods:
- `invoke_with_args()`: Main execution entry point, receives `ScalarFunctionArgs`
- `simplify()`: Optional planning-time rewrite (e.g., constant folding, function substitution)
- `signature()`: Declares accepted argument types
- `return_type()`: Declares output type

Arguments arrive as `ColumnarValue`, which is either `Scalar` (single value for all rows) or `Array`. Many UDFs use `make_scalar_function` (datafusion/functions/src/utils.rs) to handle the scalar-to-array expansion, with `Hint::Pad` (expand scalar to full-length array) and `Hint::AcceptsSingular` (keep scalar as 1-element array for efficient detection).

String functions support three array types: `Utf8`, `LargeUtf8`, and `Utf8View`. `Utf8View` enables zero-copy operations (e.g., trim can return views into original buffers without copying data).

### Optimizing UDF Performance

Common optimization patterns for built-in functions:

- **Scalar argument specialization**: When arguments like pattern/replacement are constant (`ColumnarValue::Scalar`), pre-compute work once rather than per-row (e.g., compile regex once, build lookup table once)
- **ASCII fast paths**: For byte-level operations on ASCII data, avoid UTF-8 char iteration; use `&[u8]` byte scanning instead of `str::chars()` or `str::trim_start_matches(&[char])`
- **Avoid per-row allocations**: Reuse `Vec` buffers across rows with `clear()` + `extend()` instead of allocating new collections per row
- **`simplify()` rewrites**: Rewrite function calls at planning time when possible (e.g., `regexp_like` → `~` operator, literal regex patterns → non-regex equivalents)
- **StringView zero-copy**: For operations producing substrings (trim, substr), the `Utf8View` path can construct new views pointing into original buffers without copying data

## Resources

- Website: https://datafusion.apache.org/
- API Docs: https://docs.rs/datafusion/latest/datafusion/
- Architecture: https://datafusion.apache.org/contributor-guide/architecture.html
- Contributor Guide: https://datafusion.apache.org/contributor-guide/
