# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
# Run all tests in workspace
cargo test --workspace

# Run tests for a specific package
cargo test -p datafusion-expr

# Run a specific test by name
cargo test test_name

# Run tests in a specific file
cargo test --test integration_test

# Run sqllogictest suite
cargo test -p datafusion-sqllogictest
```

### SqlLogicTests (SLTs)

DataFusion uses sqllogictest files (`.slt`) extensively for integration testing. Test files are located in `datafusion/sqllogictest/test_files/`.

```bash
# Run all tests
cargo test --test sqllogictests

# Run all tests, with debug logging enabled
RUST_LOG=debug cargo test --test sqllogictests

# Run only the tests in `information_schema.slt`
cargo test --test sqllogictests -- information_schema
```

To add a new SLT test, first add add the setup and queries you want to run to a .slt file (`my_awesome_test.slt` in this example) using the following format:

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

# Check license headers
ci/scripts/license_header.sh

# Check typos
ci/scripts/typos_check.sh
```

### Examples

The `datafusion-examples` crate contains extensive examples organized by category. Examples are located in `datafusion-examples/examples/`.

```bash
# Run all examples in a group
cargo run --example dataframe -- all

# Run specific example
cargo run --example dataframe -- dataframe
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

### Rust Version

- MSRV: 1.88.0 (defined in `rust-version` in Cargo.toml)
- CI uses: 1.92.0 (defined in rust-toolchain.toml)

### Clippy Rules

- `tokio::task::spawn` and `spawn_blocking` are disallowed - use `SpawnedTask::spawn` instead for cancel-safety
- `std::time::Instant` is disallowed - use `datafusion_common::instant::Instant` for WASM compatibility
- Large futures threshold: 10000 bytes (default 16384)
- Large error threshold: 70 bytes (default 128)

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

### Cargo.lock

This repository commits `Cargo.lock` and CI uses it. Dependencies are updated via Dependabot PRs.

## Common Workflows

### Adding a New Function

1. Add implementation in appropriate functions crate (datafusion/functions, datafusion/functions-aggregate, etc.)
2. Register function in the crate's registration module
3. Add tests in the same file
4. Add sqllogictest cases in `datafusion/sqllogictest/test_files/`
5. Documentation is auto-generated from code

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

## Resources

- Website: https://datafusion.apache.org/
- API Docs: https://docs.rs/datafusion/latest/datafusion/
- Architecture: https://datafusion.apache.org/contributor-guide/architecture.html
- Contributor Guide: https://datafusion.apache.org/contributor-guide/
