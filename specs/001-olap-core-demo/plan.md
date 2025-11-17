# Implementation Plan: OLAP Core Capabilities Tech Demo

**Branch**: `001-olap-core-demo` | **Date**: 2025-11-17 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-olap-core-demo/spec.md`

**Note**: This plan defines the technical architecture and technology stack for demonstrating OLAP core capabilities.

## Summary

Build a lightweight, locally-runnable OLAP demonstration that showcases fundamental analytical processing capabilities: fast multi-dimensional aggregations, columnar storage efficiency, partition pruning, window functions, and scalability characteristics. Use DuckDB (embeddable OLAP engine) with Parquet columnar storage to demonstrate 10-100x performance improvements over row-based storage on a 100M+ row e-commerce sales dataset. Implement benchmark suite with pytest to validate performance SLAs (p95 <5s for aggregations, 5:1+ compression ratios, sub-linear scaling).

## Technical Context

**Language/Version**: Python 3.11+
**Primary Dependencies**:
- DuckDB 0.9+ (embeddable OLAP database with columnar execution)
- PyArrow 14+ (Parquet file I/O and columnar data structures)
- Pandas 2.1+ (data manipulation and analysis)
- Faker 22+ (synthetic data generation)
- pytest 7.4+ with pytest-benchmark (testing and performance benchmarking)

**Storage**:
- Columnar: Parquet files with Snappy compression (primary analytical storage)
- Row-based: CSV files (comparison baseline)
- DuckDB database files (.duckdb) for persistent query engine state

**Testing**:
- pytest for unit and integration tests
- pytest-benchmark for performance regression testing
- Custom benchmark harness for query execution metrics

**Target Platform**: Linux/macOS/Windows (cross-platform Python, no infrastructure required)

**Project Type**: Single project (command-line tools + library)

**Performance Goals**:
- Aggregation queries: p50 <2s, p95 <5s on 100M rows
- Drill-down queries: p95 <1s
- Window functions: p95 <3s
- Data generation: >10M rows/minute
- Data loading: >100 MB/s throughput

**Constraints**:
- Must run locally without distributed infrastructure
- Dataset size: 100M-200M rows (manageable on laptop with 16GB RAM)
- Query memory footprint: <4GB per query
- Total storage: <10GB for all datasets (compressed)
- Benchmark reproducibility: <10% variance across runs

**Scale/Scope**:
- 5 core query patterns (aggregations, drill-downs, window functions, partition pruning, concurrent queries)
- 6 dimensional entities (1 fact table + 5 dimension tables)
- 100M-200M fact table rows
- ~20 columns per fact table
- 4 scalability test datasets (10M, 50M, 100M, 200M rows)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Columnar-First Architecture ✅ PASS

- **Requirement**: All analytical data storage MUST use columnar formats
- **Implementation**: Parquet with Snappy compression for all analytical tables
- **Validation**: DuckDB natively supports columnar execution on Parquet files
- **Comparison**: CSV files provided only for performance comparison benchmarks

### II. Query Performance Excellence ✅ PASS

- **Requirement**: Queries MUST complete within defined performance SLAs
- **Implementation**:
  - Aggregations: p95 <5s target on 100M rows
  - Drill-downs: p95 <1s target
  - Window functions: p95 <3s target
- **Validation**: pytest-benchmark suite enforces SLA compliance
- **Monitoring**: Query execution plans exposed via DuckDB EXPLAIN ANALYZE

### III. Benchmark-Driven Development (NON-NEGOTIABLE) ✅ PASS

- **Requirement**: TDD with performance benchmarks as first-class tests
- **Implementation**:
  - pytest-benchmark integrated for all query patterns
  - Benchmarks written BEFORE query implementation
  - CI/CD runs benchmarks and rejects >5% regressions
- **Artifacts**: benchmark-results/ directory tracks historical performance
- **Workflow**: Write benchmark → Define SLA → Fail → Implement → Pass

### IV. Data Integrity & Consistency ✅ PASS

- **Requirement**: Analytical results MUST be correct, consistent, reproducible
- **Implementation**:
  - Schema validation during data generation (Faker with constraints)
  - Deterministic data generation (fixed random seeds)
  - Query result checksums for reproducibility testing
  - Parquet file checksums for data validation
- **Validation**: Integration tests verify identical results across multiple executions

### V. Scalability & Partitioning ✅ PASS

- **Requirement**: Horizontal scaling, explicit partitioning strategies
- **Implementation**:
  - Parquet files partitioned by year/quarter (Hive-style partitioning)
  - DuckDB partition pruning demonstrated in EXPLAIN plans
  - Scalability tests on 10M, 50M, 100M, 200M row datasets
- **Validation**: Benchmarks prove sub-linear scaling (2x data → <2.5x latency)

### VI. Observability & Query Monitoring ✅ PASS

- **Requirement**: All queries observable with detailed metrics
- **Implementation**:
  - DuckDB EXPLAIN ANALYZE for execution plans
  - Custom query profiler captures: execution_time, rows_scanned, bytes_processed
  - Structured logging with query_id, timestamp, duration
  - Benchmark results dashboard (markdown reports)
- **Validation**: All query patterns include execution plan analysis

### VII. Simplicity & Focus ✅ PASS

- **Requirement**: Minimum viable demo, YAGNI principles
- **Implementation**:
  - Single star schema (sales fact + 5 dimensions)
  - 5 query patterns (no gold-plating)
  - Embeddable DuckDB (no distributed infrastructure)
  - Standard tools (Python, pytest, DuckDB)
- **Justification**: Each component demonstrates distinct OLAP capability
- **Rejected**: Advanced features (materialized views, query caching, multi-node distribution)

### Technical Constraints Compliance

**Storage & Formats**: ✅ PASS
- Parquet with Snappy compression
- Target file size: 128MB-1GB per partition
- Self-describing schemas (embedded in Parquet metadata)

**Query Engine**: ✅ PASS
- DuckDB supports ANSI SQL 2011 (including window functions)
- Native filter and projection pushdown to Parquet
- Vectorized execution engine
- Result caching handled by DuckDB query cache

**Data Model**: ✅ PASS
- Star schema: sales_fact + dim_time + dim_geography + dim_product + dim_customer
- SCD Type 2 support for dim_product (track product changes over time)
- Fact table grain documented: one row per product sold per transaction

**Performance Targets**: ✅ PASS
- Interactive queries: <500ms p95 for <10M rows ✅
- Analytical queries: <5s p95 for 100M rows ✅
- Data loading: >100MB/s (DuckDB Parquet loading) ✅
- Concurrency: 10 concurrent queries (pytest-xdist parallel execution) ✅

### Gate Decision: ✅ ALL GATES PASSED

**Proceed to Phase 0 (Research)**

No constitution violations. All core principles satisfied. Technical constraints met. Ready for detailed design.

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
src/
├── datagen/
│   ├── __init__.py
│   ├── generator.py          # Synthetic data generation (Faker-based)
│   ├── schemas.py             # Data schemas and constraints
│   └── loaders.py             # Data loading to Parquet/CSV/DuckDB
├── models/
│   ├── __init__.py
│   ├── dimensions.py          # Dimension table models (Time, Geography, Product, Customer)
│   └── facts.py               # Fact table model (Sales transactions)
├── query/
│   ├── __init__.py
│   ├── engine.py              # DuckDB query execution wrapper
│   ├── profiler.py            # Query profiling and metrics collection
│   └── patterns.py            # Predefined query patterns (aggregations, window functions)
├── storage/
│   ├── __init__.py
│   ├── parquet_handler.py     # Parquet I/O operations
│   ├── csv_handler.py         # CSV I/O operations (comparison baseline)
│   └── partitioning.py        # Partition management (Hive-style)
└── cli/
    ├── __init__.py
    ├── generate.py            # CLI: Generate datasets
    ├── benchmark.py           # CLI: Run benchmarks
    └── analyze.py             # CLI: Analyze query plans

tests/
├── benchmarks/
│   ├── __init__.py
│   ├── test_aggregations.py   # P1: Multi-dimensional aggregation benchmarks
│   ├── test_window_functions.py  # P2: Time-series analysis benchmarks
│   ├── test_storage.py        # P3: Columnar vs row storage benchmarks
│   └── test_scalability.py    # P4: Scalability validation benchmarks
├── integration/
│   ├── __init__.py
│   ├── test_data_generation.py  # End-to-end data generation tests
│   ├── test_query_execution.py  # Query correctness tests
│   └── test_partitioning.py     # Partition pruning integration tests
└── unit/
    ├── __init__.py
    ├── test_models.py         # Unit tests for data models
    ├── test_profiler.py       # Unit tests for query profiler
    └── test_storage.py        # Unit tests for storage handlers

data/                          # Generated datasets (gitignored)
├── parquet/
│   └── sales_fact/
│       ├── year=2021/
│       │   ├── quarter=Q1/
│       │   └── quarter=Q2/
│       ├── year=2022/
│       └── year=2023/
├── csv/
│   └── sales_fact.csv         # Comparison baseline
└── duckdb/
    └── olap_demo.duckdb       # Persistent DuckDB database

benchmark-results/             # Benchmark outputs (tracked in git)
├── README.md
├── baseline/
│   └── results-<date>.json
└── current/
    └── results-<date>.json

docs/
├── architecture.md            # Architecture decisions and rationale
└── query-catalog.md           # Documented query patterns with examples

.python-version                # Python version (3.11+)
pyproject.toml                 # Python dependencies and project config
pytest.ini                     # Pytest configuration
.gitignore                     # Ignore data/ directory, __pycache__, etc.
README.md                      # Project overview and quickstart
```

**Structure Decision**: Single project structure selected. This is a command-line demo with library components (src/) and comprehensive test suite (tests/). No web frontend or mobile app needed - all interaction via CLI tools and direct query execution. The structure supports:

- **Data Generation**: src/datagen/ creates synthetic e-commerce data
- **OLAP Models**: src/models/ defines star schema (fact + dimensions)
- **Query Engine**: src/query/ wraps DuckDB with profiling and metrics
- **Storage Layer**: src/storage/ handles Parquet/CSV I/O and partitioning
- **CLI Tools**: src/cli/ provides user-facing commands for generation, benchmarking, analysis
- **Benchmarks**: tests/benchmarks/ maps to 4 user stories (P1-P4)
- **Data Directory**: Generated datasets in data/ (gitignored, reproducible via seeds)
- **Results Tracking**: benchmark-results/ versioned to track performance over time

## Complexity Tracking

**No Constitution Violations**: All gates passed. No complexity justifications required.

This implementation adheres to the Simplicity & Focus principle by:
- Using single project structure (no microservices)
- Leveraging embeddable DuckDB (no distributed infrastructure)
- Standard Python toolchain (no custom frameworks)
- 5 focused query patterns (no feature creep)
- Single star schema (no unnecessary data models)
