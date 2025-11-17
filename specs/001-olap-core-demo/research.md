# Research & Technology Decisions

**Feature**: OLAP Core Capabilities Tech Demo
**Date**: 2025-11-17
**Purpose**: Document technology choices, alternatives considered, and decision rationales

## Executive Summary

Selected DuckDB + Python stack for lightweight, locally-runnable OLAP demonstration. DuckDB provides production-grade OLAP capabilities (columnar storage, vectorized execution, partition pruning) in embeddable form factor, enabling demo to run on laptops without infrastructure. Python ecosystem (PyArrow, Pandas, Faker, pytest) provides mature tooling for data generation, manipulation, and benchmarking.

## Technology Decisions

### 1. Query Engine: DuckDB

**Decision**: Use DuckDB 0.9+ as the OLAP query engine

**Rationale**:
- **Embeddable**: Runs in-process without server setup (perfect for demos)
- **Columnar Execution**: Native columnar storage and vectorized query engine
- **Parquet Support**: First-class Parquet integration with pushdown optimizations
- **SQL Compliance**: ANSI SQL 2011 support including window functions
- **Performance**: Achieves near-commercial-OLAP performance despite being embeddable
- **Observable**: EXPLAIN ANALYZE provides detailed execution plans and metrics
- **Partition Pruning**: Intelligent partition elimination based on query predicates
- **Free & Open Source**: MIT license, no licensing concerns

**Alternatives Considered**:

| Alternative | Why Rejected |
|-------------|--------------|
| Apache Drill | Requires distributed setup, overkill for single-node demo |
| ClickHouse | Server-based architecture, complex infrastructure for demo purposes |
| Presto/Trino | Distributed query engine, requires cluster setup |
| PostgreSQL with columnar extensions | Limited columnar support, primarily OLTP-optimized |
| SQLite | Row-oriented, poor analytical query performance |
| Pandas only | Lacks SQL interface, no query optimizer, slower for large datasets |

**Validation Criteria**:
- ✅ Supports 100M+ row datasets on laptop (16GB RAM)
- ✅ Provides query execution plans with metrics
- ✅ Native Parquet integration with partition pruning
- ✅ Window functions and complex aggregations
- ✅ Cross-platform (Linux, macOS, Windows)

### 2. Storage Format: Apache Parquet

**Decision**: Use Apache Parquet with Snappy compression for columnar storage

**Rationale**:
- **Columnar Layout**: Column-oriented storage enables efficient aggregations
- **Compression**: Snappy provides 5-10x compression with minimal CPU overhead
- **Schema Embedded**: Self-describing format, no separate schema management
- **Partition Support**: Hive-style partitioning for partition pruning
- **Ecosystem Support**: Wide adoption (Spark, Pandas, DuckDB, Arrow)
- **Selective Reads**: Read only required columns, dramatic I/O reduction
- **Metadata**: Rich column statistics for query optimization

**Alternatives Considered**:

| Alternative | Why Rejected |
|-------------|--------------|
| ORC | Less Python ecosystem support, primarily Hadoop-focused |
| Arrow IPC | Designed for in-memory streaming, not persistent storage |
| CSV | Row-oriented, no compression, no schema, slow parsing |
| JSON | Inefficient for analytical workloads, no columnar benefits |
| Avro | Row-oriented format, poor analytical performance |

**Compression Comparison**:

| Codec | Compression Ratio | Encode Speed | Decode Speed | Choice |
|-------|------------------|--------------|--------------|--------|
| Snappy | 5-7x | Fast | Fast | ✅ Primary |
| Zstd | 8-12x | Medium | Fast | Alternative |
| Gzip | 10-15x | Slow | Medium | Rejected (slow) |
| Uncompressed | 1x | N/A | Fastest | Comparison baseline |

**Decision**: Snappy for primary demo (fast compression), optional Zstd experiments

### 3. Programming Language: Python 3.11+

**Decision**: Python 3.11+ as primary implementation language

**Rationale**:
- **Data Ecosystem**: Rich libraries for data manipulation (Pandas, PyArrow, DuckDB bindings)
- **Synthetic Data**: Faker library for realistic data generation
- **Testing**: pytest mature ecosystem with pytest-benchmark for performance tests
- **Accessibility**: Widely known language, easy for evaluators to understand
- **Cross-Platform**: Runs on Linux, macOS, Windows without modification
- **Performance**: Python 3.11+ significant performance improvements (faster startup, better optimizations)
- **Type Hints**: Modern Python supports static typing for better code quality

**Alternatives Considered**:

| Alternative | Why Rejected |
|-------------|--------------|
| Java/Scala | Verbose for demo purposes, slower development iteration |
| Rust | Excellent performance but steeper learning curve, smaller data ecosystem |
| Go | Good performance but limited data science libraries |
| R | Strong for analytics but poor software engineering tooling |
| JavaScript/Node.js | Limited analytical database ecosystem |

**Python Version Justification**:
- Python 3.11: 10-60% faster than 3.10 (better perf for benchmarks)
- Pattern matching and improved error messages
- Async improvements (if concurrent queries needed)

### 4. Data Generation: Faker + Custom Schemas

**Decision**: Use Faker library with custom data generation logic

**Rationale**:
- **Realistic Data**: Faker generates realistic names, addresses, products, timestamps
- **Reproducible**: Fixed random seeds ensure deterministic generation
- **Flexible**: Custom providers for e-commerce domain (products, categories, transactions)
- **Fast**: Can generate >10M rows/minute on modern hardware
- **Localized**: Supports geographic diversity for geography dimensions

**Alternatives Considered**:

| Alternative | Why Rejected |
|-------------|--------------|
| Real datasets (TPC-H, TPC-DS) | Complex schemas, less relatable than e-commerce |
| Synthetic datasets (SSB) | Less flexible, fixed schemas |
| Real e-commerce data | Privacy concerns, availability issues, large downloads |
| Manual data generation | Time-consuming, not reproducible |
| DBGen tools | Focused on specific benchmarks, less educational |

**Generation Strategy**:
1. Generate dimension tables first (Time, Geography, Product, Customer)
2. Generate fact table with foreign keys to dimensions
3. Use realistic distributions (e.g., 80/20 for product popularity)
4. Fixed random seed (SEED=42) for reproducibility
5. Configurable row counts (10M, 50M, 100M, 200M)

### 5. Benchmarking: pytest-benchmark

**Decision**: Use pytest-benchmark for performance testing

**Rationale**:
- **Integration**: Seamless pytest integration (same framework for unit + benchmark tests)
- **Statistics**: Automatic percentile calculation (p50, p95, p99)
- **Comparison**: Built-in comparison against baseline results
- **Reporting**: JSON, CSV, HTML output formats
- **CI/CD Ready**: Designed for continuous integration pipelines
- **Warmup**: Automatic warmup rounds to stabilize measurements
- **Regression Detection**: Fail tests on performance regressions

**Alternatives Considered**:

| Alternative | Why Rejected |
|-------------|--------------|
| Manual timing (time.time()) | No statistical analysis, no percentiles |
| timeit module | Not integrated with test framework |
| Custom benchmark harness | Reinventing the wheel, maintenance burden |
| Apache JMeter | Web-focused, overkill for database benchmarks |
| sysbench | MySQL-focused, not flexible for custom workloads |

**Benchmark Configuration**:
```python
@pytest.mark.benchmark(
    group="aggregations",
    min_rounds=5,
    warmup=True,
    disable_gc=True  # Consistent measurements
)
def test_aggregation_query(benchmark):
    result = benchmark(execute_query, "SELECT SUM(revenue) FROM sales_fact")
    assert result is not None
```

### 6. Partitioning Strategy: Hive-Style by Time

**Decision**: Partition Parquet files by year/quarter using Hive-style partitioning

**Rationale**:
- **Time-Series Queries**: Most analytical queries filter by time period
- **Partition Pruning**: DuckDB automatically skips irrelevant partitions
- **Standard Format**: Hive-style (`year=2023/quarter=Q1/`) widely supported
- **Granularity**: Quarter-level balances partition count vs partition size
- **Demonstration**: Clearly shows partition pruning in EXPLAIN plans

**Partition Scheme**:
```
data/parquet/sales_fact/
├── year=2021/
│   ├── quarter=Q1/
│   │   └── data.parquet (100K-1M rows per file)
│   ├── quarter=Q2/
│   ├── quarter=Q3/
│   └── quarter=Q4/
├── year=2022/
│   └── [quarters...]
└── year=2023/
    └── [quarters...]
```

**Alternatives Considered**:

| Partition Scheme | Why Rejected |
|------------------|--------------|
| Daily partitions | Too many small files (365+ per year) |
| Monthly partitions | Considered but quarter provides good balance |
| Yearly only | Too coarse, limited pruning benefit |
| Product category | Less common filter, secondary partition candidate |
| Geography | Less common filter than time |
| No partitioning | Defeats purpose of demonstrating partition pruning |

### 7. Testing Strategy: Multi-Layered Approach

**Decision**: Three-layer test strategy (unit, integration, benchmarks)

**Test Layers**:

1. **Unit Tests** (tests/unit/):
   - Test individual components in isolation
   - Fast execution (<100ms per test)
   - Mocked dependencies
   - Focus: correctness of logic

2. **Integration Tests** (tests/integration/):
   - End-to-end data generation and query execution
   - Medium execution time (1-10s per test)
   - Real DuckDB, small datasets (1K-100K rows)
   - Focus: component interaction and correctness

3. **Benchmark Tests** (tests/benchmarks/):
   - Performance validation against SLAs
   - Slow execution (10s-60s per benchmark)
   - Full datasets (100M rows)
   - Focus: performance characteristics and regression detection

**Rationale**:
- **Fast Feedback**: Unit tests run in seconds (TDD workflow)
- **Correctness**: Integration tests validate end-to-end behavior
- **Performance**: Benchmarks enforce SLA compliance
- **Pyramid Structure**: Many unit tests, fewer integration tests, focused benchmarks

### 8. Local Testing Tools

**Decision**: Minimal tooling for easy local setup

**Required Tools**:
- **Python 3.11+**: Runtime environment
- **pip/uv**: Dependency management
- **pytest**: Test runner
- **DuckDB CLI** (optional): Interactive query exploration

**Setup Commands**:
```bash
# Install dependencies
pip install -e ".[dev]"

# Generate test data
python -m src.cli.generate --rows 10000000 --seed 42

# Run benchmarks
pytest tests/benchmarks/ --benchmark-only

# Interactive queries
duckdb data/duckdb/olap_demo.duckdb
```

**CI/CD Integration**:
- GitHub Actions for automated benchmarking
- Benchmark results committed to benchmark-results/
- Fail on >5% performance regression

## Architecture Decisions

### Star Schema Design

**Decision**: Single star schema with 1 fact table and 5 dimension tables

**Schema**:
```
Fact Table: sales_fact
  - Measures: revenue, quantity, cost, discount
  - Foreign Keys: time_key, geo_key, product_key, customer_key, payment_method_key

Dimensions:
  - dim_time: Date hierarchy (year, quarter, month, day, day_of_week)
  - dim_geography: Location hierarchy (region, country, state, city)
  - dim_product: Product catalog (category, subcategory, sku, brand) - SCD Type 2
  - dim_customer: Customer attributes (segment, channel, lifetime_value_tier)
  - dim_payment: Payment methods (type, provider)
```

**Rationale**:
- **Simplicity**: Single star schema easy to understand and query
- **Denormalized**: Dimensions are denormalized for query performance
- **SCD Type 2**: Product dimension tracks historical changes (demonstrates SCD concept)
- **Grain**: Clear fact grain (one row per line item per transaction)
- **Natural Dimensions**: Time and geography are standard analytical dimensions

### Query Patterns

**Decision**: 5 representative query patterns mapping to user stories

**Patterns**:

1. **Multi-Dimensional Aggregation** (P1):
   ```sql
   SELECT region, product_category, year, SUM(revenue) as total_revenue
   FROM sales_fact f
   JOIN dim_geography g ON f.geo_key = g.geo_key
   JOIN dim_product p ON f.product_key = p.product_key
   JOIN dim_time t ON f.time_key = t.time_key
   WHERE year = 2023
   GROUP BY region, product_category, year
   ```

2. **Drill-Down Analysis** (P1):
   - Yearly → Quarterly → Monthly aggregations
   - Test partition pruning effectiveness

3. **Window Functions** (P2):
   ```sql
   SELECT product_sku, month, revenue,
          AVG(revenue) OVER (PARTITION BY product_sku
                             ORDER BY month
                             ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg_3m
   FROM monthly_product_sales
   ```

4. **Storage Comparison** (P3):
   - Same query on Parquet vs CSV
   - Measure: execution time, bytes scanned, compression ratio

5. **Concurrent Queries** (P4):
   - 10 parallel analytical queries
   - Measure: throughput degradation vs single-query baseline

## Performance Validation Strategy

**Approach**: Benchmark-driven with explicit SLAs

**Benchmarks Map to User Stories**:
- P1 (Multi-Dimensional Analysis): tests/benchmarks/test_aggregations.py
- P2 (Time-Series Analysis): tests/benchmarks/test_window_functions.py
- P3 (Storage Efficiency): tests/benchmarks/test_storage.py
- P4 (Scalability): tests/benchmarks/test_scalability.py

**SLA Enforcement**:
```python
@pytest.mark.benchmark(
    group="aggregations",
    min_rounds=5
)
def test_p95_aggregation_under_5s(benchmark):
    result = benchmark(run_aggregation_query)
    # pytest-benchmark automatically computes p95
    # Fail if p95 > 5000ms
    assert benchmark.stats.stats.p95 < 5000.0, "SLA violation: p95 > 5s"
```

**Metrics Tracked**:
- Execution time (p50, p95, p99)
- Rows scanned
- Bytes processed
- Memory usage
- Compression ratios
- Partition pruning effectiveness

## Dependencies & Versions

**Core Dependencies**:
```toml
[project]
dependencies = [
    "duckdb>=0.9.0",
    "pyarrow>=14.0.0",
    "pandas>=2.1.0",
    "faker>=22.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-benchmark>=4.0.0",
    "pytest-xdist>=3.5.0",  # Parallel test execution
    "black>=23.0.0",  # Code formatting
    "ruff>=0.1.0",    # Linting
]
```

**Rationale for Version Pinning**:
- DuckDB 0.9+: Required for latest Parquet optimizations
- PyArrow 14+: Performance improvements in Parquet I/O
- Pandas 2.1+: Improved Arrow integration
- pytest-benchmark 4.0+: Percentile support

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Dataset too large for laptop memory | Demo fails to run locally | Configurable dataset sizes, default to 50M rows |
| DuckDB performance doesn't meet SLAs | Benchmarks fail | Fallback queries with smaller datasets, query tuning |
| Data generation too slow | Poor user experience | Parallel generation, smaller default datasets |
| Benchmark variance too high | Flaky tests | Multiple rounds, warmup, disable GC during benchmarks |
| Partition pruning not demonstrable | Can't prove optimization | EXPLAIN ANALYZE analysis, partition key in WHERE clause |

## Future Extensions (Out of Scope)

Explicitly not implementing in initial version:
- Distributed query execution (single-node only)
- Real-time ingestion (batch loading only)
- Query result caching (rely on DuckDB's internal cache)
- Materialized views (direct queries only)
- Advanced indexing (columnar format provides baseline optimization)
- Multiple star schemas (single schema only)

These would be Phase 2 enhancements after validating core demo.

## References

- DuckDB Documentation: https://duckdb.org/docs/
- Apache Parquet Format: https://parquet.apache.org/docs/
- pytest-benchmark: https://pytest-benchmark.readthedocs.io/
- Dimensional Modeling (Kimball): The Data Warehouse Toolkit
