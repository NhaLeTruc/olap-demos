# OLAP Core Capabilities Tech Demo

A lightweight, locally-runnable demonstration showcasing fundamental OLAP (Online Analytical Processing) capabilities: fast multi-dimensional aggregations, columnar storage efficiency, partition pruning, window functions, and scalability characteristics.

## Overview

This demo uses DuckDB (embeddable OLAP engine) with Parquet columnar storage to demonstrate 10-100x performance improvements over row-based storage on 100M+ row e-commerce sales datasets.

**Key Capabilities Demonstrated**:
- 🚀 **Fast Aggregations**: Multi-dimensional queries in <2s on 100M rows
- 📊 **Columnar Storage**: 5:1+ compression with Parquet vs CSV
- 🎯 **Partition Pruning**: 80%+ data skip with intelligent filtering
- 📈 **Window Functions**: Moving averages, YoY growth, rankings
- ⚡ **Scalability**: Sub-linear query time growth (2x data → <2.5x latency)

## Quick Start

**Prerequisites**: Python 3.11+, 16GB RAM, 15GB disk space

```bash
# 1. Install dependencies
pip install -e ".[dev]"

# 2. Generate sample data (10M rows, ~2 minutes)
python -m src.cli.generate --rows 10000000

# 3. Run sample query
duckdb data/duckdb/olap_demo.duckdb -c "SELECT region, SUM(revenue) as total FROM sales_fact f JOIN dim_geography g ON f.geo_key = g.geo_key GROUP BY region"

# 4. Run benchmarks
pytest tests/benchmarks/test_aggregations.py -v
```

For detailed setup instructions, see [specs/001-olap-core-demo/quickstart.md](specs/001-olap-core-demo/quickstart.md)

## Architecture

**Tech Stack**:
- **Query Engine**: DuckDB 0.9+ (columnar execution, vectorized processing)
- **Storage**: Parquet (Snappy compression) + CSV (comparison baseline)
- **Data Generation**: Faker (synthetic e-commerce data)
- **Benchmarking**: pytest-benchmark (performance regression testing)

**Project Structure**:
```
src/
├── datagen/     # Synthetic data generation
├── models/      # Star schema (fact + 5 dimensions)
├── query/       # DuckDB wrapper + profiling
├── storage/     # Parquet/CSV handlers + partitioning
└── cli/         # CLI tools (generate, benchmark, analyze)

tests/
├── benchmarks/  # Performance tests (P1-P4 user stories)
├── integration/ # End-to-end tests
└── unit/        # Component tests
```

## CLI Commands

### Generate Data

```bash
# Generate 100M row dataset (default: partitioned by year/quarter)
python -m src.cli.generate --rows 100000000 --seed 42

# Generate both Parquet and CSV for comparison
python -m src.cli.generate --rows 50000000 --format both
```

### Run Benchmarks

```bash
# Run all benchmarks
python -m src.cli.benchmark all

# Run specific user story benchmarks
python -m src.cli.benchmark p1  # Multi-dimensional aggregations

# Compare against baseline
python -m src.cli.benchmark all --baseline benchmark-results/baseline/results.json
```

### Analyze Queries

```bash
# Analyze query execution plan
python -m src.cli.analyze "SELECT year, quarter, SUM(revenue) FROM sales_fact f JOIN dim_time t ON f.time_key = t.time_key WHERE year = 2023 GROUP BY year, quarter"

# Execute and profile
python -m src.cli.analyze --execute --profile queries/my_query.sql
```

## Performance Targets

| Metric | Target | Dataset |
|--------|--------|---------|
| Aggregation queries | p95 <5s | 100M rows |
| Drill-down queries | p95 <1s | Filtered |
| Window functions | p95 <3s | 100M rows |
| Storage compression | 5:1+ ratio | Parquet vs CSV |
| I/O efficiency | <20% scanned | Selective columns |
| Scalability | <2.5x growth | 2x data increase |
| Concurrency | <2x latency | 10 parallel queries |

## Data Model

**Star Schema** (dimensional modeling):
- **Fact Table**: `sales_fact` (100M-200M rows, partitioned by year/quarter)
  - Measures: revenue, quantity, cost, profit
  - Grain: one row per product sold per transaction
- **Dimensions**: time, geography, product (SCD Type 2), customer, payment

## Development

### Setup Development Environment

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run linting
ruff check .

# Run formatting
black .

# Run type checking
mypy src/
```

### Running Tests

```bash
# Unit tests (fast)
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# Benchmarks (slow, requires data)
pytest tests/benchmarks/ --benchmark-only

# All tests
pytest
```

## Documentation

- **Technical Plan**: [specs/001-olap-core-demo/plan.md](specs/001-olap-core-demo/plan.md)
- **Feature Spec**: [specs/001-olap-core-demo/spec.md](specs/001-olap-core-demo/spec.md)
- **Data Model**: [specs/001-olap-core-demo/data-model.md](specs/001-olap-core-demo/data-model.md)
- **Query Patterns**: [specs/001-olap-core-demo/contracts/query-patterns.md](specs/001-olap-core-demo/contracts/query-patterns.md)
- **Quickstart Guide**: [specs/001-olap-core-demo/quickstart.md](specs/001-olap-core-demo/quickstart.md)

## Constitution Compliance

This implementation adheres to the [OLAP Tech Demo Constitution](./speckit.memory/constitution.md):

✅ **I. Columnar-First Architecture** - Parquet with Snappy compression
✅ **II. Query Performance Excellence** - SLAs enforced via benchmarks
✅ **III. Benchmark-Driven Development** - TDD with performance tests
✅ **IV. Data Integrity & Consistency** - Deterministic, reproducible results
✅ **V. Scalability & Partitioning** - Hive-style partitioning with pruning
✅ **VI. Observability** - EXPLAIN ANALYZE + custom profiling
✅ **VII. Simplicity & Focus** - Single star schema, 5 query patterns

## License

MIT

## Contributing

This is a technical demonstration project. For questions or suggestions, please open an issue.
