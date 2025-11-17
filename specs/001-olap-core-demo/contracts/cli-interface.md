# CLI Interface Contract

**Feature**: OLAP Core Capabilities Tech Demo
**Date**: 2025-11-17
**Purpose**: Define command-line interface for data generation, benchmarking, and query analysis

## Overview

The OLAP demo provides three CLI commands for interacting with the system:
1. `generate` - Generate synthetic datasets
2. `benchmark` - Run performance benchmarks
3. `analyze` - Analyze query execution plans

All commands are implemented as Python modules in `src/cli/` and accessible via `python -m src.cli.<command>`.

## Command 1: generate

**Purpose**: Generate synthetic e-commerce sales data with configurable size and partitioning

**Module**: `src.cli.generate`

**Usage**:
```bash
python -m src.cli.generate [OPTIONS]
```

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--rows` | INTEGER | 10000000 | Number of fact table rows to generate |
| `--start-date` | DATE | 2021-01-01 | Start date for transaction data |
| `--end-date` | DATE | 2023-12-31 | End date for transaction data |
| `--seed` | INTEGER | 42 | Random seed for reproducibility |
| `--format` | CHOICE | parquet | Output format: parquet, csv, or both |
| `--partition-by` | CHOICE | year-quarter | Partitioning scheme: year-quarter, year-month, or none |
| `--output-dir` | PATH | data/ | Base output directory |
| `--num-products` | INTEGER | 10000 | Number of products in catalog |
| `--num-customers` | INTEGER | 1000000 | Number of customers |
| `--parallel` | INTEGER | 4 | Number of parallel workers for generation |
| `--overwrite` | FLAG | False | Overwrite existing data |
| `--verbose` | FLAG | False | Enable verbose logging |

**Examples**:

### Example 1: Generate default 10M row dataset
```bash
python -m src.cli.generate
```

**Expected Output**:
```
[INFO] Starting data generation...
[INFO] Configuration:
  - Rows: 10,000,000
  - Date Range: 2021-01-01 to 2023-12-31
  - Seed: 42
  - Format: parquet
  - Partitioning: year-quarter
  - Workers: 4

[INFO] Phase 1/3: Generating dimension tables
  ✓ dim_time (1,095 rows) - 0.1s
  ✓ dim_geography (5,000 rows) - 0.3s
  ✓ dim_product (10,000 rows) - 0.5s
  ✓ dim_customer (1,000,000 rows) - 12.4s
  ✓ dim_payment (20 rows) - 0.1s

[INFO] Phase 2/3: Generating fact table
  ✓ Generated 10,000,000 rows - 142.3s (70,300 rows/sec)
  ✓ Partitioned into 12 partitions (year-quarter)

[INFO] Phase 3/3: Writing to Parquet
  ✓ data/parquet/sales_fact/year=2021/quarter=Q1/ - 95.2 MB
  ✓ data/parquet/sales_fact/year=2021/quarter=Q2/ - 97.8 MB
  ...
  ✓ Total written: 1.2 GB compressed (6.2x compression)

[SUCCESS] Data generation complete in 158.7s
[INFO] Output location: data/parquet/
```

### Example 2: Generate 100M row dataset for benchmarking
```bash
python -m src.cli.generate --rows 100000000 --parallel 8 --verbose
```

### Example 3: Generate both Parquet and CSV for comparison
```bash
python -m src.cli.generate --rows 50000000 --format both
```

**Expected Output** (additional CSV generation):
```
...
[INFO] Phase 4/4: Writing to CSV (comparison baseline)
  ✓ data/csv/sales_fact.csv - 7.8 GB uncompressed
[INFO] Storage comparison:
  - Parquet: 1.2 GB (6.5x compression vs CSV)
  - CSV: 7.8 GB (baseline)
```

### Example 4: Custom date range and seed
```bash
python -m src.cli.generate \
  --rows 20000000 \
  --start-date 2022-01-01 \
  --end-date 2024-12-31 \
  --seed 12345
```

**Exit Codes**:
- `0`: Success
- `1`: General error (invalid arguments, file I/O error)
- `2`: Disk space insufficient
- `3`: Output directory not writable

**Output Files**:
```
data/
├── parquet/
│   ├── dim_time.parquet
│   ├── dim_geography.parquet
│   ├── dim_product.parquet
│   ├── dim_customer.parquet
│   ├── dim_payment.parquet
│   └── sales_fact/
│       ├── year=2021/
│       │   ├── quarter=Q1/
│       │   │   └── data.parquet
│       │   ├── quarter=Q2/
│       │   └── ...
│       └── ...
├── csv/
│   ├── dim_time.csv
│   ├── dim_geography.csv
│   ├── dim_product.csv
│   ├── dim_customer.csv
│   ├── dim_payment.csv
│   └── sales_fact.csv
└── metadata.json  # Generation metadata
```

**Metadata File** (`data/metadata.json`):
```json
{
  "generated_at": "2025-11-17T10:30:45Z",
  "rows": 10000000,
  "start_date": "2021-01-01",
  "end_date": "2023-12-31",
  "seed": 42,
  "format": "parquet",
  "partitioning": "year-quarter",
  "partitions": 12,
  "compression_ratio": 6.5,
  "generation_time_seconds": 158.7,
  "schema_version": "1.0"
}
```

## Command 2: benchmark

**Purpose**: Run performance benchmarks and validate SLAs

**Module**: `src.cli.benchmark`

**Usage**:
```bash
python -m src.cli.benchmark [OPTIONS] [BENCHMARK_SUITE]
```

**Arguments**:

| Argument | Type | Description |
|----------|------|-------------|
| `BENCHMARK_SUITE` | CHOICE | Benchmark suite to run: all, p1, p2, p3, p4, or specific test name |

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--data-dir` | PATH | data/ | Data directory containing datasets |
| `--rounds` | INTEGER | 5 | Number of benchmark rounds |
| `--warmup` | INTEGER | 2 | Number of warmup rounds |
| `--output` | PATH | benchmark-results/current/ | Output directory for results |
| `--format` | CHOICE | json | Output format: json, csv, html, markdown |
| `--baseline` | PATH | None | Baseline results file for comparison |
| `--fail-on-regression` | FLOAT | 0.05 | Fail if performance regresses by > X (e.g., 0.05 = 5%) |
| `--parallel` | FLAG | False | Run benchmarks in parallel (for concurrency tests) |
| `--verbose` | FLAG | False | Enable verbose output |

**Examples**:

### Example 1: Run all benchmarks
```bash
python -m src.cli.benchmark all
```

**Expected Output**:
```
[INFO] OLAP Benchmark Suite
[INFO] Data: 100M rows in data/parquet/
[INFO] Rounds: 5 (+ 2 warmup)

Running Benchmark Suite: ALL
════════════════════════════════════════════════════════════

[P1] Multi-Dimensional Aggregations
────────────────────────────────────────────────────────────
  ✓ test_revenue_by_region_and_year
      p50: 1.23s | p95: 1.89s | p99: 2.12s
      Status: PASS (p95 < 2.00s SLA)

  ✓ test_category_performance_by_quarter
      p50: 0.67s | p95: 0.94s | p99: 1.03s
      Status: PASS (p95 < 1.00s SLA)

  ✓ test_drill_down_year_to_quarter
      p50: 0.42s | p95: 0.61s | p99: 0.72s
      Status: PASS (p95 < 1.00s SLA)

[P2] Time-Series Window Functions
────────────────────────────────────────────────────────────
  ✓ test_moving_average_3month
      p50: 2.14s | p95: 2.87s | p99: 3.01s
      Status: PASS (p95 < 3.00s SLA)

  ✓ test_product_rankings_quarterly
      p50: 1.56s | p95: 1.92s | p99: 2.05s
      Status: PASS (p95 < 2.00s SLA)

[P3] Storage Efficiency
────────────────────────────────────────────────────────────
  ✓ test_parquet_vs_csv_performance
      Parquet: p95: 1.42s | 3.2 GB scanned
      CSV:     p95: 43.5s | 18.7 GB scanned
      Speedup: 30.6x
      Status: PASS (Parquet 10-50x faster)

  ✓ test_columnar_io_efficiency
      Selective (3/20 columns):
        Parquet: 287 MB scanned
        CSV: 18.7 GB scanned
      I/O Reduction: 98.5%
      Status: PASS (>80% I/O reduction)

[P4] Scalability
────────────────────────────────────────────────────────────
  ✓ test_sublinear_scaling
      50M rows:  p95: 0.98s
      100M rows: p95: 1.89s (1.93x slower)
      Status: PASS (<2.5x threshold)

  ✓ test_concurrent_queries_10x
      1 concurrent:  avg: 1.45s
      10 concurrent: avg: 2.67s (1.84x slower)
      Status: PASS (<2.0x threshold)

════════════════════════════════════════════════════════════
Summary: 10/10 tests passed (100%)
Total Time: 324.5s
Results saved to: benchmark-results/current/results-2025-11-17.json
```

### Example 2: Run only P1 (multi-dimensional aggregation) benchmarks
```bash
python -m src.cli.benchmark p1
```

### Example 3: Compare against baseline
```bash
python -m src.cli.benchmark all \
  --baseline benchmark-results/baseline/results-2025-11-01.json \
  --fail-on-regression 0.05
```

**Expected Output** (with regression detection):
```
...
[P1] Multi-Dimensional Aggregations
────────────────────────────────────────────────────────────
  ✓ test_revenue_by_region_and_year
      Current p95: 1.89s | Baseline p95: 1.75s
      Regression: +8.0% ⚠ FAIL (exceeds 5% threshold)
...

════════════════════════════════════════════════════════════
Summary: 9/10 tests passed (90%)
1 test exceeded regression threshold
Exit code: 1 (regression detected)
```

### Example 4: Generate HTML report
```bash
python -m src.cli.benchmark all --format html --output benchmark-results/reports/
```

**Output**: `benchmark-results/reports/report-2025-11-17.html`

**Exit Codes**:
- `0`: All benchmarks passed
- `1`: Benchmark failures or regressions detected
- `2`: Data not found (run `generate` first)

## Command 3: analyze

**Purpose**: Analyze query execution plans and resource usage

**Module**: `src.cli.analyze`

**Usage**:
```bash
python -m src.cli.analyze [OPTIONS] QUERY_FILE_OR_STRING
```

**Arguments**:

| Argument | Type | Description |
|----------|------|-------------|
| `QUERY_FILE_OR_STRING` | PATH or STRING | SQL query file (.sql) or SQL string to analyze |

**Options**:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--data-dir` | PATH | data/ | Data directory containing datasets |
| `--format` | CHOICE | text | Output format: text, json, graphviz |
| `--execute` | FLAG | False | Actually execute query (default: only explain) |
| `--profile` | FLAG | False | Enable detailed profiling (memory, I/O) |
| `--output` | PATH | stdout | Output file for analysis results |

**Examples**:

### Example 1: Analyze query execution plan
```bash
python -m src.cli.analyze "SELECT region, SUM(revenue) FROM sales_fact f JOIN dim_geography g ON f.geo_key = g.geo_key GROUP BY region"
```

**Expected Output**:
```
Query Execution Plan Analysis
════════════════════════════════════════════════════════════

SQL Query:
──────────────────────────────────────────────────────────
SELECT region, SUM(revenue)
FROM sales_fact f
JOIN dim_geography g ON f.geo_key = g.geo_key
GROUP BY region

Execution Plan (EXPLAIN ANALYZE):
──────────────────────────────────────────────────────────
┌─────────────────────────────────────────────┐
│ HASH_GROUP_BY                               │
│ ├── Groups: region                          │
│ ├── Aggregates: SUM(revenue)                │
│ ├── Estimated Rows: 6                       │
│ └── Execution Time: 0.12s                   │
├─────────────────────────────────────────────┤
│ HASH_JOIN                                   │
│ ├── Join Type: INNER                        │
│ ├── Join Condition: f.geo_key = g.geo_key  │
│ ├── Estimated Rows: 100,000,000             │
│ └── Execution Time: 1.34s                   │
├─────────────────────────────────────────────┤
│ PARQUET_SCAN (sales_fact)                   │
│ ├── Partitions: 12/12 scanned               │
│ ├── Rows Scanned: 100,000,000               │
│ ├── Bytes Scanned: 3.2 GB                   │
│ ├── Columns: geo_key, revenue (2/20)        │
│ ├── Column Pruning: 90% reduction           │
│ └── Execution Time: 0.89s                   │
└─────────────────────────────────────────────┘

Performance Metrics:
──────────────────────────────────────────────────────────
Total Execution Time: 1.56s
Rows Scanned: 100,000,000
Bytes Scanned: 3.2 GB (I/O: 3.2 GB read, 0 GB written)
Partitions Scanned: 12/12 (0% pruned)
Column Selectivity: 2/20 columns (90% pruning)
Memory Usage: Peak 1.2 GB

Optimization Recommendations:
──────────────────────────────────────────────────────────
  ⚠ No partition pruning detected
    → Add WHERE clause on transaction_date to leverage partitions
    → Example: WHERE transaction_date >= '2023-01-01'

  ✓ Column pruning effective (90% reduction)
  ✓ Hash join strategy appropriate for this query
```

### Example 2: Analyze query from file
```bash
python -m src.cli.analyze queries/revenue_by_region.sql
```

### Example 3: Analyze and execute query with profiling
```bash
python -m src.cli.analyze \
  --execute \
  --profile \
  "SELECT year, quarter, SUM(revenue) FROM sales_fact f JOIN dim_time t ON f.time_key = t.time_key WHERE year = 2023 GROUP BY year, quarter"
```

**Expected Output** (with execution and profiling):
```
...
Performance Metrics:
──────────────────────────────────────────────────────────
Total Execution Time: 0.67s
Rows Scanned: 25,000,000
Bytes Scanned: 812 MB
Partitions Scanned: 4/12 (67% pruned) ✓
Column Selectivity: 3/20 columns (85% pruning)
Memory Usage: Peak 456 MB

Detailed Profiling:
──────────────────────────────────────────────────────────
CPU Time: 2.4s (3.6x parallelism on 4 cores)
I/O Wait: 0.23s
Network: 0s (local files)
Cache Hits: 23% (from DuckDB internal cache)

Query Result:
──────────────────────────────────────────────────────────
| year | quarter | revenue      |
|------|---------|--------------|
| 2023 | Q1      | $12,345,678  |
| 2023 | Q2      | $13,456,789  |
| 2023 | Q3      | $14,567,890  |
| 2023 | Q4      | $15,678,901  |
(4 rows returned)

Optimization Recommendations:
──────────────────────────────────────────────────────────
  ✓ Excellent partition pruning (67% reduction)
  ✓ Column pruning effective (85% reduction)
  ✓ Query performance within SLA (<1s target)
  ℹ Consider caching result for repeated queries
```

### Example 4: Generate Graphviz visualization
```bash
python -m src.cli.analyze \
  --format graphviz \
  --output query_plan.dot \
  queries/complex_analysis.sql
```

**Output**: `query_plan.dot` file (render with `dot -Tpng query_plan.dot -o query_plan.png`)

## Global Options

All commands support these global options:

| Option | Description |
|--------|-------------|
| `--help` | Show help message |
| `--version` | Show version information |
| `--config` | Path to configuration file (YAML) |
| `--log-level` | Logging level: DEBUG, INFO, WARNING, ERROR |
| `--log-file` | Log output file (default: stderr) |

**Example**:
```bash
python -m src.cli.generate --help
python -m src.cli.benchmark --version
python -m src.cli.analyze --log-level DEBUG query.sql
```

## Configuration File

**File**: `config.yaml` (optional, overrides defaults)

**Example**:
```yaml
# OLAP Demo Configuration
data:
  default_rows: 10000000
  default_seed: 42
  output_dir: data/
  partition_scheme: year-quarter

generation:
  num_products: 10000
  num_customers: 1000000
  parallel_workers: 4

benchmarking:
  default_rounds: 5
  warmup_rounds: 2
  fail_on_regression: 0.05
  output_dir: benchmark-results/

duckdb:
  threads: 4
  memory_limit: 8GB
  temp_directory: /tmp/duckdb

logging:
  level: INFO
  format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
```

**Usage**:
```bash
python -m src.cli.generate --config myconfig.yaml
```

## Python API Usage

All CLI commands are also available as Python functions:

```python
from src.cli import generate, benchmark, analyze

# Generate data programmatically
generate.run(rows=50000000, seed=42, format='parquet')

# Run benchmarks
results = benchmark.run_suite('all', rounds=5)
print(f"Tests passed: {results.passed}/{results.total}")

# Analyze query
plan = analyze.explain_query("SELECT * FROM sales_fact WHERE year = 2023")
print(f"Execution time: {plan.execution_time}s")
print(f"Partitions scanned: {plan.partitions_scanned}")
```

## References

- Click (CLI framework): https://click.palletsprojects.com/
- Rich (CLI formatting): https://rich.readthedocs.io/
- DuckDB Python API: https://duckdb.org/docs/api/python/overview
