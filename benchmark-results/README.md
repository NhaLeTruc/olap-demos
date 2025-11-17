# Benchmark Results

This directory tracks performance benchmark results over time for the OLAP demo.

## Directory Structure

```
benchmark-results/
├── README.md           # This file
├── baseline/           # Baseline benchmark results (for comparison)
│   └── results-<date>.json
└── current/            # Current benchmark runs (gitignored)
    └── results-<date>.json
```

## Benchmark Tracking Approach

### Baseline Results

The `baseline/` directory contains reference benchmark results that represent expected performance characteristics. These are committed to version control to enable:

- **Regression Detection**: Compare current runs against baseline to detect performance regressions
- **Historical Tracking**: Track performance improvements or degradations over time
- **CI/CD Integration**: Automated performance validation in continuous integration

**When to Update Baseline**:
- After intentional performance optimizations that improve metrics
- When upgrading to new library versions (DuckDB, PyArrow, etc.)
- When changing dataset generation logic that affects query patterns
- Major version releases (document in CHANGELOG.md)

### Current Results

The `current/` directory contains recent benchmark runs and is gitignored to avoid repository bloat. Use this for:

- Local development and testing
- Performance debugging
- Comparing experimental changes

## Benchmark Files

Each benchmark result file follows this naming convention:
```
results-YYYY-MM-DD-HHmm.json
```

Example: `results-2025-11-17-1430.json`

## Result File Format

Benchmark results are stored in JSON format with this structure:

```json
{
  "metadata": {
    "timestamp": "2025-11-17T14:30:00Z",
    "dataset_size": 100000000,
    "duckdb_version": "0.9.2",
    "python_version": "3.11.5",
    "os": "Linux 5.15.0",
    "hardware": {
      "cpu": "Intel i7-9700K",
      "ram_gb": 32,
      "storage": "NVMe SSD"
    }
  },
  "benchmarks": [
    {
      "test_name": "test_revenue_by_region_and_year",
      "user_story": "US1",
      "query_pattern": "multi_dimensional_aggregation",
      "stats": {
        "min": 1.234,
        "max": 2.456,
        "mean": 1.789,
        "median": 1.756,
        "stddev": 0.123,
        "p50": 1.756,
        "p95": 1.987,
        "p99": 2.123
      },
      "sla": {
        "metric": "p95",
        "threshold": 2.0,
        "unit": "seconds",
        "passed": true
      },
      "metrics": {
        "rows_scanned": 100000000,
        "bytes_scanned": 3200000000,
        "partitions_scanned": 12,
        "partitions_total": 12
      }
    }
  ]
}
```

## Running Benchmarks

### Generate Baseline

```bash
# Run full benchmark suite and save as baseline
python -m src.cli.benchmark all --output benchmark-results/baseline/results-$(date +%Y-%m-%d).json

# Commit the baseline
git add benchmark-results/baseline/
git commit -m "benchmark: establish baseline for v1.0.0"
```

### Compare Against Baseline

```bash
# Run benchmarks and compare to baseline
python -m src.cli.benchmark all \
  --baseline benchmark-results/baseline/results-2025-11-17.json \
  --fail-on-regression 0.05

# Generate HTML comparison report
python -m src.cli.benchmark all \
  --baseline benchmark-results/baseline/results-2025-11-17.json \
  --format html \
  --output benchmark-results/current/comparison-$(date +%Y-%m-%d).html
```

## Benchmark SLAs

Constitution Principle II (Query Performance Excellence) defines these SLAs:

| Query Pattern | Metric | Target | Dataset |
|---------------|--------|--------|---------|
| Multi-dimensional aggregation | p95 | <2s | 100M rows |
| Category by quarter | p95 | <1s | 100M rows (filtered) |
| Drill-down | p95 | <1s | Filtered |
| Moving average | p95 | <3s | 100M rows |
| Product rankings | p95 | <2s | 100M rows (filtered) |
| Parquet vs CSV | p95 | 10-50x faster | 100M rows |
| Columnar I/O efficiency | - | <20% data scanned | Selective queries |
| Sub-linear scaling | p95 | <2.5x | 2x data increase |
| Concurrent queries | avg | <2x latency | 10 parallel |

## Performance Regression Policy

**Regression Threshold**: 5% (configurable with `--fail-on-regression`)

**If regression detected**:
1. Investigate root cause (code changes, data changes, environment)
2. Document findings in GitHub issue
3. Either:
   - Fix the regression, or
   - Justify the regression and update baseline if acceptable

**Automatic Failure**: CI/CD fails if p95 latency exceeds SLA threshold or regression >5%

## Benchmark Environment

For consistent results, benchmarks should be run on:

- **Quiet system**: Close background applications
- **Sufficient resources**: 16GB+ RAM, modern CPU (4+ cores)
- **Fresh data**: Generate datasets with same seed (SEED=42)
- **Multiple rounds**: Minimum 5 rounds with 2 warmup rounds
- **Disabled GC**: pytest-benchmark --benchmark-disable-gc

## Historical Tracking

When updating baselines, preserve previous baselines for historical reference:

```bash
# Move current baseline to archived/
mkdir -p benchmark-results/archived/
mv benchmark-results/baseline/results-2025-11-17.json benchmark-results/archived/

# Add new baseline
cp benchmark-results/current/results-2025-11-20.json benchmark-results/baseline/

# Commit with changelog
git add benchmark-results/
git commit -m "benchmark: update baseline after DuckDB 0.10.0 upgrade

- 15% improvement in aggregation queries
- 8% improvement in window functions
- See CHANGELOG.md for details"
```

## Notes

- Benchmark variance should be <10% (Constitution compliance)
- Hardware differences will affect absolute numbers but relative comparisons (Parquet vs CSV, scaling factors) should be consistent
- Always document hardware specs when sharing benchmark results
- Use deterministic data generation (SEED=42) for reproducibility
