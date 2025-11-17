# Quickstart Guide: OLAP Core Capabilities Tech Demo

**Last Updated**: 2025-11-17
**Estimated Time**: 15-30 minutes
**Prerequisites**: Python 3.11+, 16GB RAM, 15GB disk space

## Overview

This guide helps you get the OLAP demo running locally in under 30 minutes. You'll:
1. Set up the Python environment
2. Generate a 10M row sample dataset
3. Run sample queries to explore OLAP capabilities
4. Execute benchmarks to validate performance

## Quick Start (5 Minutes)

**For the impatient** - get a working demo in 5 commands:

```bash
# 1. Clone and navigate
cd olap-demos

# 2. Install dependencies
pip install -e ".[dev]"

# 3. Generate 10M row dataset (takes ~2 minutes)
python -m src.cli.generate --rows 10000000

# 4. Run sample query
duckdb data/duckdb/olap_demo.duckdb -c "SELECT region, SUM(revenue) as total FROM sales_fact f JOIN dim_geography g ON f.geo_key = g.geo_key GROUP BY region"

# 5. Run benchmarks
pytest tests/benchmarks/test_aggregations.py -v
```

**Expected Result**: You should see query results in <2 seconds and passing benchmark tests.

## Detailed Setup

### Step 1: Environment Setup

#### Option A: Using pip (Recommended)

```bash
# Ensure Python 3.11+ is installed
python --version  # Should show 3.11 or higher

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install project with development dependencies
pip install -e ".[dev]"

# Verify installation
python -c "import duckdb; print(f'DuckDB {duckdb.__version__} installed')"
```

**Expected Output**:
```
DuckDB 0.9.2 installed
```

#### Option B: Using uv (Faster)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create environment and install dependencies
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Step 2: Generate Data

#### Small Dataset (Quick Testing)

```bash
# Generate 1M rows (~30 seconds)
python -m src.cli.generate --rows 1000000 --verbose
```

**Expected Output**:
```
[INFO] Starting data generation...
[INFO] Phase 1/3: Generating dimension tables
  ✓ dim_time (1,095 rows) - 0.1s
  ✓ dim_geography (5,000 rows) - 0.3s
  ✓ dim_product (10,000 rows) - 0.5s
  ✓ dim_customer (1,000,000 rows) - 12.4s
  ✓ dim_payment (20 rows) - 0.1s
[INFO] Phase 2/3: Generating fact table
  ✓ Generated 1,000,000 rows - 14.2s
[INFO] Phase 3/3: Writing to Parquet
  ✓ Total written: 120 MB compressed
[SUCCESS] Data generation complete in 28.5s
```

#### Standard Dataset (Benchmarking)

```bash
# Generate 10M rows (~3 minutes)
python -m src.cli.generate --rows 10000000
```

#### Full Dataset (Complete Demo)

```bash
# Generate 100M rows (~25 minutes)
python -m src.cli.generate --rows 100000000 --parallel 8
```

**Storage Requirements**:

| Rows | Parquet | CSV (optional) | Generation Time |
|------|---------|----------------|-----------------|
| 1M | ~120 MB | ~800 MB | ~30 seconds |
| 10M | ~1.2 GB | ~8 GB | ~3 minutes |
| 100M | ~12 GB (compressed 3 GB) | ~80 GB | ~25 minutes |

**Pro Tip**: Start with 1M rows for quick iteration, then scale up to 100M for final benchmarks.

### Step 3: Verify Data

```bash
# Check generated files
ls -lh data/parquet/

# Should see:
# dim_time.parquet
# dim_geography.parquet
# dim_product.parquet
# dim_customer.parquet
# dim_payment.parquet
# sales_fact/ (directory with partitioned data)
```

```bash
# Verify row counts with DuckDB CLI
duckdb data/duckdb/olap_demo.duckdb
```

```sql
-- Inside DuckDB shell
SELECT COUNT(*) as row_count FROM sales_fact;
-- Should show 10,000,000 (or your chosen row count)

SELECT COUNT(*) as partition_count
FROM (
    SELECT DISTINCT year, quarter
    FROM sales_fact
);
-- Should show 12 (3 years × 4 quarters)

.exit
```

## Exploring OLAP Capabilities

### Interactive Queries (DuckDB CLI)

```bash
# Start DuckDB interactive shell
duckdb data/duckdb/olap_demo.duckdb
```

#### Example 1: Multi-Dimensional Aggregation

```sql
-- Revenue by region and year
SELECT
    g.region,
    t.year,
    SUM(f.revenue) as total_revenue,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_geography g ON f.geo_key = g.geo_key
GROUP BY g.region, t.year
ORDER BY total_revenue DESC;
```

**Expected**: Results in <2 seconds for 10M rows

#### Example 2: Drill-Down Analysis

```sql
-- Start with yearly totals
SELECT t.year, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.year;

-- Drill down to quarterly
SELECT t.year, t.quarter, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year = 2023
GROUP BY t.year, t.quarter;

-- Drill down to monthly
SELECT t.year, t.quarter, t.month_name, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year = 2023 AND t.quarter = 'Q2'
GROUP BY t.year, t.quarter, t.month, t.month_name
ORDER BY t.month;
```

**Expected**: Each drill-down faster due to partition pruning

#### Example 3: Window Functions

```sql
-- 3-month moving average
WITH monthly_revenue AS (
    SELECT
        t.year,
        t.month,
        SUM(f.revenue) as monthly_revenue
    FROM sales_fact f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.month
)
SELECT
    year,
    month,
    monthly_revenue,
    AVG(monthly_revenue) OVER (
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3m
FROM monthly_revenue
ORDER BY year, month;
```

**Expected**: Results in <3 seconds

### Analyzing Query Performance

#### View Execution Plan

```sql
EXPLAIN ANALYZE
SELECT g.region, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_geography g ON f.geo_key = g.geo_key
GROUP BY g.region;
```

**What to Look For**:
- `PARQUET_SCAN`: Shows columnar scan efficiency
- `Rows Scanned`: Should be all rows (no filter)
- `Bytes Scanned`: Should be much less than total file size (column pruning)
- `Execution Time`: Should be <2s for 10M rows

#### Demonstrate Partition Pruning

```sql
-- Full table scan (no filter)
EXPLAIN ANALYZE
SELECT SUM(revenue) FROM sales_fact;

-- With year filter (partition pruning)
EXPLAIN ANALYZE
SELECT SUM(revenue) FROM sales_fact
WHERE transaction_date >= '2023-01-01' AND transaction_date <= '2023-12-31';
```

**Compare**:
- First query: Scans all 12 partitions
- Second query: Scans only 4 partitions (year=2023)
- Expect ~3x speedup with partition pruning

### Using CLI Tools

#### Generate Comparison Data (Parquet vs CSV)

```bash
# Generate both formats
python -m src.cli.generate --rows 10000000 --format both
```

#### Analyze Query Execution

```bash
# Analyze a query and get recommendations
python -m src.cli.analyze \
  "SELECT region, SUM(revenue) FROM sales_fact f JOIN dim_geography g ON f.geo_key = g.geo_key GROUP BY region"
```

**Expected Output**: Detailed execution plan with optimization recommendations

## Running Benchmarks

### Quick Benchmark (Single User Story)

```bash
# Test P1: Multi-dimensional aggregations only
pytest tests/benchmarks/test_aggregations.py -v
```

**Expected Output**:
```
tests/benchmarks/test_aggregations.py::test_revenue_by_region_and_year PASSED [p95: 1.23s < 2.00s SLA]
tests/benchmarks/test_aggregations.py::test_category_by_quarter PASSED [p95: 0.67s < 1.00s SLA]
tests/benchmarks/test_aggregations.py::test_drill_down PASSED [p95: 0.42s < 1.00s SLA]

3 passed in 12.4s
```

### Full Benchmark Suite

```bash
# Run all benchmarks (takes ~10-15 minutes on 100M rows)
pytest tests/benchmarks/ -v --benchmark-only
```

**Benchmark Phases**:
1. P1 - Multi-Dimensional Aggregations (3 tests)
2. P2 - Window Functions (2 tests)
3. P3 - Storage Comparison (2 tests)
4. P4 - Scalability (2 tests)

### Generate Benchmark Report

```bash
# Create HTML report
python -m src.cli.benchmark all --format html --output benchmark-results/reports/
```

**Output**: `benchmark-results/reports/report-<date>.html` (open in browser)

## Troubleshooting

### Issue: "Module not found"

**Symptom**:
```
ModuleNotFoundError: No module named 'duckdb'
```

**Solution**:
```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -e ".[dev]"
```

### Issue: "Out of memory" during data generation

**Symptom**:
```
MemoryError: Unable to allocate array
```

**Solution**:
```bash
# Reduce parallel workers
python -m src.cli.generate --rows 100000000 --parallel 2

# Or generate in smaller batches
python -m src.cli.generate --rows 50000000
python -m src.cli.generate --rows 50000000 --start-date 2023-07-01 --end-date 2024-12-31 --overwrite
```

### Issue: "Disk space full"

**Symptom**:
```
OSError: No space left on device
```

**Solution**:
```bash
# Check available space
df -h

# Generate smaller dataset
python -m src.cli.generate --rows 10000000  # Only ~1.2 GB

# Or use CSV format only (skip Parquet)
python -m src.cli.generate --rows 10000000 --format csv
```

### Issue: Benchmark tests failing

**Symptom**:
```
FAILED [p95: 3.45s > 2.00s SLA]
```

**Possible Causes**:
1. **Underpowered hardware**: SLAs tuned for modern quad-core CPU
   - Solution: Increase `--benchmark-rounds` for more stable measurements
   - Or adjust SLA thresholds in `tests/benchmarks/conftest.py`

2. **Background processes**: Other applications consuming resources
   - Solution: Close unnecessary applications before benchmarking

3. **Cold cache**: First query after system restart
   - Solution: Run warmup queries first or use `--warmup` option

## Next Steps

### Explore Codebase

```bash
# View project structure
tree -L 3 src/

# Read source code
cat src/query/engine.py       # DuckDB query execution
cat src/datagen/generator.py  # Data generation logic
cat src/storage/parquet_handler.py  # Parquet I/O
```

### Run Unit Tests

```bash
# Fast unit tests
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v
```

### Extend the Demo

Ideas for exploration:
1. Add new query patterns (see `src/query/patterns.py`)
2. Experiment with different partitioning schemes
3. Try different compression codecs (Zstd vs Snappy)
4. Generate custom datasets with different distributions
5. Create visualizations of query performance

### Deploy to Production

See `docs/deployment.md` (future documentation) for:
- Docker containerization
- CI/CD integration
- Cloud deployment (AWS/GCP/Azure)
- Scaling to larger datasets (1B+ rows)

## Reference

### Useful DuckDB Commands

```sql
-- List all tables
SHOW TABLES;

-- Describe table schema
DESCRIBE sales_fact;

-- Check Parquet file metadata
SELECT * FROM parquet_metadata('data/parquet/sales_fact/year=2023/quarter=Q1/data.parquet');

-- Enable profiling
SET enable_profiling=true;
SET profiling_output='profile.json';

-- View query profile
SELECT * FROM pragma_last_profiling_output();
```

### Key Directories

```
olap-demos/
├── src/                    # Source code
│   ├── datagen/            # Data generation
│   ├── query/              # Query execution
│   ├── storage/            # Parquet/CSV handlers
│   └── cli/                # Command-line tools
├── tests/
│   ├── benchmarks/         # Performance benchmarks
│   ├── integration/        # Integration tests
│   └── unit/               # Unit tests
├── data/                   # Generated datasets (gitignored)
├── benchmark-results/      # Benchmark outputs
└── specs/001-olap-core-demo/  # This feature's docs
```

### Performance Expectations

**10M Row Dataset**:
- Multi-dimensional aggregation: <1s
- Drill-down: <500ms
- Window functions: <2s

**100M Row Dataset**:
- Multi-dimensional aggregation: <2s (p95 <5s)
- Drill-down: <1s
- Window functions: <3s
- Storage: ~3 GB Parquet (6x compression)

**Hardware Assumptions**:
- CPU: 4+ cores (modern Intel i5/i7 or AMD Ryzen 5/7)
- RAM: 16 GB
- Storage: SSD (NVMe preferred)

## Support

- **Documentation**: See `specs/001-olap-core-demo/` directory
- **Issues**: Report bugs via GitHub Issues
- **Questions**: Consult `docs/architecture.md` and `docs/query-catalog.md`

## Summary

You now have a working OLAP demo! Try:
1. ✅ Running queries in DuckDB CLI
2. ✅ Analyzing execution plans
3. ✅ Running benchmarks
4. ✅ Comparing Parquet vs CSV performance
5. ✅ Exploring partition pruning

**Next**: Read `specs/001-olap-core-demo/contracts/query-patterns.md` for detailed query examples and performance SLAs.

Happy querying! 🚀
