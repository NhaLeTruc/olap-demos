# Query Pattern Contracts

**Feature**: OLAP Core Capabilities Tech Demo
**Date**: 2025-11-17
**Purpose**: Define standard query patterns with expected inputs, outputs, and performance SLAs

## Overview

This document specifies the 5 core query patterns that demonstrate OLAP capabilities. Each pattern includes SQL syntax, expected execution characteristics, and performance service level agreements (SLAs).

## Pattern 1: Multi-Dimensional Aggregation

**User Story**: P1 - Multi-Dimensional Sales Analysis

**Purpose**: Demonstrate fast aggregations across multiple dimensions

**Query Template**:
```sql
SELECT
    {dimension_1},
    {dimension_2},
    {dimension_3},
    SUM(revenue) as total_revenue,
    SUM(quantity) as total_quantity,
    COUNT(DISTINCT transaction_id) as transaction_count,
    AVG(unit_price) as avg_price
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_geography g ON f.geo_key = g.geo_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE {filter_conditions}
GROUP BY {dimension_1}, {dimension_2}, {dimension_3}
ORDER BY total_revenue DESC
LIMIT {top_n};
```

**Concrete Examples**:

### Example 1.1: Revenue by Region and Year
```sql
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

**Expected Output**:
```
| region         | year | total_revenue | unique_customers |
|----------------|------|---------------|------------------|
| North America  | 2023 | $45,234,567   | 523,451          |
| Europe         | 2023 | $32,123,456   | 412,332          |
| ...            | ...  | ...           | ...              |
```

**Performance SLA**:
- Dataset: 100M rows
- Target: p95 < 2 seconds
- Partition Pruning: If year filter applied, expect 75% reduction in scanned data

### Example 1.2: Product Category Performance by Quarter
```sql
SELECT
    p.category,
    t.year,
    t.quarter,
    SUM(f.revenue) as total_revenue,
    SUM(f.profit) as total_profit,
    SUM(f.profit) / SUM(f.revenue) * 100 as profit_margin_pct
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE t.year = 2023
GROUP BY p.category, t.year, t.quarter
ORDER BY total_revenue DESC;
```

**Expected Output**:
```
| category    | year | quarter | total_revenue | total_profit | profit_margin_pct |
|-------------|------|---------|---------------|--------------|-------------------|
| Electronics | 2023 | Q4      | $12,456,789   | $3,123,456   | 25.07             |
| Clothing    | 2023 | Q4      | $8,234,567    | $2,456,789   | 29.84             |
| ...         | ...  | ...     | ...           | ...          | ...               |
```

**Performance SLA**:
- Dataset: 100M rows
- Target: p95 < 1 second (with year=2023 filter, partition pruning active)
- Expected Partition Pruning: Scan only 4 partitions (one year)

## Pattern 2: Drill-Down Analysis

**User Story**: P1 - Multi-Dimensional Sales Analysis

**Purpose**: Demonstrate hierarchical drill-down with consistent performance

**Query Sequence**:

### Level 1: Year (Highest Aggregation)
```sql
SELECT
    t.year,
    SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.year
ORDER BY t.year;
```

**Performance SLA**: p95 < 2 seconds on 100M rows

### Level 2: Quarter (Drill-Down)
```sql
SELECT
    t.year,
    t.quarter,
    SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year = 2023  -- User drills into 2023
GROUP BY t.year, t.quarter
ORDER BY t.year, t.quarter;
```

**Performance SLA**: p95 < 1 second (partition pruning reduces scan to 1 year)

### Level 3: Month (Further Drill-Down)
```sql
SELECT
    t.year,
    t.quarter,
    t.month,
    t.month_name,
    SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
WHERE t.year = 2023 AND t.quarter = 'Q2'  -- User drills into Q2 2023
GROUP BY t.year, t.quarter, t.month, t.month_name
ORDER BY t.month;
```

**Performance SLA**: p95 < 500ms (partition pruning reduces scan to 1 quarter)

**Expected Execution**:
- Each drill-down level reduces data scanned via partition pruning
- Query time should decrease or remain constant as user drills down
- Observable in EXPLAIN ANALYZE: fewer partitions scanned

## Pattern 3: Window Functions (Time-Series Analysis)

**User Story**: P2 - Time-Series Trend Analysis

**Purpose**: Demonstrate window function performance for analytical calculations

### Example 3.1: Moving Average (3-Month)
```sql
WITH monthly_revenue AS (
    SELECT
        t.year,
        t.month,
        t.month_name,
        SUM(f.revenue) as monthly_revenue
    FROM sales_fact f
    JOIN dim_time t ON f.time_key = t.time_key
    GROUP BY t.year, t.month, t.month_name
)
SELECT
    year,
    month,
    month_name,
    monthly_revenue,
    AVG(monthly_revenue) OVER (
        ORDER BY year, month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3m,
    monthly_revenue - LAG(monthly_revenue, 12) OVER (ORDER BY year, month) as yoy_change
FROM monthly_revenue
ORDER BY year, month;
```

**Expected Output**:
```
| year | month | month_name | monthly_revenue | moving_avg_3m | yoy_change   |
|------|-------|------------|-----------------|---------------|--------------|
| 2023 | 1     | January    | $3,456,789      | $3,234,567    | +$456,789    |
| 2023 | 2     | February   | $3,234,567      | $3,345,678    | +$234,567    |
| ...  | ...   | ...        | ...             | ...           | ...          |
```

**Performance SLA**:
- Dataset: 100M rows
- Aggregation Step: p95 < 3 seconds
- Window Function Step: p95 < 1 second (operating on aggregated monthly data ~36 rows)
- Total: p95 < 3 seconds

### Example 3.2: Product Rankings by Quarter
```sql
WITH quarterly_product_sales AS (
    SELECT
        p.product_name,
        p.category,
        t.year,
        t.quarter,
        SUM(f.revenue) as total_revenue
    FROM sales_fact f
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_time t ON f.time_key = t.time_key
    WHERE t.year = 2023
    GROUP BY p.product_name, p.category, t.year, t.quarter
)
SELECT
    year,
    quarter,
    category,
    product_name,
    total_revenue,
    ROW_NUMBER() OVER (PARTITION BY year, quarter, category ORDER BY total_revenue DESC) as rank_in_category,
    PERCENT_RANK() OVER (PARTITION BY year, quarter ORDER BY total_revenue DESC) as percentile
FROM quarterly_product_sales
WHERE year = 2023
QUALIFY rank_in_category <= 10  -- Top 10 per category per quarter
ORDER BY year, quarter, category, rank_in_category;
```

**Performance SLA**:
- Dataset: 100M rows (filtered to 2023)
- Target: p95 < 2 seconds
- Partition Pruning: Year filter reduces scan by 66%

## Pattern 4: Storage Comparison

**User Story**: P3 - Storage Efficiency Demonstration

**Purpose**: Compare columnar vs row-based storage performance

**Test Query** (same query executed on both storage formats):
```sql
SELECT
    p.category,
    t.year,
    SUM(f.revenue) as total_revenue,
    AVG(f.unit_price) as avg_price,
    COUNT(*) as transaction_count
FROM sales_fact f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY p.category, t.year
ORDER BY total_revenue DESC;
```

**Execution Variants**:
- **Parquet (Columnar)**: `FROM 'data/parquet/sales_fact/**/*.parquet' f`
- **CSV (Row-Based)**: `FROM 'data/csv/sales_fact.csv' f`

**Expected Metrics**:

| Storage Format | File Size | Scan Time (p95) | Bytes Scanned | Rows Scanned | Speedup |
|----------------|-----------|-----------------|---------------|--------------|---------|
| Parquet | 3 GB | 1.5s | 3 GB | 100M | Baseline |
| CSV | 18 GB | 45s | 18 GB | 100M | 30x slower |

**Observable Metrics** (DuckDB EXPLAIN ANALYZE):
- Bytes read from disk
- Rows scanned vs rows returned
- Execution time
- Memory usage

**Performance SLA**:
- Parquet: p95 < 2 seconds
- CSV: Expected to be 10-50x slower (demonstrates columnar benefit)
- Compression Ratio: Parquet should be 5-7x smaller than CSV

### Selective Column Query (I/O Efficiency)
```sql
-- Query reads only 3 columns: transaction_date, revenue, quantity
SELECT
    DATE_TRUNC('month', transaction_date) as month,
    SUM(revenue) as total_revenue,
    SUM(quantity) as total_quantity
FROM sales_fact
GROUP BY DATE_TRUNC('month', transaction_date)
ORDER BY month;
```

**Expected I/O Comparison**:

| Storage Format | Total Columns | Columns Needed | Bytes Scanned | I/O Efficiency |
|----------------|---------------|----------------|---------------|----------------|
| Parquet | 20 | 3 | ~450 MB | 85% reduction |
| CSV | 20 | 3 | 18 GB | 0% reduction (full scan) |

**Performance SLA**:
- Parquet: p95 < 500ms (reads 15% of data)
- CSV: p95 > 30s (reads 100% of data)
- Demonstrates columnar I/O efficiency: 60x speedup

## Pattern 5: Partition Pruning Validation

**User Story**: P1 - Multi-Dimensional Sales Analysis

**Purpose**: Demonstrate partition pruning effectiveness

### Example 5.1: Full Table Scan (No Partition Filter)
```sql
SELECT SUM(revenue) as total_revenue
FROM sales_fact;
```

**Expected Execution**:
- Partitions Scanned: All (16 partitions across 4 years)
- Rows Scanned: 100M
- Execution Time: p95 < 3 seconds

### Example 5.2: Year Filter (Partition Pruning)
```sql
SELECT SUM(revenue) as total_revenue
FROM sales_fact
WHERE transaction_date >= '2023-01-01' AND transaction_date <= '2023-12-31';
```

**Expected Execution**:
- Partitions Scanned: 4 (year=2023 only)
- Partition Pruning: 75% of partitions skipped
- Rows Scanned: ~25M (25% of dataset)
- Execution Time: p95 < 1 second (4x faster than full scan)

### Example 5.3: Quarter Filter (Maximum Pruning)
```sql
SELECT SUM(revenue) as total_revenue
FROM sales_fact
WHERE transaction_date >= '2023-04-01' AND transaction_date <= '2023-06-30';
```

**Expected Execution**:
- Partitions Scanned: 1 (year=2023/quarter=Q2)
- Partition Pruning: 93% of partitions skipped
- Rows Scanned: ~6.25M (6.25% of dataset)
- Execution Time: p95 < 500ms (6x faster than full scan)

**Validation via EXPLAIN**:
```sql
EXPLAIN ANALYZE
SELECT SUM(revenue) as total_revenue
FROM sales_fact
WHERE transaction_date >= '2023-04-01' AND transaction_date <= '2023-06-30';
```

**Expected EXPLAIN Output**:
```
QUERY PLAN
───────────────────────────────────────────────────────────────
AGGREGATE
│   Expressions: SUM(revenue)
│   Estimated Rows: 1
└── PARQUET_SCAN (data/parquet/sales_fact/year=2023/quarter=Q2/*.parquet)
    │   Filters: transaction_date >= '2023-04-01' AND transaction_date <= '2023-06-30'
    │   Partitions Scanned: 1/16
    │   Rows Scanned: 6,250,000
    │   Bytes Scanned: 187 MB
    └── Execution Time: 0.42s
```

## Pattern 6: Concurrent Query Execution

**User Story**: P4 - Scalability Validation

**Purpose**: Demonstrate concurrent query performance

**Test Setup**:
- Execute 10 identical queries in parallel (using pytest-xdist)
- Measure average query time vs single-query baseline

**Test Query** (same as Pattern 1.1):
```sql
SELECT
    g.region,
    t.year,
    SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
JOIN dim_geography g ON f.geo_key = g.geo_key
GROUP BY g.region, t.year
ORDER BY total_revenue DESC;
```

**Performance SLA**:

| Concurrency | Avg Query Time | vs Baseline | Acceptable? |
|-------------|----------------|-------------|-------------|
| 1 (baseline) | 1.5s | 1.0x | ✅ |
| 5 concurrent | 2.0s | 1.33x | ✅ (<2x) |
| 10 concurrent | 2.8s | 1.87x | ✅ (<2x) |
| 20 concurrent | 4.5s | 3.0x | ❌ (>2x threshold) |

**Acceptance Criteria**:
- 10 concurrent queries: average latency <2x single-query baseline
- No query failures or timeouts
- Total throughput: >5 queries/second

## Performance Benchmarking Standards

### Benchmark Execution Requirements

**Hardware Baseline**:
- CPU: Modern quad-core (e.g., Intel i5/i7, AMD Ryzen 5/7)
- RAM: 16 GB minimum
- Storage: SSD (NVMe preferred)

**Benchmark Configuration**:
- Warm cache: Run query 2x before measurement
- Measurement rounds: Minimum 5 runs
- Statistics: Report p50, p95, p99
- Variance threshold: <10% coefficient of variation

**DuckDB Configuration**:
```python
import duckdb

conn = duckdb.connect('data/duckdb/olap_demo.duckdb')
conn.execute("SET threads TO 4")  # Consistent parallelism
conn.execute("SET memory_limit='8GB'")  # Consistent memory
```

### Metric Collection

**Required Metrics**:
1. Execution Time (ms)
2. Rows Scanned
3. Bytes Scanned
4. Partitions Accessed
5. Memory Usage (peak)

**Collection Method**:
```python
# Using DuckDB profiling
result = conn.execute("EXPLAIN ANALYZE " + query).fetchall()

# Parse metrics from explain plan
metrics = {
    'execution_time_ms': extract_time(result),
    'rows_scanned': extract_rows(result),
    'bytes_scanned': extract_bytes(result),
    'partitions_scanned': extract_partitions(result)
}
```

## Contract Validation

Each query pattern MUST satisfy:

1. **Correctness**: Query returns expected row count and aggregated values
2. **Performance SLA**: Execution time within p95 threshold
3. **Partition Pruning** (where applicable): Partitions scanned matches expectations
4. **Observability**: EXPLAIN ANALYZE provides detailed metrics
5. **Reproducibility**: Fixed dataset (SEED=42) produces identical results

**Validation Test Template**:
```python
def test_query_pattern_1_1_performance(benchmark, duckdb_conn):
    """Validate Pattern 1.1: Revenue by Region and Year"""
    query = """
        SELECT g.region, t.year, SUM(f.revenue) as total_revenue
        FROM sales_fact f
        JOIN dim_time t ON f.time_key = t.time_key
        JOIN dim_geography g ON f.geo_key = g.geo_key
        GROUP BY g.region, t.year
        ORDER BY total_revenue DESC
    """

    # Execute benchmark
    result = benchmark(lambda: duckdb_conn.execute(query).fetchall())

    # Validate correctness
    assert len(result) > 0, "Query returned no results"

    # Validate SLA
    assert benchmark.stats.stats.p95 < 2000.0, f"SLA violation: p95={benchmark.stats.stats.p95}ms > 2000ms"
```

## References

- DuckDB SQL Reference: https://duckdb.org/docs/sql/introduction
- Window Functions: https://duckdb.org/docs/sql/window_functions
- Parquet Partitioning: https://duckdb.org/docs/data/partitioning/hive_partitioning
- EXPLAIN ANALYZE: https://duckdb.org/docs/guides/meta/explain
