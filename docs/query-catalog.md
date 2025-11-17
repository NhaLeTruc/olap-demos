# OLAP Query Catalog

This document provides a catalog of predefined query patterns for OLAP demonstrations, including example queries, expected results, and execution plan insights.

## Table of Contents

- [User Story 1: Multi-Dimensional Aggregations](#user-story-1-multi-dimensional-aggregations)
- [User Story 2: Time-Series Trend Analysis](#user-story-2-time-series-trend-analysis)

---

## User Story 1: Multi-Dimensional Aggregations

### 1.1 Revenue by Region and Year

**Purpose**: Demonstrate fast multi-dimensional aggregation across geography and time dimensions.

**Query Pattern**:
```python
from src.query.patterns import QueryPatterns

patterns = QueryPatterns(executor)
result = patterns.revenue_by_dimensions(
    dimensions=['year', 'country'],
    limit=10
)
```

**Equivalent SQL**:
```sql
SELECT
    dt.year,
    dg.country,
    SUM(fs.revenue) as total_revenue,
    SUM(fs.profit) as total_profit,
    SUM(fs.quantity) as total_quantity,
    COUNT(*) as transaction_count,
    AVG(fs.revenue) as avg_revenue
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_geography dg ON fs.geo_key = dg.geo_key
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
GROUP BY dt.year, dg.country
ORDER BY total_revenue DESC
LIMIT 10
```

**Expected Output**:
```
| year | country        | total_revenue | total_profit | transaction_count |
|------|----------------|---------------|--------------|-------------------|
| 2024 | United States  | 45,234,567.89 | 12,345,678.90| 125,432           |
| 2024 | United Kingdom | 32,456,789.01 |  8,765,432.10|  98,765           |
| 2023 | United States  | 42,123,456.78 | 11,234,567.89| 118,654           |
...
```

**Execution Plan Insights**:
- **Join Strategy**: Hash join on dimension keys
- **Aggregation**: Parallel aggregation across row groups
- **Performance**: p95 <2s on 100M rows
- **I/O Pattern**: Columnar reads only required columns (time_key, geo_key, revenue, profit, quantity)

### 1.2 Category Performance by Quarter with Year Filter

**Purpose**: Demonstrate partition pruning effectiveness with temporal filters.

**Query Pattern**:
```python
result = patterns.revenue_by_dimensions(
    dimensions=['quarter', 'category'],
    filters={'year': 2024},
    limit=20
)
```

**Equivalent SQL**:
```sql
SELECT
    dt.quarter,
    dp.category,
    SUM(fs.revenue) as total_revenue,
    SUM(fs.profit) as total_profit,
    COUNT(*) as transaction_count
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_geography dg ON fs.geo_key = dg.geo_key
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
WHERE dt.year = 2024
GROUP BY dt.quarter, dp.category
ORDER BY total_revenue DESC
LIMIT 20
```

**Expected Output**:
```
| quarter | category    | total_revenue | total_profit | transaction_count |
|---------|-------------|---------------|--------------|-------------------|
| Q4      | Electronics | 15,234,567.89 | 4,123,456.78 | 45,234            |
| Q3      | Electronics | 14,987,654.32 | 3,987,654.32 | 43,876            |
| Q4      | Clothing    | 12,345,678.90 | 3,456,789.01 | 38,765            |
...
```

**Execution Plan Insights**:
- **Partition Pruning**: Only reads year=2024 partitions (75% data skipped)
- **Filter Pushdown**: Year filter applied at Parquet read level
- **Performance**: p95 <1s (faster due to partition pruning)

### 1.3 Hierarchical Drill-Down (Year → Quarter → Month)

**Purpose**: Demonstrate hierarchical navigation through time dimension.

**Query Pattern**:
```python
# Year level (shows quarters)
year_result = patterns.drill_down_time_hierarchy(year=2024)

# Quarter level (shows months)
quarter_result = patterns.drill_down_time_hierarchy(year=2024, quarter='Q1')

# Month level (shows days)
month_result = patterns.drill_down_time_hierarchy(year=2024, quarter='Q1', month=1)
```

**Expected Output (Year Level)**:
```
| quarter | quarterly_revenue | transaction_count |
|---------|-------------------|-------------------|
| Q1      | 35,234,567.89     | 98,765            |
| Q2      | 38,456,789.01     | 105,432           |
| Q3      | 42,123,456.78     | 115,678           |
| Q4      | 45,678,901.23     | 125,987           |
```

**Expected Output (Month Level)**:
```
| month | month_name | monthly_revenue | transaction_count |
|-------|------------|-----------------|-------------------|
| 1     | January    | 11,234,567.89   | 32,145            |
| 2     | February   | 10,987,654.32   | 31,234            |
| 3     | March      | 13,012,345.68   | 35,386            |
```

**Execution Plan Insights**:
- **Filter Selectivity**: Each drill-down level increases filter selectivity
- **Performance**: p95 <1s for all drill-down levels
- **Partition Access**: Month-level queries access only 1 partition

---

## User Story 2: Time-Series Trend Analysis

### 2.1 3-Month Moving Average

**Purpose**: Demonstrate window functions for smoothing revenue trends.

**Query Pattern**:
```python
result = patterns.moving_average_revenue(
    window_size=3,
    year=2024
)
```

**Equivalent SQL**:
```sql
SELECT
    dt.year,
    dt.month,
    dt.month_name,
    SUM(fs.revenue) as monthly_revenue,
    AVG(SUM(fs.revenue)) OVER (
        ORDER BY dt.year, dt.month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_3m
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year = 2024
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month
```

**Expected Output**:
```
| year | month | month_name | monthly_revenue | moving_avg_3m |
|------|-------|------------|-----------------|---------------|
| 2024 | 1     | January    | 11,234,567.89   | 11,234,567.89 |
| 2024 | 2     | February   | 10,987,654.32   | 11,111,111.11 |
| 2024 | 3     | March      | 13,012,345.68   | 11,744,855.96 |
| 2024 | 4     | April      | 12,456,789.01   | 12,152,262.67 |
...
```

**Execution Plan Insights**:
- **Window Function**: ROWS frame with 2 PRECEDING
- **Aggregation**: Two-stage aggregation (GROUP BY + WINDOW)
- **Performance**: p95 <3s on 100M rows
- **Memory**: Window frame maintained in memory

### 2.2 Year-over-Year Growth by Category

**Purpose**: Demonstrate LAG window function for temporal comparisons.

**Query Pattern**:
```python
result = patterns.yoy_growth(
    metric='revenue',
    dimension='category'
)
```

**Equivalent SQL**:
```sql
SELECT
    dt.year,
    dp.category as dimension,
    SUM(fs.revenue) as current_year_revenue,
    LAG(SUM(fs.revenue), 1) OVER (
        PARTITION BY dp.category
        ORDER BY dt.year
    ) as previous_year_revenue,
    ROUND(
        (SUM(fs.revenue) - LAG(SUM(fs.revenue), 1) OVER (
            PARTITION BY dp.category
            ORDER BY dt.year
        )) * 100.0 / LAG(SUM(fs.revenue), 1) OVER (
            PARTITION BY dp.category
            ORDER BY dt.year
        ),
        2
    ) as yoy_growth_pct
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
JOIN dim_product dp ON fs.product_key = dp.product_key
GROUP BY dt.year, dp.category
ORDER BY dt.year, dimension
```

**Expected Output**:
```
| year | dimension   | current_year_revenue | previous_year_revenue | yoy_growth_pct |
|------|-------------|----------------------|-----------------------|----------------|
| 2022 | Electronics | 45,234,567.89        | NULL                  | NULL           |
| 2023 | Electronics | 52,456,789.01        | 45,234,567.89         | 15.96          |
| 2024 | Electronics | 58,123,456.78        | 52,456,789.01         | 10.80          |
| 2022 | Clothing    | 32,123,456.78        | NULL                  | NULL           |
| 2023 | Clothing    | 35,987,654.32        | 32,123,456.78         | 12.03          |
...
```

**Execution Plan Insights**:
- **Window Function**: LAG with PARTITION BY category
- **Partitioning**: Separate window partitions per category
- **Performance**: p95 <3s
- **Calculation**: Growth percentage computed in single pass

### 2.3 Top 10 Products by Category

**Purpose**: Demonstrate ROW_NUMBER window function for rankings within partitions.

**Query Pattern**:
```python
result = patterns.product_rankings(
    partition_by='category',
    rank_by='revenue',
    year=2024,
    top_n=10
)
```

**Equivalent SQL**:
```sql
WITH ranked_products AS (
    SELECT
        dp.category as partition_key,
        dp.product_name,
        SUM(fs.revenue) as total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY dp.category
            ORDER BY SUM(fs.revenue) DESC
        ) as rank
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    WHERE dt.year = 2024
    GROUP BY dp.category, dp.product_name
)
SELECT *
FROM ranked_products
WHERE rank <= 10
ORDER BY partition_key, rank
```

**Expected Output**:
```
| partition_key | product_name              | total_revenue | rank |
|---------------|---------------------------|---------------|------|
| Electronics   | BrandA Laptop Blue        | 2,345,678.90  | 1    |
| Electronics   | BrandB Smartphone Red     | 1,987,654.32  | 2    |
| Electronics   | BrandC Tablet Green       | 1,765,432.10  | 3    |
...
| Clothing      | BrandD Mens Shirt Black   | 987,654.32    | 1    |
| Clothing      | BrandE Womens Dress White | 876,543.21    | 2    |
...
```

**Execution Plan Insights**:
- **Window Function**: ROW_NUMBER with PARTITION BY and ORDER BY
- **CTE Optimization**: DuckDB optimizes CTE as inline subquery
- **Performance**: p95 <2s with year filter
- **Top-N Pushdown**: QUALIFY clause filters ranks efficiently

---

## Performance Summary

| Query Pattern                  | Complexity | SLA (p95) | Primary Optimization      |
|--------------------------------|------------|-----------|---------------------------|
| Multi-dimensional aggregation  | Medium     | <2s       | Parallel aggregation      |
| Filtered aggregation           | Low        | <1s       | Partition pruning         |
| Hierarchical drill-down        | Low        | <1s       | Filter selectivity        |
| Moving average (3-month)       | Medium     | <3s       | Window frame optimization |
| Year-over-year growth          | Medium     | <3s       | Partitioned LAG           |
| Product rankings               | Medium     | <2s       | Top-N pushdown            |

---

## Execution Plan Keywords

When analyzing EXPLAIN output, look for these indicators of good performance:

**Partition Pruning**:
- `PARQUET_SCAN` with `Filters` applied
- `Partition filters: year=2024` in plan
- Reduced `est. cardinality` compared to table size

**Columnar I/O**:
- `Projections` list showing only required columns
- `Column pruning` in Parquet scan
- Lower `est. bytes scanned` than total file size

**Parallel Execution**:
- `HASH_GROUP_BY` with parallel workers
- `HASH_JOIN` with build/probe parallelization
- `Pipeline parallelism` in plan

**Window Function Optimization**:
- `WINDOW` operator with optimized frame
- `Partitioning on` for PARTITION BY clauses
- `Streaming window` for unbounded frames

---

## Testing Queries

To validate query patterns work correctly:

```bash
# Generate test data
olap-generate --rows 100000 --seed 42

# Test multi-dimensional aggregation
olap-analyze --query "SELECT year, country, SUM(revenue) FROM fact_sales fs JOIN dim_time dt ON fs.time_key = dt.time_key JOIN dim_geography dg ON fs.geo_key = dg.geo_key GROUP BY year, country LIMIT 10"

# Run full benchmark suite
olap-benchmark --rounds 3

# Interactive exploration
olap-analyze --interactive
```

---

## Additional Resources

- **Execution Plans**: Use `olap-analyze --profile <query>` to see detailed execution plans
- **Benchmarks**: See `tests/benchmarks/` for performance test implementations
- **Architecture**: See `docs/architecture.md` for system design details
