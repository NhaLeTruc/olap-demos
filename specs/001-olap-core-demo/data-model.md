# Data Model: OLAP Core Capabilities Tech Demo

**Feature**: 001-olap-core-demo
**Date**: 2025-11-17
**Schema Type**: Star Schema
**Purpose**: Dimensional model for e-commerce sales analytics demonstration

## Schema Overview

### Star Schema Design

```
                    ┌─────────────────┐
                    │   dim_time      │
                    │  (Date/Time)    │
                    └────────┬────────┘
                             │
    ┌─────────────────┐      │      ┌─────────────────┐
    │ dim_geography   │      │      │  dim_product    │
    │   (Location)    │──────┼──────│   (Catalog)     │
    └─────────────────┘      │      └─────────────────┘
                             │
                    ┌────────▼────────┐
                    │   sales_fact    │
                    │  (Transactions) │
                    └────────┬────────┘
                             │
    ┌─────────────────┐      │      ┌─────────────────┐
    │  dim_customer   │──────┴──────│  dim_payment    │
    │  (Customers)    │             │    (Methods)    │
    └─────────────────┘             └─────────────────┘
```

**Grain**: One row per line item in each sales transaction
**Fact Table**: sales_fact (100M-200M rows)
**Dimensions**: 5 dimension tables (Time, Geography, Product, Customer, Payment)

## Fact Table

### sales_fact

**Description**: Sales transaction line items capturing individual product sales

**Grain**: One row per product sold per transaction

**Estimated Rows**: 100M-200M (configurable for different benchmark scenarios)

**Partitioning**: Hive-style partitioning by year and quarter
- Path: `data/parquet/sales_fact/year=YYYY/quarter=QN/`
- Partition Keys: `year`, `quarter`
- Partition Count: ~12-16 partitions (3-4 years × 4 quarters)

**Columns**:

| Column Name | Data Type | Description | Constraints | Nullable |
|-------------|-----------|-------------|-------------|----------|
| transaction_id | BIGINT | Unique transaction identifier | Primary Key component | NOT NULL |
| line_item_id | INTEGER | Line item number within transaction (1, 2, 3...) | Primary Key component | NOT NULL |
| transaction_date | DATE | Date of transaction | BETWEEN '2021-01-01' AND '2023-12-31' | NOT NULL |
| transaction_timestamp | TIMESTAMP | Precise timestamp of transaction | Millisecond precision | NOT NULL |
| time_key | INTEGER | Foreign key to dim_time | References dim_time.time_key | NOT NULL |
| geo_key | INTEGER | Foreign key to dim_geography | References dim_geography.geo_key | NOT NULL |
| product_key | INTEGER | Foreign key to dim_product | References dim_product.product_key | NOT NULL |
| customer_key | INTEGER | Foreign key to dim_customer | References dim_customer.customer_key | NOT NULL |
| payment_key | INTEGER | Foreign key to dim_payment | References dim_payment.payment_key | NOT NULL |
| quantity | INTEGER | Number of units purchased | >= 1, <= 100 | NOT NULL |
| unit_price | DECIMAL(10,2) | Price per unit in USD | > 0.00 | NOT NULL |
| revenue | DECIMAL(12,2) | Total revenue (quantity × unit_price) | Calculated field | NOT NULL |
| cost | DECIMAL(12,2) | Total cost to company | >= 0.00, < revenue | NOT NULL |
| discount_amount | DECIMAL(10,2) | Total discount applied | >= 0.00, <= revenue | NOT NULL |
| profit | DECIMAL(12,2) | Gross profit (revenue - cost) | Calculated field | NOT NULL |

**Indexes**: None explicitly (columnar format provides implicit column indexing)

**Compression**: Snappy compression via Parquet encoding

**Expected Storage**:
- Uncompressed (CSV): ~15-20 GB for 100M rows
- Compressed (Parquet): ~2-4 GB for 100M rows (5-7x compression)

**Cardinality Estimates**:
- transaction_id: ~30M unique (average 3-4 line items per transaction)
- product_key: ~10K unique products
- customer_key: ~1M unique customers
- geo_key: ~5K geographic locations
- time_key: ~1K unique days (3 years)

## Dimension Tables

### dim_time

**Description**: Date/time dimension with calendar hierarchy

**Type**: Conformed dimension (standard across all fact tables)

**Rows**: ~1,095 rows (3 years × 365 days)

**Slowly Changing Dimension**: Type 0 (static, dates don't change attributes)

**Columns**:

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| time_key | INTEGER | Surrogate key | 20230415 (YYYYMMDD format) |
| date | DATE | Calendar date | 2023-04-15 |
| year | INTEGER | Year | 2023 |
| quarter | VARCHAR(2) | Quarter | Q2 |
| quarter_number | INTEGER | Quarter as number | 2 |
| month | INTEGER | Month number | 4 |
| month_name | VARCHAR(20) | Month name | April |
| day_of_month | INTEGER | Day of month | 15 |
| day_of_week | INTEGER | Day of week (1=Monday) | 6 |
| day_name | VARCHAR(20) | Day name | Saturday |
| week_of_year | INTEGER | ISO week number | 15 |
| is_weekend | BOOLEAN | Weekend indicator | TRUE |
| is_holiday | BOOLEAN | Holiday indicator | FALSE |
| fiscal_year | INTEGER | Fiscal year (if different) | 2023 |
| fiscal_quarter | VARCHAR(2) | Fiscal quarter | Q1 |

**Hierarchies**:
- Date Hierarchy: Year → Quarter → Month → Day
- Week Hierarchy: Year → Week → Day

**Sample Query**:
```sql
SELECT t.year, t.quarter, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.year, t.quarter
ORDER BY t.year, t.quarter;
```

### dim_geography

**Description**: Geographic location hierarchy for spatial analysis

**Type**: Conformed dimension

**Rows**: ~5,000 rows (multi-level geographic hierarchy)

**Slowly Changing Dimension**: Type 1 (overwrite changes, e.g., city name corrections)

**Columns**:

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| geo_key | INTEGER | Surrogate key | Auto-increment |
| region | VARCHAR(50) | Geographic region | North America |
| country | VARCHAR(100) | Country name | United States |
| country_code | VARCHAR(3) | ISO 3166-1 alpha-3 | USA |
| state_province | VARCHAR(100) | State or province | California |
| state_code | VARCHAR(10) | State abbreviation | CA |
| city | VARCHAR(100) | City name | San Francisco |
| postal_code | VARCHAR(20) | ZIP/postal code | 94102 |
| latitude | DECIMAL(9,6) | Latitude coordinate | 37.774929 |
| longitude | DECIMAL(9,6) | Longitude coordinate | -122.419418 |
| timezone | VARCHAR(50) | Time zone | America/Los_Angeles |
| population_tier | VARCHAR(20) | City size category | Large (500K+) |

**Hierarchies**:
- Geographic Hierarchy: Region → Country → State/Province → City

**Sample Data Distribution**:
- Regions: 6 (North America, Europe, Asia, South America, Africa, Oceania)
- Countries: ~50 countries
- States/Provinces: ~200 administrative divisions
- Cities: ~5,000 cities

**Sample Query**:
```sql
SELECT g.region, g.country, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_geography g ON f.geo_key = g.geo_key
WHERE g.region = 'North America'
GROUP BY g.region, g.country
ORDER BY total_revenue DESC;
```

### dim_product

**Description**: Product catalog with category hierarchy

**Type**: Slowly Changing Dimension Type 2 (track historical product changes)

**Rows**: ~10,000 current products + historical records (~15,000 total with SCD)

**Slowly Changing Dimension**: Type 2 (preserve history for price/category changes)

**Columns**:

| Column Name | Data Type | Description | Example Values | SCD Type 2 |
|-------------|-----------|-------------|----------------|------------|
| product_key | INTEGER | Surrogate key | Auto-increment | Surrogate |
| product_id | VARCHAR(50) | Natural product ID | PROD-12345 | Business key |
| product_name | VARCHAR(200) | Product name | Wireless Bluetooth Headphones | Attribute |
| product_sku | VARCHAR(50) | Stock keeping unit | SKU-WBH-001 | Attribute |
| category | VARCHAR(100) | Top-level category | Electronics | Attribute |
| subcategory | VARCHAR(100) | Product subcategory | Audio | Attribute |
| brand | VARCHAR(100) | Brand name | TechSound | Attribute |
| supplier | VARCHAR(100) | Supplier name | AudioSupplier Inc | Attribute |
| unit_cost | DECIMAL(10,2) | Cost to company | 45.00 | Attribute |
| list_price | DECIMAL(10,2) | Standard retail price | 79.99 | Attribute |
| is_active | BOOLEAN | Currently available | TRUE | Status flag |
| effective_date | DATE | Version effective date | 2023-01-01 | SCD start |
| expiration_date | DATE | Version expiration date | 2999-12-31 (current) / 2023-06-30 (expired) | SCD end |
| is_current | BOOLEAN | Current version flag | TRUE | SCD flag |

**SCD Type 2 Example**:
```
Product price change creates new version:

product_key | product_id | product_name | list_price | effective_date | expiration_date | is_current
----------- | ---------- | ------------ | ---------- | -------------- | --------------- | ----------
5001        | PROD-12345 | Headphones   | 79.99      | 2023-01-01     | 2023-06-30      | FALSE
5002        | PROD-12345 | Headphones   | 69.99      | 2023-07-01     | 2999-12-31      | TRUE
```

**Category Distribution**:
- Categories: ~20 (Electronics, Clothing, Home & Garden, Sports, Books, etc.)
- Subcategories: ~100
- Brands: ~500
- Products per category: Realistic Pareto distribution (80/20 rule)

**Sample Query (Point-in-Time)**:
```sql
SELECT p.category, COUNT(*) as product_count, AVG(p.list_price) as avg_price
FROM dim_product p
WHERE p.is_current = TRUE
GROUP BY p.category
ORDER BY product_count DESC;
```

### dim_customer

**Description**: Customer attributes for cohort and segment analysis

**Type**: Conformed dimension

**Rows**: ~1,000,000 customers

**Slowly Changing Dimension**: Type 1 (overwrite for most attributes, e.g., segment updates)

**Columns**:

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| customer_key | INTEGER | Surrogate key | Auto-increment |
| customer_id | VARCHAR(50) | Natural customer ID | CUST-789456 |
| customer_segment | VARCHAR(50) | Customer segmentation | Premium, Standard, Budget |
| acquisition_channel | VARCHAR(50) | How customer acquired | Online, Retail, Partner, Referral |
| customer_lifetime_value_tier | VARCHAR(20) | CLV classification | High (>$10K), Medium ($1K-$10K), Low (<$1K) |
| signup_date | DATE | Account creation date | 2022-03-15 |
| country_code | VARCHAR(3) | Customer country | USA, GBR, CAN |
| is_business_customer | BOOLEAN | B2B vs B2C | FALSE |
| preferred_contact_method | VARCHAR(20) | Contact preference | Email, SMS, Phone |

**Segment Distribution**:
- Premium: 10% of customers, 50% of revenue (high LTV)
- Standard: 60% of customers, 40% of revenue
- Budget: 30% of customers, 10% of revenue

**Acquisition Channel Distribution**:
- Online: 60%
- Retail: 25%
- Partner: 10%
- Referral: 5%

**Sample Query**:
```sql
SELECT c.customer_segment, COUNT(DISTINCT f.customer_key) as customer_count, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;
```

### dim_payment

**Description**: Payment method dimension for payment analysis

**Type**: Conformed dimension

**Rows**: ~20 payment methods

**Slowly Changing Dimension**: Type 1 (static, payment methods rarely change)

**Columns**:

| Column Name | Data Type | Description | Example Values |
|-------------|-----------|-------------|----------------|
| payment_key | INTEGER | Surrogate key | Auto-increment |
| payment_method_id | VARCHAR(50) | Natural payment ID | PM-001 |
| payment_type | VARCHAR(50) | Payment type | Credit Card, Debit Card, PayPal, etc. |
| payment_provider | VARCHAR(100) | Payment processor | Visa, Mastercard, PayPal, Stripe |
| processing_fee_percent | DECIMAL(5,4) | Fee percentage | 0.0290 (2.9%) |
| is_instant | BOOLEAN | Immediate settlement | TRUE |
| requires_verification | BOOLEAN | Needs additional auth | FALSE |

**Payment Types**:
- Credit Card: 50%
- Debit Card: 25%
- Digital Wallet (PayPal, Apple Pay): 15%
- Bank Transfer: 8%
- Buy Now Pay Later: 2%

**Sample Query**:
```sql
SELECT pm.payment_type, COUNT(*) as transaction_count, SUM(f.revenue) as total_revenue
FROM sales_fact f
JOIN dim_payment pm ON f.payment_key = pm.payment_key
GROUP BY pm.payment_type
ORDER BY total_revenue DESC;
```

## Data Generation Specifications

### Volume Targets

| Dataset Size | sales_fact Rows | Target Use Case |
|--------------|-----------------|-----------------|
| Small | 10M | Quick tests, development |
| Medium | 50M | Standard benchmarks |
| Large | 100M | Primary demo dataset |
| Extra Large | 200M | Scalability validation |

### Data Distribution Rules

**Temporal Distribution**:
- Uniform distribution across 3 years (2021-2023)
- Slight seasonality (20% higher Q4 due to holidays)

**Product Distribution**:
- Pareto: Top 20% of products generate 80% of sales
- Long tail: Many products with few sales

**Geographic Distribution**:
- North America: 50%
- Europe: 30%
- Asia: 15%
- Other: 5%

**Customer Distribution**:
- Repeat customers: 70% of transactions
- New customers: 30% of transactions
- Premium customers: 10% of customers, 50% of revenue

### Data Quality Rules

**Referential Integrity**:
- All foreign keys MUST reference valid dimension records
- Orphaned fact records NOT allowed

**Business Rules**:
- revenue = quantity × unit_price (enforced calculation)
- profit = revenue - cost (enforced calculation)
- cost < revenue (business constraint, except loss leaders <1%)
- discount_amount <= revenue

**Data Validity**:
- Dates within 2021-2023 range
- Quantities 1-100 per line item
- Prices > 0.00
- Timestamps in millisecond precision

**Deterministic Generation**:
- Fixed random seed (SEED=42) for reproducibility
- Identical seed produces identical dataset
- Critical for benchmark consistency

## Query Patterns and Indexing

### Partition Pruning Queries

**Time-based filtering** (leverage year/quarter partitions):
```sql
-- Query scans only year=2023 partitions
SELECT SUM(revenue) FROM sales_fact WHERE transaction_date BETWEEN '2023-01-01' AND '2023-12-31';

-- Query scans only year=2023/quarter=Q2 partition
SELECT SUM(revenue) FROM sales_fact WHERE transaction_date BETWEEN '2023-04-01' AND '2023-06-30';
```

**Expected Partition Pruning**:
- No filter: Scan all 12-16 partitions
- Year filter: Scan 4 partitions (one year)
- Quarter filter: Scan 1 partition (80%+ pruning)

### Columnar Selectivity Queries

**Selective column queries** (demonstrate columnar I/O efficiency):
```sql
-- Reads only 3 columns (revenue, quantity, transaction_date) from ~20 total
SELECT transaction_date, SUM(revenue), SUM(quantity)
FROM sales_fact
GROUP BY transaction_date;

-- Columnar storage reads <20% of data vs row storage (all columns)
```

## Validation Queries

### Data Integrity Checks

```sql
-- Check referential integrity (should return 0)
SELECT COUNT(*) FROM sales_fact f
LEFT JOIN dim_product p ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- Check calculated fields (should return 0 mismatches)
SELECT COUNT(*) FROM sales_fact
WHERE ABS(revenue - (quantity * unit_price)) > 0.01;

-- Check business rules (should return 0 violations)
SELECT COUNT(*) FROM sales_fact WHERE cost > revenue;
```

### Cardinality Checks

```sql
-- Verify distinct counts match expectations
SELECT
    COUNT(DISTINCT transaction_id) as unique_transactions,
    COUNT(DISTINCT product_key) as unique_products,
    COUNT(DISTINCT customer_key) as unique_customers
FROM sales_fact;
```

## Storage Estimates

### Parquet Storage (Compressed)

| Dataset Size | Rows | Estimated Size | Compression Ratio |
|--------------|------|----------------|-------------------|
| 10M | 10,000,000 | 200-400 MB | 5-7x |
| 50M | 50,000,000 | 1-2 GB | 5-7x |
| 100M | 100,000,000 | 2-4 GB | 5-7x |
| 200M | 200,000,000 | 4-8 GB | 5-7x |

### CSV Storage (Uncompressed, Comparison Baseline)

| Dataset Size | Rows | Estimated Size | vs Parquet |
|--------------|------|----------------|------------|
| 100M | 100,000,000 | 15-20 GB | 5-7x larger |

## SCD Type 2 Example Scenarios

### Product Price Change

```
Timeline:
- 2023-01-01: Product "Headphones" launches at $79.99
- 2023-07-01: Price reduced to $69.99 (promotion)

dim_product records:
product_key=5001, product_id=PROD-123, price=79.99, effective=2023-01-01, expiration=2023-06-30, current=FALSE
product_key=5002, product_id=PROD-123, price=69.99, effective=2023-07-01, expiration=2999-12-31, current=TRUE

Point-in-time query (2023-05-15):
SELECT * FROM dim_product WHERE product_id='PROD-123' AND '2023-05-15' BETWEEN effective_date AND expiration_date
Returns: price=79.99 (historical correct price)
```

## References

- Star Schema Design: Kimball, "The Data Warehouse Toolkit"
- Parquet Format: https://parquet.apache.org/docs/file-format/
- DuckDB Partitioning: https://duckdb.org/docs/data/partitioning/hive_partitioning
- Slowly Changing Dimensions: Kimball SCD Types
