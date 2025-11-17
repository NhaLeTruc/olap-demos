"""Fact table schema for the OLAP star schema.

The fact table contains the measurable business events (sales transactions)
with foreign keys to dimension tables.
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class SalesFact:
    """Sales transaction fact table.

    Type: Transaction fact table (immutable after insert)
    Grain: One row per product sold per transaction (line item level)
    Size: 100M-200M rows (configurable for benchmarking)
    Partitioning: Hive-style by year/quarter (e.g., year=2023/quarter=Q1/)

    Measures:
    - Additive: quantity, revenue, cost, discount_amount, profit
    - Semi-additive: unit_price (average, not sum)

    Foreign Keys:
    - time_key → dim_time.time_key
    - geo_key → dim_geography.geo_key
    - product_key → dim_product.product_key
    - customer_key → dim_customer.customer_key
    - payment_key → dim_payment.payment_key
    """

    # Primary key components
    transaction_id: int  # Unique transaction identifier
    line_item_id: int  # Line item number within transaction (1, 2, 3...)

    # Time attributes (for partitioning and queries)
    transaction_date: date  # Date of transaction (YYYY-MM-DD)
    transaction_timestamp: datetime  # Precise timestamp (millisecond precision)

    # Foreign keys to dimensions
    time_key: int  # FK to dim_time (YYYYMMDD format)
    geo_key: int  # FK to dim_geography
    product_key: int  # FK to dim_product (specific version for SCD Type 2)
    customer_key: int  # FK to dim_customer
    payment_key: int  # FK to dim_payment

    # Measures (facts)
    quantity: int  # Number of units purchased (1-100)
    unit_price: float  # Price per unit in USD
    revenue: float  # Total revenue (quantity × unit_price)
    cost: float  # Total cost to company
    discount_amount: float  # Total discount applied
    profit: float  # Gross profit (revenue - cost)


def validate_sales_fact(record: SalesFact) -> bool:
    """Validate SalesFact record constraints and business rules.

    Business Rules:
    1. revenue = quantity × unit_price (calculated field)
    2. profit = revenue - cost (calculated field)
    3. cost < revenue (except rare loss leaders <1%)
    4. discount_amount <= revenue
    5. All measures >= 0
    6. transaction_date within 2021-2023 range
    7. quantity between 1 and 100
    """
    # Primary key constraints
    assert record.transaction_id > 0, "Transaction ID must be positive"
    assert record.line_item_id > 0, "Line item ID must be positive"

    # Time constraints
    assert 2021 <= record.transaction_date.year <= 2023, \
        f"Transaction date must be 2021-2023, got {record.transaction_date.year}"
    assert record.transaction_timestamp.date() == record.transaction_date, \
        "Transaction timestamp date must match transaction_date"

    # Foreign key constraints (non-null)
    assert record.time_key > 0, "time_key must be positive"
    assert record.geo_key > 0, "geo_key must be positive"
    assert record.product_key > 0, "product_key must be positive"
    assert record.customer_key > 0, "customer_key must be positive"
    assert record.payment_key > 0, "payment_key must be positive"

    # Measure constraints
    assert 1 <= record.quantity <= 100, f"Quantity must be 1-100, got {record.quantity}"
    assert record.unit_price > 0, f"Unit price must be positive, got {record.unit_price}"
    assert record.revenue >= 0, f"Revenue must be non-negative, got {record.revenue}"
    assert record.cost >= 0, f"Cost must be non-negative, got {record.cost}"
    assert record.discount_amount >= 0, f"Discount must be non-negative, got {record.discount_amount}"

    # Business rule validations
    expected_revenue = round(record.quantity * record.unit_price, 2)
    assert abs(record.revenue - expected_revenue) < 0.01, \
        f"Revenue mismatch: {record.revenue} != quantity({record.quantity}) × unit_price({record.unit_price})"

    expected_profit = round(record.revenue - record.cost, 2)
    assert abs(record.profit - expected_profit) < 0.01, \
        f"Profit mismatch: {record.profit} != revenue({record.revenue}) - cost({record.cost})"

    # Allow loss leaders but they should be rare (<1%)
    if record.cost > record.revenue:
        # Loss leader - acceptable but should be logged for review
        pass

    assert record.discount_amount <= record.revenue, \
        f"Discount {record.discount_amount} cannot exceed revenue {record.revenue}"

    return True


def calculate_derived_measures(
    quantity: int,
    unit_price: float,
    cost: float,
    discount_amount: float = 0.0
) -> tuple[float, float]:
    """Calculate derived measures for a sales fact record.

    Args:
        quantity: Number of units purchased
        unit_price: Price per unit
        cost: Total cost to company
        discount_amount: Total discount applied

    Returns:
        Tuple of (revenue, profit)
    """
    revenue = round(quantity * unit_price, 2)
    profit = round(revenue - cost, 2)
    return revenue, profit


# Partition key extraction helpers

def extract_partition_keys(transaction_date: date) -> dict[str, str]:
    """Extract Hive-style partition keys from transaction date.

    Args:
        transaction_date: Transaction date

    Returns:
        Dictionary with partition keys: {"year": "2023", "quarter": "Q1"}

    Example:
        >>> extract_partition_keys(date(2023, 2, 15))
        {"year": "2023", "quarter": "Q1"}
    """
    year = transaction_date.year
    quarter_num = (transaction_date.month - 1) // 3 + 1
    quarter = f"Q{quarter_num}"

    return {
        "year": str(year),
        "quarter": quarter,
    }


def partition_path(transaction_date: date, base_path: str = "data/parquet/sales_fact") -> str:
    """Generate Hive-style partition path for a transaction date.

    Args:
        transaction_date: Transaction date
        base_path: Base directory path

    Returns:
        Full partition path with Hive-style keys

    Example:
        >>> partition_path(date(2023, 2, 15))
        'data/parquet/sales_fact/year=2023/quarter=Q1'
    """
    keys = extract_partition_keys(transaction_date)
    return f"{base_path}/year={keys['year']}/quarter={keys['quarter']}"
