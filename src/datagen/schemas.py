"""Schema validation and integrity checking utilities.

This module provides validation functions to ensure data quality and referential
integrity across dimension and fact tables.
"""

from typing import List, Dict, Any, Set
import pandas as pd
from datetime import date


def validate_schema(df: pd.DataFrame, expected_columns: List[str]) -> bool:
    """Validate that DataFrame has all expected columns.

    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names

    Returns:
        True if all columns present, False otherwise

    Raises:
        ValueError: If any expected columns are missing
    """
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")
    return True


def check_referential_integrity(
    fact_df: pd.DataFrame,
    dimension_dfs: Dict[str, pd.DataFrame],
    foreign_keys: Dict[str, str]
) -> Dict[str, Any]:
    """Check referential integrity between fact and dimension tables.

    Args:
        fact_df: Fact table DataFrame
        dimension_dfs: Dictionary mapping dimension names to DataFrames
        foreign_keys: Dictionary mapping fact FK columns to dimension PK columns
            Example: {"time_key": "time_key", "geo_key": "geo_key"}

    Returns:
        Dictionary with integrity check results:
        {
            "valid": bool,
            "orphan_counts": {fk_column: count},
            "orphan_records": {fk_column: [orphan_values]}
        }
    """
    results = {
        "valid": True,
        "orphan_counts": {},
        "orphan_records": {}
    }

    for fk_column, pk_column in foreign_keys.items():
        # Determine dimension name from FK column (remove _key suffix)
        dim_name = fk_column.replace("_key", "")

        # Try to find matching dimension
        dimension_df = None
        for dim_key, dim_df in dimension_dfs.items():
            if dim_name in dim_key.lower():
                dimension_df = dim_df
                break

        if dimension_df is None:
            raise ValueError(f"No dimension found for foreign key {fk_column}")

        # Get valid dimension keys
        valid_keys = set(dimension_df[pk_column])

        # Get fact table foreign keys
        fact_keys = set(fact_df[fk_column])

        # Find orphan keys
        orphan_keys = fact_keys - valid_keys

        if orphan_keys:
            results["valid"] = False
            results["orphan_counts"][fk_column] = len(orphan_keys)
            results["orphan_records"][fk_column] = sorted(list(orphan_keys))[:10]  # First 10 orphans

    return results


def validate_dimension_unique_keys(df: pd.DataFrame, key_column: str) -> bool:
    """Validate that dimension table has unique keys.

    Args:
        df: Dimension DataFrame
        key_column: Name of the primary key column

    Returns:
        True if all keys are unique

    Raises:
        ValueError: If duplicate keys found
    """
    duplicates = df[df.duplicated(subset=[key_column], keep=False)]
    if not duplicates.empty:
        dup_keys = duplicates[key_column].unique()[:10]
        raise ValueError(f"Duplicate keys found in {key_column}: {dup_keys}")
    return True


def validate_scd_type2(df: pd.DataFrame) -> bool:
    """Validate SCD Type 2 temporal consistency.

    Checks:
    - Only one current record per natural key
    - No overlapping date ranges for same natural key
    - effective_date < expiration_date

    Args:
        df: DataFrame with SCD Type 2 columns (product_id, effective_date,
            expiration_date, is_current)

    Returns:
        True if valid

    Raises:
        ValueError: If validation fails
    """
    # Check date ordering
    invalid_dates = df[df['effective_date'] >= df['expiration_date']]
    if not invalid_dates.empty:
        raise ValueError(
            f"Found {len(invalid_dates)} records with effective_date >= expiration_date"
        )

    # Check only one current record per natural key
    current_records = df[df['is_current'] == True].groupby('product_id').size()
    multiple_current = current_records[current_records > 1]
    if not multiple_current.empty:
        raise ValueError(
            f"Found {len(multiple_current)} product_ids with multiple current records"
        )

    # Check for overlapping date ranges (same natural key)
    for product_id in df['product_id'].unique():
        product_records = df[df['product_id'] == product_id].sort_values('effective_date')

        for i in range(len(product_records) - 1):
            current_record = product_records.iloc[i]
            next_record = product_records.iloc[i + 1]

            # Current expiration should equal next effective
            if current_record['expiration_date'] > next_record['effective_date']:
                raise ValueError(
                    f"Overlapping date ranges for product_id {product_id}"
                )

    return True


def validate_time_dimension(df: pd.DataFrame) -> bool:
    """Validate time dimension specific constraints.

    Checks:
    - time_key matches YYYYMMDD format
    - All dates in range
    - No missing dates in sequence

    Args:
        df: Time dimension DataFrame

    Returns:
        True if valid

    Raises:
        ValueError: If validation fails
    """
    # Validate time_key format (YYYYMMDD)
    invalid_keys = df[
        (df['time_key'] < 19000101) |
        (df['time_key'] > 21001231)
    ]
    if not invalid_keys.empty:
        raise ValueError(f"Found {len(invalid_keys)} invalid time_key values")

    # Validate time_key matches date
    df['expected_key'] = df['date'].apply(
        lambda d: d.year * 10000 + d.month * 100 + d.day
    )
    mismatches = df[df['time_key'] != df['expected_key']]
    if not mismatches.empty:
        raise ValueError(
            f"Found {len(mismatches)} time_key values that don't match date"
        )

    return True


def validate_fact_measures(df: pd.DataFrame) -> bool:
    """Validate fact table calculated measures.

    Checks:
    - revenue = quantity × unit_price
    - profit = revenue - cost
    - All measures are non-negative (except profit)

    Args:
        df: Sales fact DataFrame

    Returns:
        True if valid

    Raises:
        ValueError: If validation fails
    """
    # Check revenue calculation
    df['expected_revenue'] = df['quantity'] * df['unit_price']
    revenue_errors = df[abs(df['revenue'] - df['expected_revenue']) > 0.01]
    if not revenue_errors.empty:
        raise ValueError(
            f"Found {len(revenue_errors)} records with incorrect revenue calculation"
        )

    # Check profit calculation
    df['expected_profit'] = df['revenue'] - df['cost']
    profit_errors = df[abs(df['profit'] - df['expected_profit']) > 0.01]
    if not profit_errors.empty:
        raise ValueError(
            f"Found {len(profit_errors)} records with incorrect profit calculation"
        )

    # Check non-negative constraints
    if (df['quantity'] < 0).any():
        raise ValueError("Found negative quantity values")

    if (df['unit_price'] < 0).any():
        raise ValueError("Found negative unit_price values")

    if (df['revenue'] < 0).any():
        raise ValueError("Found negative revenue values")

    if (df['cost'] < 0).any():
        raise ValueError("Found negative cost values")

    if (df['discount_amount'] < 0).any():
        raise ValueError("Found negative discount_amount values")

    return True
