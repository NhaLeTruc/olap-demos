"""Data generation module for synthetic OLAP datasets.

This module provides utilities for generating realistic e-commerce sales data
with deterministic random seeds for reproducibility.
"""

from .generator import (
    DataGenerator,
    generate_dim_time,
    generate_dim_geography,
    generate_dim_product,
    generate_dim_customer,
    generate_dim_payment,
    generate_sales_fact,
)
from .schemas import (
    validate_schema,
    check_referential_integrity,
)

__all__ = [
    "DataGenerator",
    "generate_dim_time",
    "generate_dim_geography",
    "generate_dim_product",
    "generate_dim_customer",
    "generate_dim_payment",
    "generate_sales_fact",
    "validate_schema",
    "check_referential_integrity",
]
