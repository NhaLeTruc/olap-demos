"""Data models for OLAP star schema.

This module defines the dimensional model schemas including:
- Fact table: sales_fact
- Dimension tables: time, geography, product, customer, payment
"""

from .dimensions import (
    DimTime,
    DimGeography,
    DimProduct,
    DimCustomer,
    DimPayment,
)
from .facts import SalesFact

__all__ = [
    "DimTime",
    "DimGeography",
    "DimProduct",
    "DimCustomer",
    "DimPayment",
    "SalesFact",
]
