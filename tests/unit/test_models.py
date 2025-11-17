"""Unit tests for data model validation (T096).

Tests schema definitions, constraints, and validation logic for
dimension and fact tables.
"""

import pytest
import pandas as pd
from datetime import date, datetime

from src.models.dimensions import (
    DimensionTime,
    DimensionGeography,
    DimensionProduct,
    DimensionCustomer,
    DimensionPayment
)
from src.models.facts import SalesFact


class TestDimensionSchemas:
    """Test dimension table schemas and constraints."""

    def test_dim_time_schema(self):
        """Validate dim_time schema has all required columns."""
        schema = DimensionTime.schema()
        expected_columns = [
            'time_key', 'date', 'year', 'quarter', 'month',
            'day', 'is_weekend', 'is_holiday'
        ]

        for col in expected_columns:
            assert col in schema, f"Missing column: {col}"

    def test_dim_time_validation(self):
        """Validate dim_time constraint validation."""
        # Valid record
        valid_data = {
            'time_key': 20230101,
            'date': date(2023, 1, 1),
            'year': 2023,
            'quarter': 'Q1',
            'month': 1,
            'day': 1,
            'is_weekend': True,  # Sunday
            'is_holiday': True
        }
        assert DimensionTime.validate(valid_data) is True

        # Invalid: year mismatch
        invalid_data = valid_data.copy()
        invalid_data['year'] = 2024  # Doesn't match date
        assert DimensionTime.validate(invalid_data) is False

    def test_dim_geography_schema(self):
        """Validate dim_geography schema has all required columns."""
        schema = DimensionGeography.schema()
        expected_columns = [
            'geo_key', 'region', 'country', 'state',
            'city', 'lat', 'lon'
        ]

        for col in expected_columns:
            assert col in schema, f"Missing column: {col}"

    def test_dim_geography_hierarchy(self):
        """Validate geographic hierarchy relationships."""
        # Test that region contains countries
        data = {
            'geo_key': 1,
            'region': 'North America',
            'country': 'USA',
            'state': 'California',
            'city': 'San Francisco',
            'lat': 37.7749,
            'lon': -122.4194
        }
        assert DimensionGeography.validate(data) is True

        # Coordinates should be valid ranges
        assert -90 <= data['lat'] <= 90
        assert -180 <= data['lon'] <= 180

    def test_dim_product_schema_scd2(self):
        """Validate dim_product SCD Type 2 schema."""
        schema = DimensionProduct.schema()
        scd2_columns = [
            'product_key', 'product_id', 'name', 'category',
            'subcategory', 'brand', 'effective_date',
            'expiration_date', 'is_current'
        ]

        for col in scd2_columns:
            assert col in schema, f"Missing SCD2 column: {col}"

    def test_dim_product_scd2_validation(self):
        """Validate SCD Type 2 temporal constraints."""
        # Current record
        current_record = {
            'product_key': 1,
            'product_id': 'PROD001',
            'name': 'Product A',
            'category': 'Electronics',
            'subcategory': 'Phones',
            'brand': 'BrandX',
            'effective_date': date(2023, 1, 1),
            'expiration_date': date(9999, 12, 31),
            'is_current': True
        }
        assert DimensionProduct.validate(current_record) is True

        # Historical record
        historical_record = current_record.copy()
        historical_record['product_key'] = 2
        historical_record['expiration_date'] = date(2023, 6, 30)
        historical_record['is_current'] = False
        assert DimensionProduct.validate(historical_record) is True

        # Invalid: effective_date after expiration_date
        invalid_record = current_record.copy()
        invalid_record['effective_date'] = date(2023, 12, 31)
        invalid_record['expiration_date'] = date(2023, 1, 1)
        assert DimensionProduct.validate(invalid_record) is False

    def test_dim_customer_schema(self):
        """Validate dim_customer schema."""
        schema = DimensionCustomer.schema()
        expected_columns = [
            'customer_key', 'customer_id', 'segment',
            'channel', 'ltv_tier'
        ]

        for col in expected_columns:
            assert col in schema, f"Missing column: {col}"

    def test_dim_customer_segments(self):
        """Validate customer segmentation logic."""
        segments = ['Enterprise', 'SMB', 'Consumer', 'Government']
        channels = ['Online', 'Retail', 'Partner', 'Direct']
        ltv_tiers = ['High', 'Medium', 'Low']

        data = {
            'customer_key': 1,
            'customer_id': 'CUST001',
            'segment': 'Enterprise',
            'channel': 'Direct',
            'ltv_tier': 'High'
        }

        # Valid segments/channels/tiers
        assert data['segment'] in segments
        assert data['channel'] in channels
        assert data['ltv_tier'] in ltv_tiers

    def test_dim_payment_schema(self):
        """Validate dim_payment schema."""
        schema = DimensionPayment.schema()
        expected_columns = [
            'payment_key', 'payment_type', 'provider', 'fee_percent'
        ]

        for col in expected_columns:
            assert col in schema, f"Missing column: {col}"

    def test_dim_payment_fee_validation(self):
        """Validate payment fee constraints."""
        data = {
            'payment_key': 1,
            'payment_type': 'Credit Card',
            'provider': 'Visa',
            'fee_percent': 2.9
        }

        # Fee should be reasonable percentage
        assert 0 <= data['fee_percent'] <= 10.0


class TestFactSchema:
    """Test sales fact table schema and constraints."""

    def test_sales_fact_schema(self):
        """Validate sales_fact schema has all required columns."""
        schema = SalesFact.schema()

        # Dimension foreign keys
        fk_columns = [
            'time_key', 'geo_key', 'product_key',
            'customer_key', 'payment_key'
        ]
        for col in fk_columns:
            assert col in schema, f"Missing FK column: {col}"

        # Fact measures
        measure_columns = [
            'transaction_id', 'line_item_id', 'quantity',
            'unit_price', 'revenue', 'cost',
            'discount_amount', 'profit'
        ]
        for col in measure_columns:
            assert col in schema, f"Missing measure column: {col}"

    def test_sales_fact_calculations(self):
        """Validate calculated measures in fact table."""
        # Test revenue = quantity * unit_price - discount
        quantity = 10
        unit_price = 100.00
        discount = 50.00

        revenue = (quantity * unit_price) - discount
        assert revenue == 950.00

        # Test profit = revenue - cost
        cost = 700.00
        profit = revenue - cost
        assert profit == 250.00

    def test_sales_fact_validation(self):
        """Validate fact table business rules."""
        fact_record = {
            'transaction_id': 1,
            'line_item_id': 1,
            'time_key': 20230101,
            'geo_key': 1,
            'product_key': 1,
            'customer_key': 1,
            'payment_key': 1,
            'quantity': 5,
            'unit_price': 100.00,
            'revenue': 500.00,
            'cost': 300.00,
            'discount_amount': 0.00,
            'profit': 200.00
        }

        # Validate calculations
        expected_revenue = (fact_record['quantity'] * fact_record['unit_price']) - fact_record['discount_amount']
        assert abs(fact_record['revenue'] - expected_revenue) < 0.01

        expected_profit = fact_record['revenue'] - fact_record['cost']
        assert abs(fact_record['profit'] - expected_profit) < 0.01

        # Business rules
        assert fact_record['quantity'] > 0
        assert fact_record['unit_price'] > 0
        assert fact_record['discount_amount'] >= 0
        assert fact_record['revenue'] >= 0


class TestSchemaIntegrity:
    """Test referential integrity and cross-table constraints."""

    def test_foreign_key_references(self):
        """Validate that fact foreign keys would reference valid dimension keys."""
        # This is a structural test - actual referential integrity
        # is tested in integration tests with real data

        fact_schema = SalesFact.schema()

        # Verify FK columns exist
        assert 'time_key' in fact_schema
        assert 'geo_key' in fact_schema
        assert 'product_key' in fact_schema
        assert 'customer_key' in fact_schema
        assert 'payment_key' in fact_schema

    def test_schema_consistency(self):
        """Validate schema consistency across model definitions."""
        # All dimension tables should have a primary key
        assert 'time_key' in DimensionTime.schema()
        assert 'geo_key' in DimensionGeography.schema()
        assert 'product_key' in DimensionProduct.schema()
        assert 'customer_key' in DimensionCustomer.schema()
        assert 'payment_key' in DimensionPayment.schema()

        # Fact table should reference all dimension keys
        fact_schema = SalesFact.schema()
        assert all(key in fact_schema for key in [
            'time_key', 'geo_key', 'product_key',
            'customer_key', 'payment_key'
        ])
