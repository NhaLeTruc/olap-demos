"""Unit tests for data generators.

Tests the data generation functions to ensure correct schema,
data types, and referential integrity.
"""

import pytest
from datetime import date, timedelta

from src.datagen.generator import (
    generate_dim_time,
    generate_dim_geography,
    generate_dim_product,
    generate_dim_customer,
    generate_dim_payment,
    generate_sales_fact,
)
from src.datagen.schemas import (
    validate_dimension_unique_keys,
    validate_scd_type2,
    validate_time_dimension,
    validate_fact_measures,
)


@pytest.mark.unit
class TestDimensionGenerators:
    """Test suite for dimension table generators."""

    def test_generate_dim_time_schema(self, sample_dim_time):
        """Test time dimension has correct schema."""
        expected_columns = [
            'time_key', 'date', 'year', 'quarter', 'month', 'month_name',
            'week', 'day_of_month', 'day_of_week', 'day_name', 'is_weekend',
            'is_holiday', 'fiscal_year', 'fiscal_quarter', 'fiscal_period'
        ]

        assert set(sample_dim_time.columns) == set(expected_columns)

    def test_generate_dim_time_unique_keys(self, sample_dim_time):
        """Test time dimension has unique keys."""
        validate_dimension_unique_keys(sample_dim_time, 'time_key')

    def test_generate_dim_time_validation(self, sample_dim_time):
        """Test time dimension passes validation."""
        assert validate_time_dimension(sample_dim_time)

    def test_generate_dim_time_date_range(self, test_seed):
        """Test time dimension generates correct date range."""
        start = date(2023, 1, 1)
        end = date(2023, 12, 31)

        dim_time = generate_dim_time(start, end, test_seed)

        assert dim_time['date'].min() == start
        assert dim_time['date'].max() == end
        assert len(dim_time) == 365  # Non-leap year

    def test_generate_dim_geography_schema(self, sample_dim_geography):
        """Test geography dimension has correct schema."""
        expected_columns = [
            'geo_key', 'city', 'region', 'country', 'country_code',
            'latitude', 'longitude', 'population_segment', 'timezone'
        ]

        assert set(sample_dim_geography.columns) == set(expected_columns)

    def test_generate_dim_geography_unique_keys(self, sample_dim_geography):
        """Test geography dimension has unique keys."""
        validate_dimension_unique_keys(sample_dim_geography, 'geo_key')

    def test_generate_dim_product_schema(self, sample_dim_product):
        """Test product dimension has correct schema."""
        expected_columns = [
            'product_key', 'product_id', 'product_name', 'category',
            'subcategory', 'brand', 'unit_cost', 'unit_price',
            'effective_date', 'expiration_date', 'is_current'
        ]

        assert set(sample_dim_product.columns) == set(expected_columns)

    def test_generate_dim_product_scd_type2(self, sample_dim_product):
        """Test product dimension SCD Type 2 validation."""
        assert validate_scd_type2(sample_dim_product)

    def test_generate_dim_product_current_records(self, sample_dim_product):
        """Test product dimension has current records for all products."""
        current_products = sample_dim_product[sample_dim_product['is_current'] == True]
        unique_products = sample_dim_product['product_id'].nunique()

        # Each product should have exactly one current record
        assert len(current_products) == unique_products

    def test_generate_dim_customer_schema(self, sample_dim_customer):
        """Test customer dimension has correct schema."""
        expected_columns = [
            'customer_key', 'customer_id', 'first_name', 'last_name',
            'email', 'phone', 'date_of_birth', 'gender', 'income_segment',
            'customer_segment', 'registration_date', 'is_active'
        ]

        assert set(sample_dim_customer.columns) == set(expected_columns)

    def test_generate_dim_customer_unique_keys(self, sample_dim_customer):
        """Test customer dimension has unique keys."""
        validate_dimension_unique_keys(sample_dim_customer, 'customer_key')

    def test_generate_dim_payment_schema(self, sample_dim_payment):
        """Test payment dimension has correct schema."""
        expected_columns = [
            'payment_key', 'payment_method', 'payment_type',
            'processing_fee_pct', 'is_digital'
        ]

        assert set(sample_dim_payment.columns) == set(expected_columns)

    def test_generate_dim_payment_fixed_count(self, sample_dim_payment):
        """Test payment dimension has expected number of methods."""
        assert len(sample_dim_payment) == 7  # Fixed set of payment methods


@pytest.mark.unit
class TestFactGenerator:
    """Test suite for fact table generator."""

    def test_generate_sales_fact_schema(self, sample_fact_sales):
        """Test sales fact has correct schema."""
        expected_columns = [
            'transaction_id', 'line_item_id', 'transaction_date',
            'transaction_timestamp', 'time_key', 'geo_key', 'product_key',
            'customer_key', 'payment_key', 'quantity', 'unit_price',
            'revenue', 'cost', 'discount_amount', 'profit'
        ]

        assert set(sample_fact_sales.columns) == set(expected_columns)

    def test_generate_sales_fact_measures(self, sample_fact_sales):
        """Test sales fact measure calculations are correct."""
        assert validate_fact_measures(sample_fact_sales)

    def test_generate_sales_fact_foreign_keys(
        self,
        sample_fact_sales,
        sample_dim_time,
        sample_dim_geography,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_payment
    ):
        """Test sales fact has valid foreign keys."""
        # Check all foreign keys exist in dimensions
        assert sample_fact_sales['time_key'].isin(sample_dim_time['time_key']).all()
        assert sample_fact_sales['geo_key'].isin(sample_dim_geography['geo_key']).all()
        assert sample_fact_sales['product_key'].isin(sample_dim_product['product_key']).all()
        assert sample_fact_sales['customer_key'].isin(sample_dim_customer['customer_key']).all()
        assert sample_fact_sales['payment_key'].isin(sample_dim_payment['payment_key']).all()

    def test_generate_sales_fact_positive_measures(self, sample_fact_sales):
        """Test sales fact has positive measures (except profit)."""
        assert (sample_fact_sales['quantity'] > 0).all()
        assert (sample_fact_sales['unit_price'] > 0).all()
        assert (sample_fact_sales['revenue'] >= 0).all()
        assert (sample_fact_sales['cost'] >= 0).all()
        assert (sample_fact_sales['discount_amount'] >= 0).all()


@pytest.mark.unit
class TestDataDeterminism:
    """Test that data generation is deterministic with fixed seed."""

    def test_time_dimension_deterministic(self, test_seed):
        """Test time dimension generates same data with same seed."""
        start = date(2023, 1, 1)
        end = date(2023, 1, 31)

        dim1 = generate_dim_time(start, end, test_seed)
        dim2 = generate_dim_time(start, end, test_seed)

        assert dim1.equals(dim2)

    def test_product_dimension_deterministic(self, test_seed):
        """Test product dimension generates same data with same seed."""
        dim1 = generate_dim_product(num_products=10, seed=test_seed)
        dim2 = generate_dim_product(num_products=10, seed=test_seed)

        assert dim1.equals(dim2)
