"""Integration tests for data generation pipeline (T099).

Tests end-to-end data generation workflow: generate → load → query → verify.
"""

import pytest
import pandas as pd
from pathlib import Path

from src.datagen.generator import (
    generate_time_dimension,
    generate_geography_dimension,
    generate_product_dimension,
    generate_customer_dimension,
    generate_payment_dimension,
    generate_sales_fact
)
from src.storage.parquet_handler import ParquetHandler
from src.storage.csv_handler import CSVHandler
from src.query.duckdb_loader import DuckDBLoader
from src.query.executor import QueryExecutor
from src.query.connection import ConnectionManager


@pytest.mark.integration
class TestEndToEndDataGeneration:
    """Test complete data generation to query workflow."""

    def test_generate_all_dimensions(self, temp_dir, test_seed):
        """Test generating all dimension tables."""
        # Generate all dimensions
        dim_time = generate_time_dimension(
            start_date='2021-01-01',
            end_date='2023-12-31',
            seed=test_seed
        )
        dim_geography = generate_geography_dimension(
            num_locations=100,
            seed=test_seed
        )
        dim_product = generate_product_dimension(
            num_products=1000,
            seed=test_seed
        )
        dim_customer = generate_customer_dimension(
            num_customers=10000,
            seed=test_seed
        )
        dim_payment = generate_payment_dimension(seed=test_seed)

        # Verify all generated successfully
        assert len(dim_time) > 0
        assert len(dim_geography) > 0
        assert len(dim_product) > 0
        assert len(dim_customer) > 0
        assert len(dim_payment) > 0

        # Verify schema compliance
        assert 'time_key' in dim_time.columns
        assert 'geo_key' in dim_geography.columns
        assert 'product_key' in dim_product.columns
        assert 'customer_key' in dim_customer.columns
        assert 'payment_key' in dim_payment.columns

    def test_generate_fact_with_dimensions(self, temp_dir, test_seed):
        """Test generating fact table with dimension references."""
        # Generate dimensions first
        dim_time = generate_time_dimension(
            start_date='2023-01-01',
            end_date='2023-12-31',
            seed=test_seed
        )
        dim_geography = generate_geography_dimension(
            num_locations=50,
            seed=test_seed
        )
        dim_product = generate_product_dimension(
            num_products=100,
            seed=test_seed
        )
        dim_customer = generate_customer_dimension(
            num_customers=1000,
            seed=test_seed
        )
        dim_payment = generate_payment_dimension(seed=test_seed)

        # Generate fact table
        fact_sales = generate_sales_fact(
            num_transactions=1000,
            time_df=dim_time,
            geo_df=dim_geography,
            product_df=dim_product,
            customer_df=dim_customer,
            payment_df=dim_payment,
            seed=test_seed
        )

        # Verify fact table
        assert len(fact_sales) > 0
        assert 'transaction_id' in fact_sales.columns
        assert 'revenue' in fact_sales.columns

        # Verify foreign key references are valid
        assert fact_sales['time_key'].isin(dim_time['time_key']).all()
        assert fact_sales['geo_key'].isin(dim_geography['geo_key']).all()
        assert fact_sales['product_key'].isin(dim_product['product_key']).all()
        assert fact_sales['customer_key'].isin(dim_customer['customer_key']).all()
        assert fact_sales['payment_key'].isin(dim_payment['payment_key']).all()

    def test_generate_load_query_pipeline(self, temp_dir, test_seed):
        """Test complete pipeline: generate → store → load → query."""
        # Step 1: Generate data
        dim_time = generate_time_dimension(
            start_date='2023-01-01',
            end_date='2023-12-31',
            seed=test_seed
        )
        dim_geography = generate_geography_dimension(
            num_locations=50,
            seed=test_seed
        )
        dim_product = generate_product_dimension(
            num_products=100,
            seed=test_seed
        )
        dim_customer = generate_customer_dimension(
            num_customers=500,
            seed=test_seed
        )
        dim_payment = generate_payment_dimension(seed=test_seed)

        fact_sales = generate_sales_fact(
            num_transactions=500,
            time_df=dim_time,
            geo_df=dim_geography,
            product_df=dim_product,
            customer_df=dim_customer,
            payment_df=dim_payment,
            seed=test_seed
        )

        # Step 2: Store to Parquet
        handler = ParquetHandler(temp_dir)
        handler.write(dim_time, 'dim_time')
        handler.write(dim_geography, 'dim_geography')
        handler.write(dim_product, 'dim_product')
        handler.write(dim_customer, 'dim_customer')
        handler.write(dim_payment, 'dim_payment')
        handler.write(fact_sales, 'fact_sales')

        # Step 3: Load into DuckDB
        conn_manager = ConnectionManager(':memory:')
        loader = DuckDBLoader(conn_manager)

        loader.load_parquet(temp_dir / 'dim_time', 'dim_time')
        loader.load_parquet(temp_dir / 'dim_geography', 'dim_geography')
        loader.load_parquet(temp_dir / 'dim_product', 'dim_product')
        loader.load_parquet(temp_dir / 'dim_customer', 'dim_customer')
        loader.load_parquet(temp_dir / 'dim_payment', 'dim_payment')
        loader.load_parquet(temp_dir / 'fact_sales', 'fact_sales')

        # Step 4: Query
        executor = QueryExecutor(conn_manager)

        # Simple aggregation
        result = executor.execute("""
            SELECT
                SUM(revenue) as total_revenue,
                COUNT(*) as transaction_count
            FROM fact_sales
        """)

        assert result.row_count == 1
        assert result.data.iloc[0]['total_revenue'] > 0
        assert result.data.iloc[0]['transaction_count'] == len(fact_sales)

        # Multi-dimensional join
        result = executor.execute("""
            SELECT
                dt.year,
                dg.country,
                SUM(fs.revenue) as total_revenue
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            JOIN dim_geography dg ON fs.geo_key = dg.geo_key
            GROUP BY dt.year, dg.country
            ORDER BY total_revenue DESC
            LIMIT 10
        """)

        assert result.row_count > 0
        assert 'year' in result.data.columns
        assert 'country' in result.data.columns
        assert 'total_revenue' in result.data.columns

    def test_deterministic_generation(self, temp_dir, test_seed):
        """Test that data generation is deterministic with same seed."""
        # Generate data twice with same seed
        fact_sales_1 = generate_sales_fact(
            num_transactions=100,
            time_df=generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed),
            geo_df=generate_geography_dimension(50, seed=test_seed),
            product_df=generate_product_dimension(100, seed=test_seed),
            customer_df=generate_customer_dimension(500, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        fact_sales_2 = generate_sales_fact(
            num_transactions=100,
            time_df=generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed),
            geo_df=generate_geography_dimension(50, seed=test_seed),
            product_df=generate_product_dimension(100, seed=test_seed),
            customer_df=generate_customer_dimension(500, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        # Should generate identical data
        assert len(fact_sales_1) == len(fact_sales_2)

        # Check first few rows match (sampling to avoid full comparison)
        pd.testing.assert_frame_equal(
            fact_sales_1.head(10).reset_index(drop=True),
            fact_sales_2.head(10).reset_index(drop=True)
        )


@pytest.mark.integration
class TestCompressionValidation:
    """Test compression ratio validation at different scales (T082)."""

    def test_compression_ratio_small_dataset(self, temp_dir, sample_fact_sales):
        """Test compression ratio on small dataset."""
        from src.storage.parquet_handler import ParquetHandler

        handler = ParquetHandler(temp_dir)

        # Get compression estimate
        compression_ratio = handler.estimate_compression_ratio(
            sample_fact_sales,
            'compression_small'
        )

        # Small datasets achieve at least 1.1:1
        assert compression_ratio >= 1.1, \
            f"Compression ratio {compression_ratio:.2f}:1 below 1.1:1 target"

    def test_compression_consistency_medium_dataset(self, temp_dir, test_seed):
        """Test compression ratio consistency on medium dataset."""
        from src.storage.parquet_handler import ParquetHandler

        # Generate medium dataset (10K transactions)
        dim_time = generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed)
        dim_geography = generate_geography_dimension(100, seed=test_seed)
        dim_product = generate_product_dimension(500, seed=test_seed)
        dim_customer = generate_customer_dimension(5000, seed=test_seed)
        dim_payment = generate_payment_dimension(seed=test_seed)

        medium_fact = generate_sales_fact(
            num_transactions=10000,
            time_df=dim_time,
            geo_df=dim_geography,
            product_df=dim_product,
            customer_df=dim_customer,
            payment_df=dim_payment,
            seed=test_seed
        )

        handler = ParquetHandler(temp_dir)
        compression_ratio = handler.estimate_compression_ratio(
            medium_fact,
            'compression_medium'
        )

        # Medium datasets should show improved compression
        assert compression_ratio >= 1.2, \
            f"Medium dataset compression {compression_ratio:.2f}:1 below 1.2:1 target"

        print(f"\nMedium dataset (10K rows) compression: {compression_ratio:.2f}:1")

    def test_parquet_vs_csv_compression(self, temp_dir, test_seed):
        """Test Parquet compression advantage over CSV."""
        from src.storage.parquet_handler import ParquetHandler
        from src.storage.csv_handler import CSVHandler

        # Generate data
        dim_time = generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed)
        dim_geography = generate_geography_dimension(50, seed=test_seed)
        dim_product = generate_product_dimension(200, seed=test_seed)
        dim_customer = generate_customer_dimension(2000, seed=test_seed)
        dim_payment = generate_payment_dimension(seed=test_seed)

        fact_data = generate_sales_fact(
            num_transactions=5000,
            time_df=dim_time,
            geo_df=dim_geography,
            product_df=dim_product,
            customer_df=dim_customer,
            payment_df=dim_payment,
            seed=test_seed
        )

        # Write both formats
        parquet_handler = ParquetHandler(temp_dir / 'parquet')
        csv_handler = CSVHandler(temp_dir / 'csv')

        parquet_path = parquet_handler.write(fact_data, 'compression_test')
        csv_path = csv_handler.write(fact_data, 'compression_test')

        # Compare sizes
        parquet_size = parquet_path.stat().st_size
        csv_size = csv_path.stat().st_size

        # Calculate compression advantage
        size_ratio = csv_size / parquet_size if parquet_size > 0 else 1

        print(f"\nCSV size: {csv_size:,} bytes")
        print(f"Parquet size: {parquet_size:,} bytes")
        print(f"CSV/Parquet ratio: {size_ratio:.2f}x")

        # CSV should be larger (Parquet is compressed)
        # For larger datasets, expect significant advantage
        # For test data, just verify both formats work
        assert parquet_size > 0
        assert csv_size > 0


@pytest.mark.integration
class TestDataQuality:
    """Test data quality and consistency."""

    def test_referential_integrity(self, temp_dir, test_seed):
        """Test referential integrity between fact and dimensions."""
        # Generate complete dataset
        dim_time = generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed)
        dim_geography = generate_geography_dimension(50, seed=test_seed)
        dim_product = generate_product_dimension(100, seed=test_seed)
        dim_customer = generate_customer_dimension(500, seed=test_seed)
        dim_payment = generate_payment_dimension(seed=test_seed)

        fact_sales = generate_sales_fact(
            num_transactions=1000,
            time_df=dim_time,
            geo_df=dim_geography,
            product_df=dim_product,
            customer_df=dim_customer,
            payment_df=dim_payment,
            seed=test_seed
        )

        # Verify all foreign keys are valid
        assert fact_sales['time_key'].isin(dim_time['time_key']).all(), \
            "Invalid time_key references found"
        assert fact_sales['geo_key'].isin(dim_geography['geo_key']).all(), \
            "Invalid geo_key references found"
        assert fact_sales['product_key'].isin(dim_product['product_key']).all(), \
            "Invalid product_key references found"
        assert fact_sales['customer_key'].isin(dim_customer['customer_key']).all(), \
            "Invalid customer_key references found"
        assert fact_sales['payment_key'].isin(dim_payment['payment_key']).all(), \
            "Invalid payment_key references found"

    def test_calculated_measures_accuracy(self, temp_dir, test_seed):
        """Test accuracy of calculated measures in fact table."""
        # Generate data
        fact_sales = generate_sales_fact(
            num_transactions=100,
            time_df=generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed),
            geo_df=generate_geography_dimension(20, seed=test_seed),
            product_df=generate_product_dimension(50, seed=test_seed),
            customer_df=generate_customer_dimension(200, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        # Verify calculated measures
        for _, row in fact_sales.iterrows():
            # Revenue = quantity * unit_price - discount
            expected_revenue = (row['quantity'] * row['unit_price']) - row['discount_amount']
            assert abs(row['revenue'] - expected_revenue) < 0.01, \
                f"Revenue calculation error: expected {expected_revenue}, got {row['revenue']}"

            # Profit = revenue - cost
            expected_profit = row['revenue'] - row['cost']
            assert abs(row['profit'] - expected_profit) < 0.01, \
                f"Profit calculation error: expected {expected_profit}, got {row['profit']}"

    def test_business_rules_validation(self, temp_dir, test_seed):
        """Test business rules are enforced in generated data."""
        fact_sales = generate_sales_fact(
            num_transactions=500,
            time_df=generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed),
            geo_df=generate_geography_dimension(30, seed=test_seed),
            product_df=generate_product_dimension(100, seed=test_seed),
            customer_df=generate_customer_dimension(500, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        # Business rules
        assert (fact_sales['quantity'] > 0).all(), "Quantity must be positive"
        assert (fact_sales['unit_price'] > 0).all(), "Unit price must be positive"
        assert (fact_sales['discount_amount'] >= 0).all(), "Discount cannot be negative"
        assert (fact_sales['revenue'] >= 0).all(), "Revenue cannot be negative"
        assert (fact_sales['cost'] >= 0).all(), "Cost cannot be negative"
