"""Integration tests for end-to-end OLAP workflows.

Tests complete workflows from data generation through query execution,
validating the entire OLAP pipeline.
"""

import pytest
from pathlib import Path

from src.storage.partition_manager import PartitionManager


@pytest.mark.integration
class TestDataPipeline:
    """Test complete data generation and storage pipeline."""

    def test_parquet_write_read_roundtrip(
        self,
        parquet_handler,
        sample_fact_sales
    ):
        """Test writing and reading Parquet preserves data."""
        # Write
        parquet_handler.write(sample_fact_sales, 'test_fact')

        # Read
        loaded_df = parquet_handler.read('test_fact')

        # Compare
        assert len(loaded_df) == len(sample_fact_sales)
        assert set(loaded_df.columns) == set(sample_fact_sales.columns)

    def test_parquet_partitioned_write_read(
        self,
        parquet_handler,
        sample_fact_sales
    ):
        """Test partitioned Parquet write and read."""
        # Add partition columns
        partitioned_df = PartitionManager.add_partition_columns(
            sample_fact_sales,
            'transaction_date'
        )

        # Write partitioned
        parquet_handler.write_partitioned(
            partitioned_df,
            'test_partitioned',
            partition_cols=['year', 'quarter']
        )

        # Read all partitions
        loaded_df = parquet_handler.read_partitioned('test_partitioned')

        assert len(loaded_df) == len(sample_fact_sales)

    def test_csv_write_read_roundtrip(
        self,
        csv_handler,
        sample_dim_customer
    ):
        """Test writing and reading CSV preserves data."""
        # Write
        csv_handler.write(sample_dim_customer, 'test_customer')

        # Read
        loaded_df = csv_handler.read('test_customer')

        # Compare
        assert len(loaded_df) == len(sample_dim_customer)
        assert set(loaded_df.columns) == set(sample_dim_customer.columns)


@pytest.mark.integration
class TestQueryPipeline:
    """Test complete query execution pipeline."""

    def test_load_and_query_star_schema(self, loaded_duckdb, query_executor):
        """Test loading star schema and executing queries."""
        # Verify tables loaded
        tables = loaded_duckdb.list_tables()
        assert 'fact_sales' in tables
        assert 'dim_time' in tables

        # Execute simple query
        result = query_executor.execute(
            "SELECT COUNT(*) as row_count FROM fact_sales"
        )

        assert result.row_count == 1
        assert result.data.iloc[0, 0] > 0

    def test_multi_dimensional_aggregation(self, loaded_duckdb, query_executor):
        """Test multi-dimensional aggregation query."""
        query = """
        SELECT
            dt.year,
            dg.country,
            SUM(fs.revenue) as total_revenue,
            COUNT(*) as transaction_count
        FROM fact_sales fs
        JOIN dim_time dt ON fs.time_key = dt.time_key
        JOIN dim_geography dg ON fs.geo_key = dg.geo_key
        GROUP BY dt.year, dg.country
        """

        result = query_executor.execute(query)

        # Should have results
        assert result.row_count > 0

        # Should have expected columns
        assert 'year' in result.data.columns
        assert 'country' in result.data.columns
        assert 'total_revenue' in result.data.columns

        # Revenue should be positive
        assert (result.data['total_revenue'] > 0).all()

    def test_product_scd_query(self, loaded_duckdb, query_executor):
        """Test querying SCD Type 2 product dimension."""
        query = """
        SELECT
            product_id,
            COUNT(*) as version_count,
            SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_count
        FROM dim_product
        GROUP BY product_id
        """

        result = query_executor.execute(query)

        # Each product should have at least one version
        assert (result.data['version_count'] >= 1).all()

        # Each product should have exactly one current version
        assert (result.data['current_count'] == 1).all()

    def test_partition_pruning_query(self, loaded_duckdb, query_executor):
        """Test query with partition filter."""
        # This would use partition pruning if fact table is partitioned
        query = """
        SELECT
            COUNT(*) as row_count,
            SUM(revenue) as total_revenue
        FROM fact_sales
        WHERE year = 2024
        """

        result = query_executor.execute(query)

        assert result.row_count == 1
        assert result.data.iloc[0, 0] >= 0  # Some rows for 2024


@pytest.mark.integration
class TestQueryPerformance:
    """Test query performance characteristics."""

    def test_simple_aggregation_performance(self, loaded_duckdb, query_profiler):
        """Test simple aggregation query performance."""
        query = "SELECT SUM(revenue), AVG(profit) FROM fact_sales"

        profile = query_profiler.profile_query("simple_agg", query)

        # Should complete reasonably fast (even for small dataset)
        assert profile.execution_time_ms < 5000  # 5 seconds

    def test_join_query_performance(self, loaded_duckdb, query_profiler):
        """Test join query performance."""
        query = """
        SELECT
            dt.year,
            dp.category,
            COUNT(*) as count
        FROM fact_sales fs
        JOIN dim_time dt ON fs.time_key = dt.time_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
        GROUP BY dt.year, dp.category
        """

        profile = query_profiler.profile_query("join_agg", query)

        # Should complete within SLA
        assert profile.execution_time_ms < 5000  # 5 seconds

    def test_benchmark_consistency(self, loaded_duckdb, query_profiler):
        """Test benchmark produces consistent results."""
        query = "SELECT COUNT(*) FROM fact_sales"

        benchmark = query_profiler.benchmark_query(
            "consistency_test",
            query,
            num_runs=3
        )

        # Execution times should be similar (within 2x)
        min_time = benchmark['min_execution_time_ms']
        max_time = benchmark['max_execution_time_ms']

        assert max_time / min_time < 2.0  # Less than 2x variance
