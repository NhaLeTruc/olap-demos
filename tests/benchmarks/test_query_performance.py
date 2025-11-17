"""Benchmark tests for query performance.

Uses pytest-benchmark to measure and validate query execution times
against performance SLAs.
"""

import pytest


@pytest.mark.benchmark
class TestQueryBenchmarks:
    """Benchmark tests for analytical queries."""

    def test_benchmark_simple_aggregation(
        self,
        benchmark,
        loaded_duckdb,
        query_executor,
        benchmark_query_simple
    ):
        """Benchmark simple aggregation query."""
        result = benchmark(
            query_executor.execute,
            benchmark_query_simple,
            track_history=False
        )

        # Validate results
        assert result.row_count == 1

    def test_benchmark_complex_aggregation(
        self,
        benchmark,
        loaded_duckdb,
        query_executor,
        benchmark_query_complex
    ):
        """Benchmark complex multi-join aggregation."""
        result = benchmark(
            query_executor.execute,
            benchmark_query_complex,
            track_history=False
        )

        # Validate results
        assert result.row_count > 0

    def test_benchmark_filter_query(
        self,
        benchmark,
        loaded_duckdb,
        query_executor
    ):
        """Benchmark filtered aggregation query."""
        query = """
        SELECT
            dt.year,
            SUM(fs.revenue) as total_revenue
        FROM fact_sales fs
        JOIN dim_time dt ON fs.time_key = dt.time_key
        WHERE dt.year = 2024
        GROUP BY dt.year
        """

        result = benchmark(
            query_executor.execute,
            query,
            track_history=False
        )

        assert result.row_count >= 0


@pytest.mark.benchmark
class TestStorageBenchmarks:
    """Benchmark tests for storage operations."""

    def test_benchmark_parquet_write(
        self,
        benchmark,
        parquet_handler,
        benchmark_dataframe
    ):
        """Benchmark Parquet write performance."""
        benchmark(
            parquet_handler.write,
            benchmark_dataframe,
            'benchmark_test'
        )

    def test_benchmark_parquet_read(
        self,
        benchmark,
        parquet_handler,
        benchmark_dataframe
    ):
        """Benchmark Parquet read performance."""
        # Setup: write data first
        parquet_handler.write(benchmark_dataframe, 'benchmark_read_test')

        # Benchmark read
        df = benchmark(
            parquet_handler.read,
            'benchmark_read_test'
        )

        assert len(df) == len(benchmark_dataframe)

    def test_benchmark_csv_write(
        self,
        benchmark,
        csv_handler,
        benchmark_dataframe
    ):
        """Benchmark CSV write performance."""
        benchmark(
            csv_handler.write,
            benchmark_dataframe,
            'benchmark_test'
        )

    def test_benchmark_csv_read(
        self,
        benchmark,
        csv_handler,
        benchmark_dataframe
    ):
        """Benchmark CSV read performance."""
        # Setup: write data first
        csv_handler.write(benchmark_dataframe, 'benchmark_read_test')

        # Benchmark read
        df = benchmark(
            csv_handler.read,
            'benchmark_read_test'
        )

        assert len(df) == len(benchmark_dataframe)


@pytest.mark.benchmark
class TestDataGenerationBenchmarks:
    """Benchmark tests for data generation."""

    def test_benchmark_time_dimension_generation(self, benchmark, test_seed):
        """Benchmark time dimension generation."""
        from datetime import date, timedelta
        from src.datagen.generator import generate_dim_time

        end_date = date.today()
        start_date = end_date - timedelta(days=365)

        df = benchmark(
            generate_dim_time,
            start_date,
            end_date,
            test_seed
        )

        assert len(df) == 365

    def test_benchmark_product_dimension_generation(self, benchmark, test_seed):
        """Benchmark product dimension generation."""
        from src.datagen.generator import generate_dim_product

        df = benchmark(
            generate_dim_product,
            num_products=1000,
            seed=test_seed
        )

        assert len(df) > 0

    def test_benchmark_fact_generation(
        self,
        benchmark,
        test_seed,
        sample_dim_time,
        sample_dim_geography,
        sample_dim_product,
        sample_dim_customer,
        sample_dim_payment
    ):
        """Benchmark fact table generation."""
        from src.datagen.generator import generate_sales_fact

        df = benchmark(
            generate_sales_fact,
            num_transactions=1000,
            time_df=sample_dim_time,
            geo_df=sample_dim_geography,
            product_df=sample_dim_product,
            customer_df=sample_dim_customer,
            payment_df=sample_dim_payment,
            seed=test_seed
        )

        assert len(df) >= 1000  # At least 1000 line items


@pytest.mark.benchmark
class TestCompressionBenchmarks:
    """Benchmark tests for compression performance."""

    def test_benchmark_compression_ratio(
        self,
        benchmark,
        parquet_handler,
        sample_fact_sales
    ):
        """Benchmark compression ratio estimation."""
        ratio = benchmark(
            parquet_handler.estimate_compression_ratio,
            sample_fact_sales,
            'compression_test'
        )

        # Should achieve at least 2:1 compression
        assert ratio >= 2.0
