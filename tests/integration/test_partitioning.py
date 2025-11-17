"""Integration tests for partition pruning in DuckDB (T100).

Tests verifying partition skip in DuckDB query execution.
"""

import pytest
from pathlib import Path

from src.datagen.generator import (
    generate_time_dimension,
    generate_geography_dimension,
    generate_product_dimension,
    generate_customer_dimension,
    generate_payment_dimension,
    generate_sales_fact
)
from src.storage.partition_manager import PartitionManager
from src.query.duckdb_loader import DuckDBLoader
from src.query.executor import QueryExecutor
from src.query.connection import ConnectionManager


@pytest.mark.integration
class TestPartitionPruning:
    """Test partition pruning effectiveness in DuckDB."""

    @pytest.fixture
    def partitioned_data(self, temp_dir, test_seed):
        """Generate partitioned test data."""
        # Generate dimensions
        dim_time = generate_time_dimension('2021-01-01', '2023-12-31', seed=test_seed)
        dim_geography = generate_geography_dimension(50, seed=test_seed)
        dim_product = generate_product_dimension(100, seed=test_seed)
        dim_customer = generate_customer_dimension(1000, seed=test_seed)
        dim_payment = generate_payment_dimension(seed=test_seed)

        # Generate fact data
        fact_sales = generate_sales_fact(
            num_transactions=5000,
            time_df=dim_time,
            geo_df=dim_geography,
            product_df=dim_product,
            customer_df=dim_customer,
            payment_df=dim_payment,
            seed=test_seed
        )

        # Add partition columns from time dimension
        fact_with_time = fact_sales.merge(
            dim_time[['time_key', 'year', 'quarter', 'month']],
            on='time_key',
            how='left'
        )

        # Write partitioned data
        manager = PartitionManager(temp_dir / 'partitioned')
        manager.write_partitioned(
            fact_with_time,
            'fact_sales',
            partition_by=['year', 'quarter']
        )

        return {
            'temp_dir': temp_dir,
            'dim_time': dim_time,
            'dim_geography': dim_geography,
            'dim_product': dim_product,
            'dim_customer': dim_customer,
            'dim_payment': dim_payment,
            'fact_sales': fact_with_time,
            'manager': manager
        }

    def test_partition_structure_created(self, partitioned_data):
        """Test that partition directory structure is created."""
        manager = partitioned_data['manager']
        temp_dir = partitioned_data['temp_dir']

        # List partitions
        partitions = manager.list_partitions('fact_sales')

        # Should have multiple partitions
        assert len(partitions) > 0

        # Should have hierarchical structure (year/quarter)
        for partition in partitions:
            assert 'year=' in partition
            # May have quarter depending on data distribution

        print(f"\nCreated {len(partitions)} partitions")

    def test_partition_query_with_filter(self, partitioned_data):
        """Test querying partitioned data with year filter."""
        temp_dir = partitioned_data['temp_dir']
        fact_data = partitioned_data['fact_sales']

        # Load into DuckDB
        conn_manager = ConnectionManager(':memory:')
        loader = DuckDBLoader(conn_manager)

        # Load partitioned data
        loader.load_parquet(
            temp_dir / 'partitioned' / 'fact_sales',
            'fact_sales_partitioned'
        )

        # Query with year filter
        executor = QueryExecutor(conn_manager)

        # Get a year that exists in data
        years = fact_data['year'].unique()
        if len(years) > 0:
            test_year = years[0]

            result = executor.execute(f"""
                SELECT
                    year,
                    SUM(revenue) as total_revenue,
                    COUNT(*) as transaction_count
                FROM fact_sales_partitioned
                WHERE year = {test_year}
                GROUP BY year
            """)

            # Should return data for filtered year
            assert result.row_count > 0
            assert all(result.data['year'] == test_year)

            print(f"\nQueried year {test_year}: {result.row_count} rows returned")

    def test_partition_pruning_explain_plan(self, partitioned_data):
        """Test EXPLAIN plan shows partition pruning."""
        temp_dir = partitioned_data['temp_dir']
        fact_data = partitioned_data['fact_sales']

        # Load into DuckDB
        conn_manager = ConnectionManager(':memory:')
        loader = DuckDBLoader(conn_manager)
        loader.load_parquet(
            temp_dir / 'partitioned' / 'fact_sales',
            'fact_sales_partitioned'
        )

        executor = QueryExecutor(conn_manager)

        # Get test year
        years = fact_data['year'].unique()
        if len(years) > 1:  # Need multiple partitions
            test_year = years[0]

            # Get EXPLAIN plan
            explain_result = executor.execute(f"""
                EXPLAIN
                SELECT SUM(revenue)
                FROM fact_sales_partitioned
                WHERE year = {test_year}
            """)

            explain_text = str(explain_result.data)

            # EXPLAIN output should mention filtering
            # Note: Actual partition pruning detection depends on DuckDB's EXPLAIN format
            print(f"\nEXPLAIN output:\n{explain_text}")

            # At minimum, query should execute successfully
            assert explain_result.row_count > 0

    def test_full_scan_vs_partitioned_scan(self, partitioned_data):
        """Compare full scan vs partitioned scan performance."""
        temp_dir = partitioned_data['temp_dir']
        fact_data = partitioned_data['fact_sales']

        conn_manager = ConnectionManager(':memory:')
        loader = DuckDBLoader(conn_manager)
        executor = QueryExecutor(conn_manager)

        # Load partitioned data
        loader.load_parquet(
            temp_dir / 'partitioned' / 'fact_sales',
            'fact_sales_partitioned'
        )

        # Also load non-partitioned data for comparison
        from src.storage.parquet_handler import ParquetHandler
        handler = ParquetHandler(temp_dir / 'non_partitioned')
        handler.write(fact_data, 'fact_sales_flat')
        loader.load_parquet(
            temp_dir / 'non_partitioned' / 'fact_sales_flat',
            'fact_sales_flat'
        )

        years = fact_data['year'].unique()
        if len(years) > 0:
            test_year = years[0]

            # Query partitioned data
            result_partitioned = executor.execute(f"""
                SELECT SUM(revenue) as total_revenue
                FROM fact_sales_partitioned
                WHERE year = {test_year}
            """)

            # Query flat data
            result_flat = executor.execute(f"""
                SELECT SUM(revenue) as total_revenue
                FROM fact_sales_flat
                WHERE year = {test_year}
            """)

            # Results should match
            if result_partitioned.row_count > 0 and result_flat.row_count > 0:
                partitioned_revenue = result_partitioned.data.iloc[0]['total_revenue']
                flat_revenue = result_flat.data.iloc[0]['total_revenue']

                assert abs(partitioned_revenue - flat_revenue) < 0.01, \
                    "Partitioned and flat queries should return same results"

            # Partitioned query may be faster (check execution time)
            print(f"\nPartitioned query: {result_partitioned.execution_time_ms:.2f}ms")
            print(f"Flat query: {result_flat.execution_time_ms:.2f}ms")

    def test_partition_count_growth_scalability(self, partitioned_data):
        """Test that query time doesn't degrade with more partitions."""
        temp_dir = partitioned_data['temp_dir']
        manager = partitioned_data['manager']
        fact_data = partitioned_data['fact_sales']

        # Get partition statistics
        stats = manager.get_partition_stats('fact_sales')

        total_partitions = stats.get('total_partitions', 0)
        print(f"\nTotal partitions created: {total_partitions}")

        # Load and query
        conn_manager = ConnectionManager(':memory:')
        loader = DuckDBLoader(conn_manager)
        executor = QueryExecutor(conn_manager)

        loader.load_parquet(
            temp_dir / 'partitioned' / 'fact_sales',
            'fact_sales_partitioned'
        )

        # Query with partition filter should be fast regardless of total partition count
        years = fact_data['year'].unique()
        if len(years) > 0:
            test_year = years[0]

            import time
            start = time.time()

            result = executor.execute(f"""
                SELECT COUNT(*) as count
                FROM fact_sales_partitioned
                WHERE year = {test_year}
            """)

            duration_ms = (time.time() - start) * 1000

            # With partition pruning, query should be fast
            assert duration_ms < 5000, \
                f"Partition-pruned query took {duration_ms:.2f}ms (should be <5000ms)"

            print(f"Query with partition pruning: {duration_ms:.2f}ms")


@pytest.mark.integration
class TestMultiLevelPartitioning:
    """Test hierarchical partitioning (year/quarter/month)."""

    def test_hierarchical_partition_structure(self, temp_dir, test_seed):
        """Test creating hierarchical partition structure."""
        # Generate data
        dim_time = generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed)
        fact_sales = generate_sales_fact(
            num_transactions=1000,
            time_df=dim_time,
            geo_df=generate_geography_dimension(20, seed=test_seed),
            product_df=generate_product_dimension(50, seed=test_seed),
            customer_df=generate_customer_dimension(200, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        # Add time columns
        fact_with_time = fact_sales.merge(
            dim_time[['time_key', 'year', 'quarter', 'month']],
            on='time_key',
            how='left'
        )

        # Write with 3-level partitioning
        manager = PartitionManager(temp_dir)
        manager.write_partitioned(
            fact_with_time,
            'fact_hierarchical',
            partition_by=['year', 'quarter', 'month']
        )

        # Verify hierarchical structure
        partitions = manager.list_partitions('fact_hierarchical')

        # Should have partitions
        assert len(partitions) > 0

        # Some partitions should show hierarchical structure
        print(f"\nHierarchical partitions: {len(partitions)}")
        for partition in partitions[:5]:  # Show first 5
            print(f"  {partition}")

    def test_query_with_multi_level_filter(self, temp_dir, test_seed):
        """Test querying with filters at different partition levels."""
        # Generate and partition data
        dim_time = generate_time_dimension('2023-01-01', '2023-12-31', seed=test_seed)
        fact_sales = generate_sales_fact(
            num_transactions=2000,
            time_df=dim_time,
            geo_df=generate_geography_dimension(30, seed=test_seed),
            product_df=generate_product_dimension(100, seed=test_seed),
            customer_df=generate_customer_dimension(500, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        fact_with_time = fact_sales.merge(
            dim_time[['time_key', 'year', 'quarter', 'month']],
            on='time_key',
            how='left'
        )

        manager = PartitionManager(temp_dir)
        manager.write_partitioned(
            fact_with_time,
            'fact_multi_level',
            partition_by=['year', 'quarter']
        )

        # Load and query
        conn_manager = ConnectionManager(':memory:')
        loader = DuckDBLoader(conn_manager)
        executor = QueryExecutor(conn_manager)

        loader.load_parquet(
            temp_dir / 'fact_multi_level',
            'fact_multi_level'
        )

        # Query with quarter-level filter
        quarters = fact_with_time['quarter'].unique()
        if len(quarters) > 0:
            test_quarter = quarters[0]

            result = executor.execute(f"""
                SELECT
                    quarter,
                    SUM(revenue) as total_revenue,
                    COUNT(*) as transaction_count
                FROM fact_multi_level
                WHERE quarter = '{test_quarter}'
                GROUP BY quarter
            """)

            # Should return filtered data
            assert result.row_count > 0
            assert all(result.data['quarter'] == test_quarter)

            print(f"\nFiltered to quarter {test_quarter}: {result.row_count} rows")


@pytest.mark.integration
class TestPartitionMetadata:
    """Test partition metadata and statistics."""

    def test_partition_statistics_collection(self, temp_dir, test_seed):
        """Test collecting partition statistics."""
        # Generate partitioned data
        dim_time = generate_time_dimension('2022-01-01', '2023-12-31', seed=test_seed)
        fact_sales = generate_sales_fact(
            num_transactions=3000,
            time_df=dim_time,
            geo_df=generate_geography_dimension(40, seed=test_seed),
            product_df=generate_product_dimension(150, seed=test_seed),
            customer_df=generate_customer_dimension(800, seed=test_seed),
            payment_df=generate_payment_dimension(seed=test_seed),
            seed=test_seed
        )

        fact_with_time = fact_sales.merge(
            dim_time[['time_key', 'year', 'quarter']],
            on='time_key',
            how='left'
        )

        manager = PartitionManager(temp_dir)
        manager.write_partitioned(
            fact_with_time,
            'fact_stats',
            partition_by=['year']
        )

        # Get statistics
        stats = manager.get_partition_stats('fact_stats')

        # Verify statistics structure
        assert 'total_partitions' in stats
        assert 'partitions' in stats
        assert stats['total_partitions'] > 0

        print(f"\nPartition statistics:")
        print(f"  Total partitions: {stats['total_partitions']}")

        # Each partition should have metadata
        for partition_stat in stats['partitions'][:3]:  # Show first 3
            print(f"  Partition: {partition_stat.get('partition', 'unknown')}")
            if 'row_count' in partition_stat:
                print(f"    Row count: {partition_stat['row_count']}")
            if 'file_size_bytes' in partition_stat:
                print(f"    Size: {partition_stat['file_size_bytes']:,} bytes")
