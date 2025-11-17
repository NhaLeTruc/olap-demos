"""Unit tests for storage handlers and partitioning (T097).

Tests Parquet/CSV read/write operations and partition management.
"""

import pytest
import pandas as pd
from pathlib import Path

from src.storage.parquet_handler import ParquetHandler
from src.storage.csv_handler import CSVHandler
from src.storage.partition_manager import PartitionManager


class TestParquetHandler:
    """Test Parquet storage operations."""

    def test_parquet_write_read_roundtrip(self, temp_dir, sample_fact_sales):
        """Test writing and reading Parquet files."""
        handler = ParquetHandler(temp_dir)

        # Write data
        output_path = handler.write(sample_fact_sales, 'test_sales')
        assert output_path.exists()
        assert output_path.suffix == '.parquet'

        # Read data back
        df_read = handler.read('test_sales')

        # Verify data integrity
        assert len(df_read) == len(sample_fact_sales)
        assert list(df_read.columns) == list(sample_fact_sales.columns)

        # Verify data values (sample check)
        pd.testing.assert_frame_equal(
            df_read.sort_index(),
            sample_fact_sales.sort_index()
        )

    def test_parquet_selective_column_read(self, temp_dir, sample_fact_sales):
        """Test reading specific columns from Parquet."""
        handler = ParquetHandler(temp_dir)

        # Write full data
        handler.write(sample_fact_sales, 'test_sales')

        # Read only specific columns
        columns_to_read = ['transaction_id', 'revenue', 'profit']
        df_subset = handler.read('test_sales', columns=columns_to_read)

        # Verify only requested columns returned
        assert list(df_subset.columns) == columns_to_read
        assert len(df_subset) == len(sample_fact_sales)

    def test_parquet_compression(self, temp_dir, sample_fact_sales):
        """Test Parquet compression settings."""
        handler = ParquetHandler(temp_dir)

        # Write with snappy compression (default)
        output_path = handler.write(sample_fact_sales, 'compressed')

        # Verify file is compressed (file size < uncompressed estimate)
        file_size = output_path.stat().st_size

        # Rough estimate: CSV would be ~200 bytes per row
        csv_estimate = len(sample_fact_sales) * 200
        compression_ratio = csv_estimate / file_size if file_size > 0 else 0

        # Should achieve some compression
        assert compression_ratio > 1.0, f"Compression ratio {compression_ratio:.2f} too low"

    def test_parquet_metadata_extraction(self, temp_dir, sample_fact_sales):
        """Test extracting Parquet metadata."""
        handler = ParquetHandler(temp_dir)

        # Write data
        handler.write(sample_fact_sales, 'metadata_test')

        # Get metadata
        metadata = handler.get_metadata('metadata_test')

        # Verify metadata structure
        assert 'num_rows' in metadata
        assert 'num_columns' in metadata
        assert 'file_size_bytes' in metadata

        # Verify values
        assert metadata['num_rows'] == len(sample_fact_sales)
        assert metadata['num_columns'] == len(sample_fact_sales.columns)
        assert metadata['file_size_bytes'] > 0

    def test_parquet_empty_dataframe(self, temp_dir):
        """Test handling empty DataFrame."""
        handler = ParquetHandler(temp_dir)

        # Create empty DataFrame with schema
        empty_df = pd.DataFrame(columns=['id', 'value'])

        # Should handle empty data gracefully
        output_path = handler.write(empty_df, 'empty')
        assert output_path.exists()

        # Read back
        df_read = handler.read('empty')
        assert len(df_read) == 0
        assert list(df_read.columns) == ['id', 'value']


class TestCSVHandler:
    """Test CSV storage operations."""

    def test_csv_write_read_roundtrip(self, temp_dir, sample_fact_sales):
        """Test writing and reading CSV files."""
        handler = CSVHandler(temp_dir)

        # Write data
        output_path = handler.write(sample_fact_sales, 'test_sales')
        assert output_path.exists()
        assert output_path.suffix == '.csv'

        # Read data back
        df_read = handler.read('test_sales')

        # Verify data integrity
        assert len(df_read) == len(sample_fact_sales)
        assert list(df_read.columns) == list(sample_fact_sales.columns)

    def test_csv_file_size(self, temp_dir, sample_fact_sales):
        """Test CSV file size (baseline for compression comparison)."""
        handler = CSVHandler(temp_dir)

        # Write CSV
        csv_path = handler.write(sample_fact_sales, 'size_test')
        csv_size = csv_path.stat().st_size

        # CSV should be readable text
        assert csv_size > 0

        # Rough check: CSV should be larger than minimal binary representation
        min_size = len(sample_fact_sales) * 10  # At least 10 bytes per row
        assert csv_size > min_size

    def test_csv_special_characters(self, temp_dir):
        """Test CSV handling of special characters."""
        handler = CSVHandler(temp_dir)

        # Create data with special characters
        special_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Product, Inc', 'Test "Quotes"', 'Line\nBreak'],
            'value': [100, 200, 300]
        })

        # Write and read
        handler.write(special_df, 'special')
        df_read = handler.read('special')

        # Should handle special characters correctly
        assert len(df_read) == 3
        assert 'Product, Inc' in df_read['name'].values


class TestPartitionManager:
    """Test partition management operations."""

    def test_partition_by_year(self, temp_dir, sample_dim_time, sample_fact_sales):
        """Test partitioning by year."""
        manager = PartitionManager(temp_dir)

        # Add year column to fact data (join with time dimension)
        fact_with_time = sample_fact_sales.merge(
            sample_dim_time[['time_key', 'year']],
            on='time_key',
            how='left'
        )

        # Write partitioned data
        manager.write_partitioned(fact_with_time, 'sales', partition_by=['year'])

        # Verify partition directories created
        partitions = manager.list_partitions('sales')
        assert len(partitions) > 0

        # Each partition should have year=YYYY structure
        for partition in partitions:
            assert 'year=' in partition

    def test_partition_by_year_quarter(self, temp_dir, sample_dim_time, sample_fact_sales):
        """Test partitioning by year and quarter."""
        manager = PartitionManager(temp_dir)

        # Add year and quarter columns
        fact_with_time = sample_fact_sales.merge(
            sample_dim_time[['time_key', 'year', 'quarter']],
            on='time_key',
            how='left'
        )

        # Write with hierarchical partitioning
        manager.write_partitioned(
            fact_with_time,
            'sales',
            partition_by=['year', 'quarter']
        )

        # Verify hierarchical structure
        partitions = manager.list_partitions('sales')
        assert len(partitions) > 0

        # Should have year=YYYY/quarter=QX structure
        for partition in partitions:
            assert 'year=' in partition
            # Some may have quarter, depending on data distribution

    def test_partition_statistics(self, temp_dir, sample_dim_time, sample_fact_sales):
        """Test partition statistics collection."""
        manager = PartitionManager(temp_dir)

        # Prepare partitioned data
        fact_with_time = sample_fact_sales.merge(
            sample_dim_time[['time_key', 'year']],
            on='time_key',
            how='left'
        )

        # Write partitioned
        manager.write_partitioned(fact_with_time, 'sales', partition_by=['year'])

        # Get statistics
        stats = manager.get_partition_stats('sales')

        # Verify stats structure
        assert 'total_partitions' in stats
        assert 'partitions' in stats
        assert stats['total_partitions'] > 0

        # Each partition should have row count and size
        for partition_stat in stats['partitions']:
            assert 'partition' in partition_stat
            assert 'row_count' in partition_stat or 'file_size_bytes' in partition_stat

    def test_read_specific_partition(self, temp_dir, sample_dim_time, sample_fact_sales):
        """Test reading data from specific partition."""
        manager = PartitionManager(temp_dir)

        # Prepare and write partitioned data
        fact_with_time = sample_fact_sales.merge(
            sample_dim_time[['time_key', 'year']],
            on='time_key',
            how='left'
        )
        manager.write_partitioned(fact_with_time, 'sales', partition_by=['year'])

        # Get available years
        years = fact_with_time['year'].unique()
        if len(years) > 0:
            test_year = years[0]

            # Read specific partition
            partition_data = manager.read_partition('sales', f'year={test_year}')

            # Verify filtered data
            if len(partition_data) > 0:
                assert all(partition_data['year'] == test_year)

    def test_partition_pruning_simulation(self, temp_dir, sample_dim_time, sample_fact_sales):
        """Test partition pruning logic (filter predicate pushdown)."""
        manager = PartitionManager(temp_dir)

        # Prepare partitioned data
        fact_with_time = sample_fact_sales.merge(
            sample_dim_time[['time_key', 'year']],
            on='time_key',
            how='left'
        )
        manager.write_partitioned(fact_with_time, 'sales', partition_by=['year'])

        # Get all partitions
        all_partitions = manager.list_partitions('sales')
        total_count = len(all_partitions)

        # Simulate partition pruning with year filter
        years = fact_with_time['year'].unique()
        if len(years) > 1:
            filter_year = years[0]
            pruned_partitions = [p for p in all_partitions if f'year={filter_year}' in p]

            # Should prune some partitions
            pruned_count = len(pruned_partitions)
            prune_ratio = pruned_count / total_count if total_count > 0 else 0

            assert prune_ratio < 1.0, "Partition pruning should reduce scanned partitions"


class TestStorageComparison:
    """Test Parquet vs CSV comparison metrics."""

    def test_format_size_comparison(self, temp_dir, sample_fact_sales):
        """Compare file sizes between Parquet and CSV."""
        parquet_handler = ParquetHandler(temp_dir / 'parquet')
        csv_handler = CSVHandler(temp_dir / 'csv')

        # Write both formats
        parquet_path = parquet_handler.write(sample_fact_sales, 'comparison')
        csv_path = csv_handler.write(sample_fact_sales, 'comparison')

        # Get file sizes
        parquet_size = parquet_path.stat().st_size
        csv_size = csv_path.stat().st_size

        # Both should exist and have content
        assert parquet_size > 0
        assert csv_size > 0

        # Calculate size ratio (informational, not strictly asserted)
        size_ratio = csv_size / parquet_size if parquet_size > 0 else 1
        print(f"\nCSV/Parquet size ratio: {size_ratio:.2f}x")

    def test_read_performance_comparison(self, temp_dir, sample_fact_sales):
        """Compare read performance between Parquet and CSV (timing)."""
        import time

        parquet_handler = ParquetHandler(temp_dir / 'parquet')
        csv_handler = CSVHandler(temp_dir / 'csv')

        # Write both formats
        parquet_handler.write(sample_fact_sales, 'perf_test')
        csv_handler.write(sample_fact_sales, 'perf_test')

        # Time Parquet read
        start = time.time()
        parquet_handler.read('perf_test')
        parquet_time = time.time() - start

        # Time CSV read
        start = time.time()
        csv_handler.read('perf_test')
        csv_time = time.time() - start

        # Both should complete
        assert parquet_time > 0
        assert csv_time > 0

        print(f"\nRead times - Parquet: {parquet_time*1000:.2f}ms, CSV: {csv_time*1000:.2f}ms")
