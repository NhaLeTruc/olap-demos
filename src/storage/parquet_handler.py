"""Parquet storage handler with partitioning and compression support.

This module provides utilities for writing and reading Parquet files
with Hive-style partitioning and optimal compression settings.
"""

from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date


class ParquetHandler:
    """Handler for Parquet file operations with partitioning support.

    Implements Hive-style partitioning (e.g., year=2023/quarter=Q1) and
    configurable compression for optimal OLAP performance.
    """

    def __init__(
        self,
        base_path: Path,
        compression: str = "snappy",
        row_group_size: int = 100000
    ):
        """Initialize Parquet handler.

        Args:
            base_path: Base directory for Parquet files
            compression: Compression codec (snappy, gzip, zstd, etc.)
            row_group_size: Target row group size for writes
        """
        self.base_path = Path(base_path)
        self.compression = compression
        self.row_group_size = row_group_size

    def write_partitioned(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """Write DataFrame to partitioned Parquet files.

        Creates Hive-style partitioned directory structure.

        Args:
            df: DataFrame to write
            table_name: Name of the table (used as subdirectory)
            partition_cols: Columns to partition by (e.g., ['year', 'quarter'])
            **kwargs: Additional arguments passed to pyarrow.parquet.write_to_dataset
        """
        output_path = self.base_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write with partitioning
        pq.write_to_dataset(
            table,
            root_path=str(output_path),
            partition_cols=partition_cols,
            compression=self.compression,
            row_group_size=self.row_group_size,
            existing_data_behavior='overwrite_or_ignore',
            **kwargs
        )

    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        filename: Optional[str] = None,
        **kwargs
    ) -> Path:
        """Write DataFrame to single Parquet file.

        Args:
            df: DataFrame to write
            table_name: Name of the table (used as subdirectory)
            filename: Optional filename (default: {table_name}.parquet)
            **kwargs: Additional arguments passed to pyarrow.parquet.write_table

        Returns:
            Path to written file
        """
        output_path = self.base_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        if filename is None:
            filename = f"{table_name}.parquet"

        file_path = output_path / filename

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write file
        pq.write_table(
            table,
            str(file_path),
            compression=self.compression,
            row_group_size=self.row_group_size,
            **kwargs
        )

        return file_path

    def read(
        self,
        table_name: str,
        filename: Optional[str] = None,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read Parquet file into DataFrame.

        Args:
            table_name: Name of the table (subdirectory)
            filename: Optional filename (default: {table_name}.parquet)
            columns: Optional list of columns to read (column pruning)
            filters: Optional PyArrow filters for partition pruning
            **kwargs: Additional arguments passed to pyarrow.parquet.read_table

        Returns:
            DataFrame with loaded data
        """
        if filename is None:
            filename = f"{table_name}.parquet"

        file_path = self.base_path / table_name / filename

        if not file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")

        # Read with column pruning
        table = pq.read_table(
            str(file_path),
            columns=columns,
            filters=filters,
            **kwargs
        )

        return table.to_pandas()

    def read_partitioned(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filters: Optional[List[Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read partitioned Parquet dataset into DataFrame.

        Supports partition pruning via filters.

        Args:
            table_name: Name of the table (subdirectory)
            columns: Optional list of columns to read (column pruning)
            filters: Optional PyArrow filters for partition pruning
                Example: [('year', '=', 2023), ('quarter', '=', 'Q1')]
            **kwargs: Additional arguments passed to pyarrow.parquet.read_table

        Returns:
            DataFrame with loaded data
        """
        dataset_path = self.base_path / table_name

        if not dataset_path.exists():
            raise FileNotFoundError(f"Parquet dataset not found: {dataset_path}")

        # Read partitioned dataset
        dataset = pq.ParquetDataset(
            str(dataset_path),
            use_legacy_dataset=False,
            filters=filters
        )

        table = dataset.read(columns=columns, **kwargs)

        return table.to_pandas()

    def get_metadata(self, table_name: str, filename: Optional[str] = None) -> Dict[str, Any]:
        """Get Parquet file metadata.

        Args:
            table_name: Name of the table (subdirectory)
            filename: Optional filename (default: {table_name}.parquet)

        Returns:
            Dictionary with metadata including:
            - num_rows: Number of rows
            - num_row_groups: Number of row groups
            - num_columns: Number of columns
            - compression: Compression codec
            - file_size_bytes: File size in bytes
        """
        if filename is None:
            filename = f"{table_name}.parquet"

        file_path = self.base_path / table_name / filename

        if not file_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {file_path}")

        parquet_file = pq.ParquetFile(str(file_path))
        metadata = parquet_file.metadata

        return {
            'num_rows': metadata.num_rows,
            'num_row_groups': metadata.num_row_groups,
            'num_columns': metadata.num_columns,
            'compression': self.compression,
            'file_size_bytes': file_path.stat().st_size,
            'file_path': str(file_path),
        }

    def get_partitions(self, table_name: str) -> List[str]:
        """Get list of partition directories for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of partition directory paths
        """
        dataset_path = self.base_path / table_name

        if not dataset_path.exists():
            return []

        # Find all partition directories
        partitions = []
        for path in dataset_path.rglob("*.parquet"):
            partition = str(path.parent.relative_to(dataset_path))
            if partition != ".":
                partitions.append(partition)

        return sorted(set(partitions))

    def estimate_compression_ratio(
        self,
        df: pd.DataFrame,
        table_name: str = "temp"
    ) -> float:
        """Estimate compression ratio for DataFrame.

        Args:
            df: DataFrame to estimate
            table_name: Temporary table name for estimation

        Returns:
            Compression ratio (uncompressed / compressed)
        """
        # Write temporary file
        temp_path = self.base_path / f"_temp_{table_name}"
        temp_path.mkdir(parents=True, exist_ok=True)

        temp_file = temp_path / "temp.parquet"

        table = pa.Table.from_pandas(df)

        # Write compressed
        pq.write_table(table, str(temp_file), compression=self.compression)
        compressed_size = temp_file.stat().st_size

        # Write uncompressed
        pq.write_table(table, str(temp_file), compression='none')
        uncompressed_size = temp_file.stat().st_size

        # Cleanup
        temp_file.unlink()
        temp_path.rmdir()

        return uncompressed_size / compressed_size if compressed_size > 0 else 0
