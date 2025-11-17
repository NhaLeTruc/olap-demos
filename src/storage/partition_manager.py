"""Partition management utilities for OLAP data.

This module provides utilities for managing Hive-style partitions,
extracting partition keys, and validating partition structures.
"""

from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from datetime import date


class PartitionManager:
    """Manager for Hive-style partition operations.

    Handles partition key extraction, validation, and management
    for both Parquet and CSV storage formats.
    """

    @staticmethod
    def extract_year_quarter(transaction_date: date) -> Tuple[int, str]:
        """Extract year and quarter from transaction date.

        Args:
            transaction_date: Transaction date

        Returns:
            Tuple of (year, quarter) where quarter is 'Q1'-'Q4'
        """
        year = transaction_date.year
        quarter = f"Q{((transaction_date.month - 1) // 3) + 1}"
        return year, quarter

    @staticmethod
    def add_partition_columns(df: pd.DataFrame, date_column: str = 'transaction_date') -> pd.DataFrame:
        """Add partition columns (year, quarter) to DataFrame.

        Args:
            df: DataFrame with date column
            date_column: Name of the date column to extract partitions from

        Returns:
            DataFrame with added 'year' and 'quarter' columns
        """
        df = df.copy()

        # Ensure date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column])

        # Extract partition keys
        df['year'] = df[date_column].dt.year
        df['quarter'] = df[date_column].dt.quarter.apply(lambda q: f"Q{q}")

        return df

    @staticmethod
    def parse_partition_path(partition_path: str) -> Dict[str, Any]:
        """Parse Hive-style partition path into key-value pairs.

        Args:
            partition_path: Partition path like 'year=2023/quarter=Q1'

        Returns:
            Dictionary of partition key-value pairs
            Example: {'year': '2023', 'quarter': 'Q1'}
        """
        partitions = {}

        for part in partition_path.split('/'):
            if '=' in part:
                key, value = part.split('=', 1)
                partitions[key] = value

        return partitions

    @staticmethod
    def build_partition_path(partition_values: Dict[str, Any]) -> str:
        """Build Hive-style partition path from key-value pairs.

        Args:
            partition_values: Dictionary of partition key-value pairs
                Example: {'year': 2023, 'quarter': 'Q1'}

        Returns:
            Partition path string like 'year=2023/quarter=Q1'
        """
        path_parts = [f"{key}={value}" for key, value in sorted(partition_values.items())]
        return '/'.join(path_parts)

    @staticmethod
    def list_partitions(base_path: Path, table_name: str) -> List[Dict[str, Any]]:
        """List all partitions for a table.

        Args:
            base_path: Base storage path
            table_name: Name of the table

        Returns:
            List of partition dictionaries with keys and values
        """
        table_path = base_path / table_name

        if not table_path.exists():
            return []

        partitions = []
        seen_partitions = set()

        # Find all data files (Parquet or CSV)
        for file_path in table_path.rglob("*"):
            if file_path.suffix in ['.parquet', '.csv']:
                # Extract partition path relative to table root
                partition_path = str(file_path.parent.relative_to(table_path))

                if partition_path != "." and partition_path not in seen_partitions:
                    partition_dict = PartitionManager.parse_partition_path(partition_path)
                    if partition_dict:
                        partitions.append(partition_dict)
                        seen_partitions.add(partition_path)

        return partitions

    @staticmethod
    def validate_partitions(
        df: pd.DataFrame,
        partition_cols: List[str]
    ) -> Tuple[bool, List[str]]:
        """Validate that DataFrame has required partition columns.

        Args:
            df: DataFrame to validate
            partition_cols: Required partition column names

        Returns:
            Tuple of (is_valid, missing_columns)
        """
        missing_cols = [col for col in partition_cols if col not in df.columns]
        is_valid = len(missing_cols) == 0

        return is_valid, missing_cols

    @staticmethod
    def get_partition_statistics(
        base_path: Path,
        table_name: str
    ) -> Dict[str, Any]:
        """Get statistics about table partitions.

        Args:
            base_path: Base storage path
            table_name: Name of the table

        Returns:
            Dictionary with partition statistics:
            - num_partitions: Total number of partitions
            - partitions: List of partition key-value dicts
            - partition_keys: List of partition column names
        """
        partitions = PartitionManager.list_partitions(base_path, table_name)

        partition_keys = []
        if partitions:
            partition_keys = sorted(partitions[0].keys())

        return {
            'num_partitions': len(partitions),
            'partitions': partitions,
            'partition_keys': partition_keys,
        }

    @staticmethod
    def filter_partitions(
        partitions: List[Dict[str, Any]],
        filters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Filter partition list by criteria.

        Args:
            partitions: List of partition dictionaries
            filters: Filter criteria
                Example: {'year': 2023, 'quarter': 'Q1'}

        Returns:
            Filtered list of partitions
        """
        filtered = []

        for partition in partitions:
            matches = True
            for key, value in filters.items():
                if partition.get(key) != str(value):
                    matches = False
                    break

            if matches:
                filtered.append(partition)

        return filtered

    @staticmethod
    def create_partition_filters(
        year: Optional[int] = None,
        quarter: Optional[str] = None
    ) -> List[Tuple[str, str, Any]]:
        """Create PyArrow partition filters for Parquet reads.

        Args:
            year: Optional year filter
            quarter: Optional quarter filter ('Q1'-'Q4')

        Returns:
            List of PyArrow filter tuples
            Example: [('year', '=', 2023), ('quarter', '=', 'Q1')]
        """
        filters = []

        if year is not None:
            filters.append(('year', '=', year))

        if quarter is not None:
            filters.append(('quarter', '=', quarter))

        return filters if filters else None

    @staticmethod
    def estimate_partition_sizes(
        base_path: Path,
        table_name: str
    ) -> List[Dict[str, Any]]:
        """Estimate size of each partition.

        Args:
            base_path: Base storage path
            table_name: Name of the table

        Returns:
            List of dictionaries with partition info and size:
            [
                {
                    'partition': {'year': '2023', 'quarter': 'Q1'},
                    'size_bytes': 12345,
                    'num_files': 2
                },
                ...
            ]
        """
        table_path = base_path / table_name

        if not table_path.exists():
            return []

        partition_stats = {}

        # Aggregate file sizes by partition
        for file_path in table_path.rglob("*"):
            if file_path.suffix in ['.parquet', '.csv']:
                partition_path = str(file_path.parent.relative_to(table_path))

                if partition_path != ".":
                    partition_dict = PartitionManager.parse_partition_path(partition_path)
                    partition_key = PartitionManager.build_partition_path(partition_dict)

                    if partition_key not in partition_stats:
                        partition_stats[partition_key] = {
                            'partition': partition_dict,
                            'size_bytes': 0,
                            'num_files': 0
                        }

                    partition_stats[partition_key]['size_bytes'] += file_path.stat().st_size
                    partition_stats[partition_key]['num_files'] += 1

        return list(partition_stats.values())
