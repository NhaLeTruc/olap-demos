"""CSV storage handler for baseline comparison.

This module provides utilities for writing and reading CSV files
for performance comparison with columnar formats.
"""

from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import csv


class CSVHandler:
    """Handler for CSV file operations.

    Provides baseline storage format for performance comparison
    with columnar formats like Parquet.
    """

    def __init__(
        self,
        base_path: Path,
        delimiter: str = ",",
        quoting: int = csv.QUOTE_MINIMAL
    ):
        """Initialize CSV handler.

        Args:
            base_path: Base directory for CSV files
            delimiter: Field delimiter (default: comma)
            quoting: CSV quoting style
        """
        self.base_path = Path(base_path)
        self.delimiter = delimiter
        self.quoting = quoting

    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        filename: Optional[str] = None,
        **kwargs
    ) -> Path:
        """Write DataFrame to CSV file.

        Args:
            df: DataFrame to write
            table_name: Name of the table (used as subdirectory)
            filename: Optional filename (default: {table_name}.csv)
            **kwargs: Additional arguments passed to DataFrame.to_csv

        Returns:
            Path to written file
        """
        output_path = self.base_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        if filename is None:
            filename = f"{table_name}.csv"

        file_path = output_path / filename

        # Write CSV with optimized settings
        df.to_csv(
            file_path,
            index=False,
            sep=self.delimiter,
            quoting=self.quoting,
            **kwargs
        )

        return file_path

    def read(
        self,
        table_name: str,
        filename: Optional[str] = None,
        columns: Optional[List[str]] = None,
        nrows: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read CSV file into DataFrame.

        Args:
            table_name: Name of the table (subdirectory)
            filename: Optional filename (default: {table_name}.csv)
            columns: Optional list of columns to read
            nrows: Optional number of rows to read
            **kwargs: Additional arguments passed to pd.read_csv

        Returns:
            DataFrame with loaded data
        """
        if filename is None:
            filename = f"{table_name}.csv"

        file_path = self.base_path / table_name / filename

        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        # Read CSV with column selection
        df = pd.read_csv(
            file_path,
            sep=self.delimiter,
            usecols=columns,
            nrows=nrows,
            **kwargs
        )

        return df

    def get_metadata(self, table_name: str, filename: Optional[str] = None) -> Dict[str, Any]:
        """Get CSV file metadata.

        Args:
            table_name: Name of the table (subdirectory)
            filename: Optional filename (default: {table_name}.csv)

        Returns:
            Dictionary with metadata including:
            - num_rows: Number of rows (excluding header)
            - num_columns: Number of columns
            - file_size_bytes: File size in bytes
            - delimiter: Field delimiter
        """
        if filename is None:
            filename = f"{table_name}.csv"

        file_path = self.base_path / table_name / filename

        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")

        # Count rows efficiently
        with open(file_path, 'r') as f:
            num_rows = sum(1 for _ in f) - 1  # Exclude header

        # Get column count from header
        with open(file_path, 'r') as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            header = next(reader)
            num_columns = len(header)

        return {
            'num_rows': num_rows,
            'num_columns': num_columns,
            'file_size_bytes': file_path.stat().st_size,
            'delimiter': self.delimiter,
            'file_path': str(file_path),
        }

    def write_partitioned(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_cols: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """Write DataFrame to partitioned CSV files.

        Creates Hive-style partitioned directory structure (for comparison).

        Args:
            df: DataFrame to write
            table_name: Name of the table (used as subdirectory)
            partition_cols: Columns to partition by (e.g., ['year', 'quarter'])
            **kwargs: Additional arguments passed to DataFrame.to_csv
        """
        if partition_cols is None or len(partition_cols) == 0:
            # No partitioning, write single file
            self.write(df, table_name, **kwargs)
            return

        output_path = self.base_path / table_name
        output_path.mkdir(parents=True, exist_ok=True)

        # Group by partition columns
        grouped = df.groupby(partition_cols, as_index=False)

        for partition_values, partition_df in grouped:
            # Create partition directory path
            partition_path = output_path

            if len(partition_cols) == 1:
                partition_values = [partition_values]

            for col, val in zip(partition_cols, partition_values):
                partition_path = partition_path / f"{col}={val}"

            partition_path.mkdir(parents=True, exist_ok=True)

            # Write partition file
            partition_file = partition_path / "data.csv"

            # Drop partition columns from data (they're in the path)
            partition_df_clean = partition_df.drop(columns=partition_cols)

            partition_df_clean.to_csv(
                partition_file,
                index=False,
                sep=self.delimiter,
                quoting=self.quoting,
                **kwargs
            )

    def read_partitioned(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        partition_filter: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read partitioned CSV dataset into DataFrame.

        Args:
            table_name: Name of the table (subdirectory)
            columns: Optional list of columns to read
            partition_filter: Optional partition filter dict
                Example: {'year': 2023, 'quarter': 'Q1'}
            **kwargs: Additional arguments passed to pd.read_csv

        Returns:
            DataFrame with loaded data
        """
        dataset_path = self.base_path / table_name

        if not dataset_path.exists():
            raise FileNotFoundError(f"CSV dataset not found: {dataset_path}")

        # Find all CSV files in partitions
        csv_files = list(dataset_path.rglob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in: {dataset_path}")

        # Apply partition filtering if specified
        if partition_filter:
            filtered_files = []
            for csv_file in csv_files:
                # Check if file path matches partition filter
                path_str = str(csv_file.relative_to(dataset_path))
                matches = True

                for col, val in partition_filter.items():
                    if f"{col}={val}" not in path_str:
                        matches = False
                        break

                if matches:
                    filtered_files.append(csv_file)

            csv_files = filtered_files

        if not csv_files:
            # Return empty DataFrame with correct columns
            sample_df = pd.read_csv(csv_files[0] if csv_files else dataset_path / "data.csv", nrows=0)
            return sample_df

        # Read all matching files
        dfs = []
        for csv_file in csv_files:
            df = pd.read_csv(
                csv_file,
                sep=self.delimiter,
                usecols=columns,
                **kwargs
            )

            # Extract partition values from path and add as columns
            path_parts = csv_file.relative_to(dataset_path).parent.parts
            for part in path_parts:
                if '=' in part:
                    col, val = part.split('=', 1)
                    df[col] = val

            dfs.append(df)

        # Combine all partitions
        return pd.concat(dfs, ignore_index=True)
