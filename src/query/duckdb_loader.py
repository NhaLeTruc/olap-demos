"""DuckDB data loader for OLAP operations.

This module provides utilities for loading Parquet and CSV data
into DuckDB for analytical query execution.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any
import duckdb
import pandas as pd


class DuckDBLoader:
    """Loader for ingesting data into DuckDB.

    Supports loading from Parquet, CSV, and Pandas DataFrames
    with optimized settings for OLAP workloads.
    """

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize DuckDB loader.

        Args:
            db_path: Path to DuckDB database file (None for in-memory)
        """
        self.db_path = db_path
        self.connection = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Establish connection to DuckDB.

        Returns:
            DuckDB connection object
        """
        if self.connection is None:
            db_str = str(self.db_path) if self.db_path else ":memory:"
            self.connection = duckdb.connect(db_str)

            # Configure for OLAP performance
            self.connection.execute("PRAGMA threads=4")
            self.connection.execute("PRAGMA memory_limit='2GB'")

        return self.connection

    def disconnect(self):
        """Close DuckDB connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def load_parquet(
        self,
        table_name: str,
        parquet_path: Path,
        partition_filter: Optional[str] = None
    ) -> None:
        """Load Parquet file(s) into DuckDB table.

        Args:
            table_name: Name of the table to create
            parquet_path: Path to Parquet file or directory
            partition_filter: Optional SQL WHERE clause for partition pruning
                Example: "year = 2023 AND quarter = 'Q1'"
        """
        conn = self.connect()

        # Build CREATE TABLE statement
        if parquet_path.is_dir():
            # Load partitioned dataset
            parquet_pattern = str(parquet_path / "**" / "*.parquet")
        else:
            # Load single file
            parquet_pattern = str(parquet_path)

        create_sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_pattern}')"

        if partition_filter:
            create_sql += f" WHERE {partition_filter}"

        conn.execute(create_sql)

    def load_csv(
        self,
        table_name: str,
        csv_path: Path,
        delimiter: str = ",",
        header: bool = True
    ) -> None:
        """Load CSV file(s) into DuckDB table.

        Args:
            table_name: Name of the table to create
            csv_path: Path to CSV file or directory
            delimiter: Field delimiter (default: comma)
            header: Whether CSV has header row
        """
        conn = self.connect()

        # Build CREATE TABLE statement
        if csv_path.is_dir():
            # Load multiple CSV files
            csv_pattern = str(csv_path / "**" / "*.csv")
        else:
            # Load single file
            csv_pattern = str(csv_path)

        create_sql = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv(
            '{csv_pattern}',
            delim='{delimiter}',
            header={str(header).lower()}
        )
        """

        conn.execute(create_sql)

    def load_dataframe(
        self,
        table_name: str,
        df: pd.DataFrame,
        replace: bool = True
    ) -> None:
        """Load Pandas DataFrame into DuckDB table.

        Args:
            table_name: Name of the table to create
            df: DataFrame to load
            replace: Whether to replace existing table
        """
        conn = self.connect()

        if replace:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Register DataFrame as table
        conn.register(table_name, df)

        # Persist as DuckDB table
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")

    def bulk_load_star_schema(
        self,
        parquet_base_path: Path,
        dimension_tables: List[str],
        fact_table: str,
        partition_filter: Optional[str] = None
    ) -> Dict[str, int]:
        """Bulk load complete star schema from Parquet files.

        Args:
            parquet_base_path: Base path containing table directories
            dimension_tables: List of dimension table names
            fact_table: Name of fact table
            partition_filter: Optional partition filter for fact table

        Returns:
            Dictionary with row counts for each table
        """
        conn = self.connect()
        row_counts = {}

        # Load dimension tables
        for dim_table in dimension_tables:
            dim_path = parquet_base_path / dim_table

            if dim_path.exists():
                self.load_parquet(dim_table, dim_path)

                # Get row count
                result = conn.execute(f"SELECT COUNT(*) FROM {dim_table}").fetchone()
                row_counts[dim_table] = result[0]

        # Load fact table
        fact_path = parquet_base_path / fact_table

        if fact_path.exists():
            self.load_parquet(fact_table, fact_path, partition_filter)

            # Get row count
            result = conn.execute(f"SELECT COUNT(*) FROM {fact_table}").fetchone()
            row_counts[fact_table] = result[0]

        return row_counts

    def create_indexes(
        self,
        table_name: str,
        index_columns: List[str]
    ) -> None:
        """Create indexes on table columns.

        Note: DuckDB automatically creates indexes on foreign keys,
        but explicit indexes can improve query performance.

        Args:
            table_name: Name of the table
            index_columns: List of columns to index
        """
        conn = self.connect()

        # DuckDB doesn't support CREATE INDEX directly,
        # but we can use statistics to improve query planning
        for col in index_columns:
            conn.execute(f"ANALYZE {table_name}")

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table metadata
        """
        conn = self.connect()

        # Get row count
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]

        # Get column info
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()

        # Get table size estimate
        size_query = f"""
        SELECT
            SUM(estimated_size) as total_size_bytes
        FROM duckdb_tables()
        WHERE table_name = '{table_name}'
        """
        size_result = conn.execute(size_query).fetchone()
        size_bytes = size_result[0] if size_result else 0

        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': [{'name': col[0], 'type': col[1]} for col in columns],
            'num_columns': len(columns),
            'estimated_size_bytes': size_bytes,
        }

    def list_tables(self) -> List[str]:
        """List all tables in the database.

        Returns:
            List of table names
        """
        conn = self.connect()

        result = conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()

        return [row[0] for row in result]

    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            DataFrame with query results
        """
        conn = self.connect()

        return conn.execute(sql).df()

    def export_to_parquet(
        self,
        table_name: str,
        output_path: Path,
        partition_by: Optional[List[str]] = None
    ) -> None:
        """Export table to Parquet file.

        Args:
            table_name: Name of the table to export
            output_path: Path for output Parquet file
            partition_by: Optional columns to partition by
        """
        conn = self.connect()

        output_path.parent.mkdir(parents=True, exist_ok=True)

        if partition_by:
            partition_clause = f"PARTITION_BY ({', '.join(partition_by)})"
            export_sql = f"""
            COPY {table_name}
            TO '{output_path}'
            (FORMAT PARQUET, {partition_clause})
            """
        else:
            export_sql = f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET)"

        conn.execute(export_sql)
