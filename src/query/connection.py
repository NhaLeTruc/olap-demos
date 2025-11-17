"""DuckDB connection management with configuration.

This module provides connection pooling and configuration
management for DuckDB analytical queries.
"""

from pathlib import Path
from typing import Optional, Dict, Any
import duckdb
from contextlib import contextmanager


class ConnectionManager:
    """Manager for DuckDB database connections.

    Provides connection pooling, configuration management,
    and performance tuning for OLAP workloads.
    """

    def __init__(
        self,
        db_path: Optional[Path] = None,
        threads: int = 4,
        memory_limit: str = "2GB",
        enable_profiling: bool = False
    ):
        """Initialize connection manager.

        Args:
            db_path: Path to DuckDB database file (None for in-memory)
            threads: Number of threads for parallel query execution
            memory_limit: Memory limit for queries (e.g., '2GB', '500MB')
            enable_profiling: Whether to enable query profiling
        """
        self.db_path = db_path
        self.threads = threads
        self.memory_limit = memory_limit
        self.enable_profiling = enable_profiling
        self._connection = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection.

        Returns:
            DuckDB connection object
        """
        if self._connection is None:
            self._connection = self._create_connection()
            self._configure_connection()

        return self._connection

    def _create_connection(self) -> duckdb.DuckDBPyConnection:
        """Create new DuckDB connection.

        Returns:
            New DuckDB connection
        """
        db_str = str(self.db_path) if self.db_path else ":memory:"
        return duckdb.connect(db_str)

    def _configure_connection(self) -> None:
        """Configure connection for OLAP performance."""
        conn = self._connection

        # Set thread count
        conn.execute(f"PRAGMA threads={self.threads}")

        # Set memory limit
        conn.execute(f"PRAGMA memory_limit='{self.memory_limit}'")

        # Enable profiling if requested
        if self.enable_profiling:
            conn.execute("PRAGMA enable_profiling='query_tree'")
            conn.execute("PRAGMA profiling_output='profile.json'")

        # Enable progress bar for long queries
        conn.execute("PRAGMA enable_progress_bar=true")

        # Optimize for read-only analytical queries
        conn.execute("PRAGMA temp_directory='./tmp'")

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def reconnect(self) -> None:
        """Close and reopen connection with current configuration."""
        self.close()
        # Connection will be recreated on next access

    @contextmanager
    def get_connection(self):
        """Context manager for database connection.

        Yields:
            DuckDB connection object

        Example:
            with manager.get_connection() as conn:
                result = conn.execute("SELECT * FROM table")
        """
        try:
            yield self.connection
        except Exception as e:
            # Log error and re-raise
            raise e

    def execute(self, sql: str) -> duckdb.DuckDBPyRelation:
        """Execute SQL query.

        Args:
            sql: SQL query string

        Returns:
            DuckDB relation with query results
        """
        return self.connection.execute(sql)

    def execute_many(self, statements: list[str]) -> None:
        """Execute multiple SQL statements.

        Args:
            statements: List of SQL statement strings
        """
        for stmt in statements:
            self.connection.execute(stmt)

    def get_config(self) -> Dict[str, Any]:
        """Get current connection configuration.

        Returns:
            Dictionary with configuration settings
        """
        return {
            'db_path': str(self.db_path) if self.db_path else ':memory:',
            'threads': self.threads,
            'memory_limit': self.memory_limit,
            'enable_profiling': self.enable_profiling,
        }

    def set_memory_limit(self, limit: str) -> None:
        """Update memory limit for queries.

        Args:
            limit: New memory limit (e.g., '4GB', '1GB')
        """
        self.memory_limit = limit
        if self._connection:
            self._connection.execute(f"PRAGMA memory_limit='{limit}'")

    def set_threads(self, threads: int) -> None:
        """Update thread count for parallel execution.

        Args:
            threads: Number of threads
        """
        self.threads = threads
        if self._connection:
            self._connection.execute(f"PRAGMA threads={threads}")

    def enable_query_profiling(self) -> None:
        """Enable query profiling for performance analysis."""
        self.enable_profiling = True
        if self._connection:
            self._connection.execute("PRAGMA enable_profiling='query_tree'")
            self._connection.execute("PRAGMA profiling_output='profile.json'")

    def disable_query_profiling(self) -> None:
        """Disable query profiling."""
        self.enable_profiling = False
        if self._connection:
            self._connection.execute("PRAGMA disable_profiling")

    def get_database_size(self) -> int:
        """Get size of database file in bytes.

        Returns:
            Database size in bytes (0 for in-memory)
        """
        if self.db_path and self.db_path.exists():
            return self.db_path.stat().st_size
        return 0

    def vacuum(self) -> None:
        """Vacuum database to reclaim space."""
        if self._connection:
            self._connection.execute("VACUUM")

    def checkpoint(self) -> None:
        """Create checkpoint for WAL mode."""
        if self._connection:
            self._connection.execute("CHECKPOINT")
