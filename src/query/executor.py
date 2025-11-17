"""Query executor with timing and result management.

This module provides utilities for executing analytical queries
with performance tracking and result handling.
"""

from typing import Dict, Any, Optional, List
import time
from datetime import datetime
import pandas as pd
from .connection import ConnectionManager


class QueryResult:
    """Container for query results with metadata.

    Stores query results, execution metrics, and metadata
    for analysis and reporting.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        execution_time_ms: float,
        row_count: int,
        query_sql: str,
        timestamp: datetime
    ):
        """Initialize query result.

        Args:
            data: Result DataFrame
            execution_time_ms: Query execution time in milliseconds
            row_count: Number of rows returned
            query_sql: SQL query that was executed
            timestamp: Timestamp of query execution
        """
        self.data = data
        self.execution_time_ms = execution_time_ms
        self.row_count = row_count
        self.query_sql = query_sql
        self.timestamp = timestamp

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary.

        Returns:
            Dictionary with result metadata
        """
        return {
            'row_count': self.row_count,
            'execution_time_ms': self.execution_time_ms,
            'execution_time_s': self.execution_time_ms / 1000,
            'query_sql': self.query_sql,
            'timestamp': self.timestamp.isoformat(),
            'columns': list(self.data.columns) if not self.data.empty else [],
        }


class QueryExecutor:
    """Executor for analytical queries with performance tracking.

    Provides query execution, timing, and result management
    for OLAP workloads.
    """

    def __init__(self, connection_manager: ConnectionManager):
        """Initialize query executor.

        Args:
            connection_manager: DuckDB connection manager
        """
        self.conn_manager = connection_manager
        self.query_history: List[QueryResult] = []

    def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        track_history: bool = True
    ) -> QueryResult:
        """Execute SQL query with timing.

        Args:
            sql: SQL query string
            params: Optional query parameters for parameterized queries
            track_history: Whether to store result in query history

        Returns:
            QueryResult with data and execution metadata
        """
        # Substitute parameters if provided
        if params:
            for key, value in params.items():
                placeholder = f":{key}"
                if isinstance(value, str):
                    sql = sql.replace(placeholder, f"'{value}'")
                else:
                    sql = sql.replace(placeholder, str(value))

        # Execute with timing
        start_time = time.time()
        timestamp = datetime.now()

        try:
            df = self.conn_manager.execute(sql).df()
            execution_time_ms = (time.time() - start_time) * 1000

            result = QueryResult(
                data=df,
                execution_time_ms=execution_time_ms,
                row_count=len(df),
                query_sql=sql,
                timestamp=timestamp
            )

            if track_history:
                self.query_history.append(result)

            return result

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            raise QueryExecutionError(
                f"Query failed after {execution_time_ms:.2f}ms: {str(e)}"
            ) from e

    def execute_batch(
        self,
        queries: List[str],
        track_history: bool = True
    ) -> List[QueryResult]:
        """Execute batch of queries.

        Args:
            queries: List of SQL query strings
            track_history: Whether to store results in query history

        Returns:
            List of QueryResult objects
        """
        results = []

        for query in queries:
            result = self.execute(query, track_history=track_history)
            results.append(result)

        return results

    def execute_and_fetch_one(self, sql: str) -> Optional[Any]:
        """Execute query and fetch single value.

        Args:
            sql: SQL query string

        Returns:
            Single value from first row/column, or None
        """
        result = self.execute(sql, track_history=False)

        if result.data.empty:
            return None

        return result.data.iloc[0, 0]

    def execute_and_count(self, sql: str) -> int:
        """Execute query and return row count.

        Args:
            sql: SQL query string

        Returns:
            Number of rows returned
        """
        result = self.execute(sql, track_history=False)
        return result.row_count

    def clear_history(self) -> None:
        """Clear query execution history."""
        self.query_history = []

    def get_history(self, limit: Optional[int] = None) -> List[QueryResult]:
        """Get query execution history.

        Args:
            limit: Optional limit on number of results to return

        Returns:
            List of QueryResult objects (most recent first)
        """
        history = list(reversed(self.query_history))

        if limit:
            return history[:limit]

        return history

    def get_history_summary(self) -> Dict[str, Any]:
        """Get summary statistics for query history.

        Returns:
            Dictionary with execution statistics
        """
        if not self.query_history:
            return {
                'total_queries': 0,
                'avg_execution_time_ms': 0,
                'min_execution_time_ms': 0,
                'max_execution_time_ms': 0,
                'total_rows_returned': 0,
            }

        execution_times = [r.execution_time_ms for r in self.query_history]
        total_rows = sum(r.row_count for r in self.query_history)

        return {
            'total_queries': len(self.query_history),
            'avg_execution_time_ms': sum(execution_times) / len(execution_times),
            'min_execution_time_ms': min(execution_times),
            'max_execution_time_ms': max(execution_times),
            'total_rows_returned': total_rows,
        }

    def explain(self, sql: str) -> str:
        """Get query execution plan.

        Args:
            sql: SQL query string

        Returns:
            Query plan as string
        """
        explain_sql = f"EXPLAIN {sql}"
        result = self.execute(explain_sql, track_history=False)

        # Format explain output
        plan_lines = result.data.iloc[:, 0].tolist()
        return '\n'.join(plan_lines)

    def analyze(self, sql: str) -> Dict[str, Any]:
        """Analyze query with execution plan and timing.

        Args:
            sql: SQL query string

        Returns:
            Dictionary with plan and execution metadata
        """
        # Get query plan
        plan = self.explain(sql)

        # Execute query
        result = self.execute(sql, track_history=False)

        return {
            'query_plan': plan,
            'execution_time_ms': result.execution_time_ms,
            'row_count': result.row_count,
            'query_sql': sql,
        }


class QueryExecutionError(Exception):
    """Exception raised when query execution fails."""
    pass
