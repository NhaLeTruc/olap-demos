"""Query engine infrastructure for OLAP operations.

This module provides DuckDB-based query execution, profiling,
and result formatting for analytical workloads.
"""

from .duckdb_loader import DuckDBLoader
from .connection import ConnectionManager
from .executor import QueryExecutor
from .profiler import QueryProfiler
from .formatter import ResultFormatter

__all__ = [
    "DuckDBLoader",
    "ConnectionManager",
    "QueryExecutor",
    "QueryProfiler",
    "ResultFormatter",
]
