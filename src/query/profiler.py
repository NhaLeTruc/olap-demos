"""Query profiler for performance analysis and benchmarking.

This module provides detailed profiling of query execution,
including timing breakdowns, resource usage, and optimization hints.
"""

from typing import Dict, Any, List, Optional
import time
import json
from pathlib import Path
from datetime import datetime
from .executor import QueryExecutor


class QueryProfile:
    """Container for detailed query profiling information.

    Stores comprehensive execution metrics for performance
    analysis and optimization.
    """

    def __init__(
        self,
        query_name: str,
        query_sql: str,
        execution_time_ms: float,
        row_count: int,
        explain_plan: str,
        timestamp: datetime,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Initialize query profile.

        Args:
            query_name: Descriptive name for the query
            query_sql: SQL query string
            execution_time_ms: Total execution time in milliseconds
            row_count: Number of rows returned
            explain_plan: Query execution plan
            timestamp: Profile timestamp
            metadata: Optional additional metadata
        """
        self.query_name = query_name
        self.query_sql = query_sql
        self.execution_time_ms = execution_time_ms
        self.row_count = row_count
        self.explain_plan = explain_plan
        self.timestamp = timestamp
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary.

        Returns:
            Dictionary with all profile data
        """
        return {
            'query_name': self.query_name,
            'query_sql': self.query_sql,
            'execution_time_ms': self.execution_time_ms,
            'execution_time_s': self.execution_time_ms / 1000,
            'row_count': self.row_count,
            'explain_plan': self.explain_plan,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata,
        }

    def meets_sla(self, sla_ms: float) -> bool:
        """Check if execution time meets SLA.

        Args:
            sla_ms: SLA threshold in milliseconds

        Returns:
            True if execution time is within SLA
        """
        return self.execution_time_ms <= sla_ms


class QueryProfiler:
    """Profiler for analytical query performance.

    Provides detailed profiling, benchmarking, and performance
    analysis for OLAP queries.
    """

    def __init__(self, executor: QueryExecutor):
        """Initialize query profiler.

        Args:
            executor: Query executor instance
        """
        self.executor = executor
        self.profiles: List[QueryProfile] = []

    def profile_query(
        self,
        query_name: str,
        sql: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> QueryProfile:
        """Profile a single query execution.

        Args:
            query_name: Descriptive name for the query
            sql: SQL query string
            metadata: Optional metadata to attach to profile

        Returns:
            QueryProfile with execution details
        """
        timestamp = datetime.now()

        # Get execution plan
        explain_plan = self.executor.explain(sql)

        # Execute query with timing
        result = self.executor.execute(sql, track_history=False)

        # Create profile
        profile = QueryProfile(
            query_name=query_name,
            query_sql=sql,
            execution_time_ms=result.execution_time_ms,
            row_count=result.row_count,
            explain_plan=explain_plan,
            timestamp=timestamp,
            metadata=metadata
        )

        self.profiles.append(profile)

        return profile

    def benchmark_query(
        self,
        query_name: str,
        sql: str,
        num_runs: int = 3,
        warmup_runs: int = 1
    ) -> Dict[str, Any]:
        """Benchmark query with multiple runs.

        Args:
            query_name: Descriptive name for the query
            sql: SQL query string
            num_runs: Number of benchmark runs
            warmup_runs: Number of warmup runs (not counted)

        Returns:
            Dictionary with benchmark statistics
        """
        # Warmup runs
        for _ in range(warmup_runs):
            self.executor.execute(sql, track_history=False)

        # Benchmark runs
        execution_times = []
        row_counts = []

        for _ in range(num_runs):
            result = self.executor.execute(sql, track_history=False)
            execution_times.append(result.execution_time_ms)
            row_counts.append(result.row_count)

        # Calculate statistics
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
        p50_time = sorted(execution_times)[len(execution_times) // 2]
        p95_idx = int(len(execution_times) * 0.95)
        p95_time = sorted(execution_times)[p95_idx] if p95_idx < len(execution_times) else max_time

        # Get execution plan
        explain_plan = self.executor.explain(sql)

        benchmark = {
            'query_name': query_name,
            'num_runs': num_runs,
            'avg_execution_time_ms': avg_time,
            'min_execution_time_ms': min_time,
            'max_execution_time_ms': max_time,
            'p50_execution_time_ms': p50_time,
            'p95_execution_time_ms': p95_time,
            'row_count': row_counts[0],
            'execution_times': execution_times,
            'explain_plan': explain_plan,
            'timestamp': datetime.now().isoformat(),
        }

        return benchmark

    def compare_queries(
        self,
        queries: Dict[str, str],
        num_runs: int = 3
    ) -> Dict[str, Any]:
        """Compare performance of multiple queries.

        Args:
            queries: Dictionary mapping query names to SQL strings
            num_runs: Number of runs per query

        Returns:
            Dictionary with comparison results
        """
        results = {}

        for query_name, sql in queries.items():
            benchmark = self.benchmark_query(query_name, sql, num_runs)
            results[query_name] = benchmark

        # Add comparison summary
        fastest = min(results.items(), key=lambda x: x[1]['avg_execution_time_ms'])
        slowest = max(results.items(), key=lambda x: x[1]['avg_execution_time_ms'])

        comparison = {
            'queries': results,
            'summary': {
                'fastest_query': fastest[0],
                'fastest_time_ms': fastest[1]['avg_execution_time_ms'],
                'slowest_query': slowest[0],
                'slowest_time_ms': slowest[1]['avg_execution_time_ms'],
                'speedup_factor': slowest[1]['avg_execution_time_ms'] / fastest[1]['avg_execution_time_ms'],
            }
        }

        return comparison

    def profile_storage_formats(
        self,
        query_sql: str,
        parquet_table: str,
        csv_table: str,
        num_runs: int = 3
    ) -> Dict[str, Any]:
        """Compare query performance across storage formats.

        Args:
            query_sql: Template SQL with {table} placeholder
            parquet_table: Name of Parquet-backed table
            csv_table: Name of CSV-backed table
            num_runs: Number of benchmark runs

        Returns:
            Dictionary with format comparison results
        """
        # Benchmark Parquet
        parquet_sql = query_sql.replace('{table}', parquet_table)
        parquet_bench = self.benchmark_query(
            f"{parquet_table}_query",
            parquet_sql,
            num_runs
        )

        # Benchmark CSV
        csv_sql = query_sql.replace('{table}', csv_table)
        csv_bench = self.benchmark_query(
            f"{csv_table}_query",
            csv_sql,
            num_runs
        )

        # Calculate speedup
        speedup = csv_bench['avg_execution_time_ms'] / parquet_bench['avg_execution_time_ms']

        return {
            'parquet': parquet_bench,
            'csv': csv_bench,
            'speedup_factor': speedup,
            'faster_format': 'parquet' if speedup > 1 else 'csv',
            'time_saved_ms': csv_bench['avg_execution_time_ms'] - parquet_bench['avg_execution_time_ms'],
        }

    def validate_partition_pruning(
        self,
        full_scan_sql: str,
        partitioned_sql: str,
        num_runs: int = 3
    ) -> Dict[str, Any]:
        """Validate partition pruning effectiveness.

        Args:
            full_scan_sql: SQL query without partition filter
            partitioned_sql: SQL query with partition filter
            num_runs: Number of benchmark runs

        Returns:
            Dictionary with pruning effectiveness metrics
        """
        # Benchmark full scan
        full_bench = self.benchmark_query("full_scan", full_scan_sql, num_runs)

        # Benchmark with partition pruning
        pruned_bench = self.benchmark_query("partition_pruned", partitioned_sql, num_runs)

        # Calculate improvement
        speedup = full_bench['avg_execution_time_ms'] / pruned_bench['avg_execution_time_ms']

        return {
            'full_scan': full_bench,
            'partition_pruned': pruned_bench,
            'speedup_factor': speedup,
            'time_saved_ms': full_bench['avg_execution_time_ms'] - pruned_bench['avg_execution_time_ms'],
            'pruning_effective': speedup > 1.5,  # At least 50% improvement
        }

    def export_profiles(self, output_path: Path) -> None:
        """Export all profiles to JSON file.

        Args:
            output_path: Path for output JSON file
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        profiles_data = [p.to_dict() for p in self.profiles]

        with open(output_path, 'w') as f:
            json.dump(profiles_data, f, indent=2)

    def get_profiles_by_name(self, query_name: str) -> List[QueryProfile]:
        """Get all profiles for a specific query.

        Args:
            query_name: Name of the query

        Returns:
            List of QueryProfile objects
        """
        return [p for p in self.profiles if p.query_name == query_name]

    def get_slowest_queries(self, limit: int = 10) -> List[QueryProfile]:
        """Get slowest query executions.

        Args:
            limit: Maximum number of results

        Returns:
            List of QueryProfile objects sorted by execution time
        """
        sorted_profiles = sorted(
            self.profiles,
            key=lambda p: p.execution_time_ms,
            reverse=True
        )

        return sorted_profiles[:limit]

    def clear_profiles(self) -> None:
        """Clear all stored profiles."""
        self.profiles = []
