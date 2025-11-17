"""Result formatting and presentation utilities.

This module provides formatters for query results, making them
suitable for CLI display, reporting, and analysis.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
from tabulate import tabulate


class ResultFormatter:
    """Formatter for query results and analytics output.

    Provides formatted output for CLI display, reports,
    and analysis summaries.
    """

    @staticmethod
    def format_dataframe(
        df: pd.DataFrame,
        max_rows: int = 20,
        table_format: str = "grid"
    ) -> str:
        """Format DataFrame as ASCII table.

        Args:
            df: DataFrame to format
            max_rows: Maximum rows to display
            table_format: Table format (grid, simple, plain, etc.)

        Returns:
            Formatted table string
        """
        if df.empty:
            return "No results"

        # Limit rows
        display_df = df.head(max_rows)

        # Format with tabulate
        table = tabulate(
            display_df,
            headers='keys',
            tablefmt=table_format,
            showindex=False,
            floatfmt=".2f"
        )

        # Add row count footer
        if len(df) > max_rows:
            footer = f"\n\n[Showing {max_rows} of {len(df)} rows]"
            return table + footer

        return table

    @staticmethod
    def format_execution_time(time_ms: float) -> str:
        """Format execution time in human-readable format.

        Args:
            time_ms: Execution time in milliseconds

        Returns:
            Formatted time string
        """
        if time_ms < 1:
            return f"{time_ms * 1000:.2f} μs"
        elif time_ms < 1000:
            return f"{time_ms:.2f} ms"
        else:
            return f"{time_ms / 1000:.2f} s"

    @staticmethod
    def format_size_bytes(size_bytes: int) -> str:
        """Format file size in human-readable format.

        Args:
            size_bytes: Size in bytes

        Returns:
            Formatted size string (e.g., '1.5 GB', '256 MB')
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0

        return f"{size_bytes:.2f} PB"

    @staticmethod
    def format_query_result(result: Any, show_sql: bool = True) -> str:
        """Format complete query result with metadata.

        Args:
            result: QueryResult object
            show_sql: Whether to include SQL in output

        Returns:
            Formatted result string
        """
        output = []

        if show_sql:
            output.append("SQL Query:")
            output.append("-" * 80)
            output.append(result.query_sql)
            output.append("")

        output.append("Results:")
        output.append("-" * 80)
        output.append(ResultFormatter.format_dataframe(result.data))
        output.append("")

        output.append("Execution Metadata:")
        output.append(f"  Rows: {result.row_count}")
        output.append(f"  Execution Time: {ResultFormatter.format_execution_time(result.execution_time_ms)}")
        output.append(f"  Timestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")

        return "\n".join(output)

    @staticmethod
    def format_benchmark_result(benchmark: Dict[str, Any]) -> str:
        """Format benchmark results.

        Args:
            benchmark: Benchmark results dictionary

        Returns:
            Formatted benchmark report
        """
        output = []

        output.append(f"Benchmark: {benchmark['query_name']}")
        output.append("=" * 80)
        output.append("")

        output.append("Performance Metrics:")
        output.append(f"  Number of Runs: {benchmark['num_runs']}")
        output.append(f"  Average Time: {ResultFormatter.format_execution_time(benchmark['avg_execution_time_ms'])}")
        output.append(f"  Min Time: {ResultFormatter.format_execution_time(benchmark['min_execution_time_ms'])}")
        output.append(f"  Max Time: {ResultFormatter.format_execution_time(benchmark['max_execution_time_ms'])}")
        output.append(f"  P50 Time: {ResultFormatter.format_execution_time(benchmark['p50_execution_time_ms'])}")
        output.append(f"  P95 Time: {ResultFormatter.format_execution_time(benchmark['p95_execution_time_ms'])}")
        output.append(f"  Rows Returned: {benchmark['row_count']}")
        output.append("")

        # Individual run times
        output.append("Individual Run Times:")
        for idx, time_ms in enumerate(benchmark['execution_times'], 1):
            output.append(f"  Run {idx}: {ResultFormatter.format_execution_time(time_ms)}")

        return "\n".join(output)

    @staticmethod
    def format_comparison(comparison: Dict[str, Any]) -> str:
        """Format query comparison results.

        Args:
            comparison: Comparison results dictionary

        Returns:
            Formatted comparison report
        """
        output = []

        output.append("Query Performance Comparison")
        output.append("=" * 80)
        output.append("")

        # Summary
        summary = comparison['summary']
        output.append("Summary:")
        output.append(f"  Fastest Query: {summary['fastest_query']}")
        output.append(f"  Fastest Time: {ResultFormatter.format_execution_time(summary['fastest_time_ms'])}")
        output.append(f"  Slowest Query: {summary['slowest_query']}")
        output.append(f"  Slowest Time: {ResultFormatter.format_execution_time(summary['slowest_time_ms'])}")
        output.append(f"  Speedup Factor: {summary['speedup_factor']:.2f}x")
        output.append("")

        # Individual query details
        output.append("Query Details:")
        output.append("-" * 80)

        for query_name, benchmark in comparison['queries'].items():
            output.append(f"\n{query_name}:")
            output.append(f"  Avg Time: {ResultFormatter.format_execution_time(benchmark['avg_execution_time_ms'])}")
            output.append(f"  P95 Time: {ResultFormatter.format_execution_time(benchmark['p95_execution_time_ms'])}")
            output.append(f"  Rows: {benchmark['row_count']}")

        return "\n".join(output)

    @staticmethod
    def format_storage_comparison(comparison: Dict[str, Any]) -> str:
        """Format storage format comparison results.

        Args:
            comparison: Storage comparison results

        Returns:
            Formatted comparison report
        """
        output = []

        output.append("Storage Format Performance Comparison")
        output.append("=" * 80)
        output.append("")

        # Summary
        output.append("Summary:")
        output.append(f"  Faster Format: {comparison['faster_format'].upper()}")
        output.append(f"  Speedup Factor: {comparison['speedup_factor']:.2f}x")
        output.append(f"  Time Saved: {ResultFormatter.format_execution_time(comparison['time_saved_ms'])}")
        output.append("")

        # Parquet results
        parquet = comparison['parquet']
        output.append("Parquet Performance:")
        output.append(f"  Avg Time: {ResultFormatter.format_execution_time(parquet['avg_execution_time_ms'])}")
        output.append(f"  P95 Time: {ResultFormatter.format_execution_time(parquet['p95_execution_time_ms'])}")
        output.append(f"  Rows: {parquet['row_count']}")
        output.append("")

        # CSV results
        csv = comparison['csv']
        output.append("CSV Performance:")
        output.append(f"  Avg Time: {ResultFormatter.format_execution_time(csv['avg_execution_time_ms'])}")
        output.append(f"  P95 Time: {ResultFormatter.format_execution_time(csv['p95_execution_time_ms'])}")
        output.append(f"  Rows: {csv['row_count']}")

        return "\n".join(output)

    @staticmethod
    def format_partition_pruning(result: Dict[str, Any]) -> str:
        """Format partition pruning validation results.

        Args:
            result: Partition pruning validation results

        Returns:
            Formatted validation report
        """
        output = []

        output.append("Partition Pruning Validation")
        output.append("=" * 80)
        output.append("")

        output.append("Summary:")
        output.append(f"  Pruning Effective: {'Yes' if result['pruning_effective'] else 'No'}")
        output.append(f"  Speedup Factor: {result['speedup_factor']:.2f}x")
        output.append(f"  Time Saved: {ResultFormatter.format_execution_time(result['time_saved_ms'])}")
        output.append("")

        # Full scan results
        full = result['full_scan']
        output.append("Full Scan Performance:")
        output.append(f"  Avg Time: {ResultFormatter.format_execution_time(full['avg_execution_time_ms'])}")
        output.append(f"  P95 Time: {ResultFormatter.format_execution_time(full['p95_execution_time_ms'])}")
        output.append(f"  Rows: {full['row_count']}")
        output.append("")

        # Pruned scan results
        pruned = result['partition_pruned']
        output.append("Partition Pruned Performance:")
        output.append(f"  Avg Time: {ResultFormatter.format_execution_time(pruned['avg_execution_time_ms'])}")
        output.append(f"  P95 Time: {ResultFormatter.format_execution_time(pruned['p95_execution_time_ms'])}")
        output.append(f"  Rows: {pruned['row_count']}")

        return "\n".join(output)

    @staticmethod
    def format_table_metadata(metadata: Dict[str, Any]) -> str:
        """Format table metadata.

        Args:
            metadata: Table metadata dictionary

        Returns:
            Formatted metadata string
        """
        output = []

        output.append(f"Table: {metadata['table_name']}")
        output.append("=" * 80)
        output.append("")

        output.append("Statistics:")
        output.append(f"  Rows: {metadata['row_count']:,}")
        output.append(f"  Columns: {metadata['num_columns']}")

        if 'file_size_bytes' in metadata:
            output.append(f"  File Size: {ResultFormatter.format_size_bytes(metadata['file_size_bytes'])}")

        if 'estimated_size_bytes' in metadata and metadata['estimated_size_bytes']:
            output.append(f"  Estimated Size: {ResultFormatter.format_size_bytes(metadata['estimated_size_bytes'])}")

        output.append("")

        # Column details
        if 'columns' in metadata:
            output.append("Columns:")
            for col in metadata['columns']:
                if isinstance(col, dict):
                    output.append(f"  - {col['name']}: {col['type']}")
                else:
                    output.append(f"  - {col}")

        return "\n".join(output)

    @staticmethod
    def format_aggregation_result(
        df: pd.DataFrame,
        title: str,
        aggregation_type: str
    ) -> str:
        """Format aggregation query results.

        Args:
            df: Result DataFrame
            title: Title for the report
            aggregation_type: Type of aggregation (sum, avg, count, etc.)

        Returns:
            Formatted aggregation report
        """
        output = []

        output.append(title)
        output.append("=" * 80)
        output.append("")

        output.append(f"Aggregation Type: {aggregation_type}")
        output.append(f"Result Count: {len(df)} groups")
        output.append("")

        output.append("Results:")
        output.append("-" * 80)
        output.append(ResultFormatter.format_dataframe(df, max_rows=50))

        return "\n".join(output)
