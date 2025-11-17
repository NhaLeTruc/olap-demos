"""Benchmarking CLI command.

This module provides the command-line interface for benchmarking
OLAP query performance across different storage formats.
"""

import click
from pathlib import Path
import json

from src.query.duckdb_loader import DuckDBLoader
from src.query.connection import ConnectionManager
from src.query.executor import QueryExecutor
from src.query.profiler import QueryProfiler
from src.query.formatter import ResultFormatter


@click.command()
@click.option(
    '--parquet-path',
    type=click.Path(exists=True, path_type=Path),
    default=Path('data/parquet'),
    help='Path to Parquet data files'
)
@click.option(
    '--csv-path',
    type=click.Path(exists=True, path_type=Path),
    default=Path('data/csv'),
    help='Path to CSV data files'
)
@click.option(
    '--db-path',
    type=click.Path(path_type=Path),
    default=Path('data/duckdb/olap_demo.db'),
    help='Path to DuckDB database file'
)
@click.option(
    '--num-runs',
    type=int,
    default=3,
    help='Number of benchmark runs per query'
)
@click.option(
    '--format-comparison/--no-format-comparison',
    default=True,
    help='Compare Parquet vs CSV performance'
)
@click.option(
    '--partition-pruning/--no-partition-pruning',
    default=True,
    help='Validate partition pruning effectiveness'
)
@click.option(
    '--output-json',
    type=click.Path(path_type=Path),
    help='Export results to JSON file'
)
@click.option(
    '--verbose',
    is_flag=True,
    help='Enable verbose output'
)
def main(
    parquet_path: Path,
    csv_path: Path,
    db_path: Path,
    num_runs: int,
    format_comparison: bool,
    partition_pruning: bool,
    output_json: Path,
    verbose: bool
):
    """Benchmark OLAP query performance.

    Executes analytical queries against Parquet and CSV datasets,
    measures performance, and validates partition pruning effectiveness.
    """
    click.echo("=" * 80)
    click.echo("OLAP Query Benchmarking")
    click.echo("=" * 80)
    click.echo("")

    # Initialize DuckDB
    click.echo("Initializing DuckDB...")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    loader = DuckDBLoader(db_path)
    loader.connect()

    # Load Parquet data
    click.echo("Loading Parquet data...")
    dimension_tables = ['dim_time', 'dim_geography', 'dim_product', 'dim_customer', 'dim_payment']

    row_counts = loader.bulk_load_star_schema(
        parquet_path,
        dimension_tables,
        'fact_sales'
    )

    for table, count in row_counts.items():
        click.echo(f"  ✓ {table}: {count:,} rows")

    click.echo("")

    # Load CSV data if comparing formats
    if format_comparison:
        click.echo("Loading CSV data...")

        csv_loader = DuckDBLoader(loader.connection)

        for dim_table in dimension_tables:
            csv_path_table = csv_path / dim_table / f"{dim_table}.csv"
            if csv_path_table.exists():
                csv_loader.load_csv(f"{dim_table}_csv", csv_path_table)
                if verbose:
                    click.echo(f"  ✓ {dim_table}_csv")

        # Load CSV fact table
        csv_fact_path = csv_path / 'fact_sales'
        if csv_fact_path.exists():
            csv_loader.load_csv('fact_sales_csv', csv_fact_path, header=True)
            click.echo(f"  ✓ fact_sales_csv")

        click.echo("")

    # Initialize query infrastructure
    conn_manager = ConnectionManager(db_path)
    executor = QueryExecutor(conn_manager)
    profiler = QueryProfiler(executor)

    all_results = {}

    # Benchmark 1: Multi-dimensional aggregation
    click.echo("Benchmark 1: Multi-dimensional Aggregation")
    click.echo("-" * 80)

    agg_query = """
    SELECT
        dt.year,
        dt.quarter,
        dg.country,
        dp.category,
        SUM(fs.revenue) as total_revenue,
        SUM(fs.profit) as total_profit,
        COUNT(*) as transaction_count
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    JOIN dim_geography dg ON fs.geo_key = dg.geo_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    GROUP BY dt.year, dt.quarter, dg.country, dp.category
    ORDER BY dt.year, dt.quarter, total_revenue DESC
    """

    parquet_agg_bench = profiler.benchmark_query(
        "multi_dimensional_aggregation_parquet",
        agg_query,
        num_runs=num_runs
    )

    click.echo(ResultFormatter.format_benchmark_result(parquet_agg_bench))
    click.echo("")

    all_results['multi_dimensional_aggregation'] = parquet_agg_bench

    # Benchmark 2: Drill-down query
    click.echo("Benchmark 2: Drill-down Analysis")
    click.echo("-" * 80)

    drilldown_query = """
    SELECT
        dt.year,
        dt.quarter,
        dt.month_name,
        SUM(fs.revenue) as monthly_revenue,
        AVG(fs.revenue) as avg_transaction_revenue,
        COUNT(DISTINCT fs.customer_key) as unique_customers
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    WHERE dt.year = 2024
    GROUP BY dt.year, dt.quarter, dt.month, dt.month_name
    ORDER BY dt.month
    """

    parquet_drill_bench = profiler.benchmark_query(
        "drilldown_analysis_parquet",
        drilldown_query,
        num_runs=num_runs
    )

    click.echo(ResultFormatter.format_benchmark_result(parquet_drill_bench))
    click.echo("")

    all_results['drilldown_analysis'] = parquet_drill_bench

    # Benchmark 3: Window functions
    click.echo("Benchmark 3: Window Functions")
    click.echo("-" * 80)

    window_query = """
    SELECT
        dp.category,
        dp.product_name,
        SUM(fs.revenue) as product_revenue,
        RANK() OVER (PARTITION BY dp.category ORDER BY SUM(fs.revenue) DESC) as revenue_rank
    FROM fact_sales fs
    JOIN dim_product dp ON fs.product_key = dp.product_key
    WHERE dp.is_current = true
    GROUP BY dp.category, dp.product_name
    QUALIFY revenue_rank <= 10
    ORDER BY dp.category, revenue_rank
    """

    parquet_window_bench = profiler.benchmark_query(
        "window_functions_parquet",
        window_query,
        num_runs=num_runs
    )

    click.echo(ResultFormatter.format_benchmark_result(parquet_window_bench))
    click.echo("")

    all_results['window_functions'] = parquet_window_bench

    # Partition pruning validation
    if partition_pruning:
        click.echo("Benchmark 4: Partition Pruning Validation")
        click.echo("-" * 80)

        full_scan = """
        SELECT
            SUM(revenue) as total_revenue,
            COUNT(*) as record_count
        FROM fact_sales
        """

        pruned_scan = """
        SELECT
            SUM(revenue) as total_revenue,
            COUNT(*) as record_count
        FROM fact_sales
        WHERE year = 2024 AND quarter = 'Q1'
        """

        pruning_result = profiler.validate_partition_pruning(
            full_scan,
            pruned_scan,
            num_runs=num_runs
        )

        click.echo(ResultFormatter.format_partition_pruning(pruning_result))
        click.echo("")

        all_results['partition_pruning'] = pruning_result

    # Format comparison
    if format_comparison:
        click.echo("Benchmark 5: Storage Format Comparison")
        click.echo("-" * 80)

        comparison_query = """
        SELECT
            dt.year,
            dp.category,
            SUM(fs.revenue) as total_revenue
        FROM {table} fs
        JOIN dim_time dt ON fs.time_key = dt.time_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
        GROUP BY dt.year, dp.category
        """

        format_result = profiler.profile_storage_formats(
            comparison_query,
            'fact_sales',
            'fact_sales_csv',
            num_runs=num_runs
        )

        click.echo(ResultFormatter.format_storage_comparison(format_result))
        click.echo("")

        all_results['storage_format_comparison'] = format_result

    # Summary
    click.echo("=" * 80)
    click.echo("Benchmark Summary")
    click.echo("=" * 80)
    click.echo("")

    # Calculate aggregate statistics
    all_p95_times = []
    for benchmark_name, result in all_results.items():
        if 'p95_execution_time_ms' in result:
            all_p95_times.append(result['p95_execution_time_ms'])

    if all_p95_times:
        avg_p95 = sum(all_p95_times) / len(all_p95_times)
        max_p95 = max(all_p95_times)
        sla_ms = 5000  # 5 seconds

        click.echo(f"Average P95 Latency: {ResultFormatter.format_execution_time(avg_p95)}")
        click.echo(f"Maximum P95 Latency: {ResultFormatter.format_execution_time(max_p95)}")
        click.echo(f"SLA Target: {ResultFormatter.format_execution_time(sla_ms)}")

        meets_sla = max_p95 <= sla_ms
        click.echo(f"SLA Status: {'✓ PASS' if meets_sla else '✗ FAIL'}")
        click.echo("")

    # Export to JSON if requested
    if output_json:
        output_json.parent.mkdir(parents=True, exist_ok=True)

        with open(output_json, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)

        click.echo(f"Results exported to: {output_json}")
        click.echo("")

    # Cleanup
    loader.disconnect()

    click.echo("Benchmarking complete!")


if __name__ == '__main__':
    main()
