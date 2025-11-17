"""Analysis CLI command.

This module provides the command-line interface for exploring
and analyzing OLAP datasets with ad-hoc queries.
"""

import click
from pathlib import Path

from src.query.duckdb_loader import DuckDBLoader
from src.query.connection import ConnectionManager
from src.query.executor import QueryExecutor
from src.query.formatter import ResultFormatter
from src.storage.parquet_handler import ParquetHandler
from src.storage.partition_manager import PartitionManager


@click.command()
@click.option(
    '--data-path',
    type=click.Path(exists=True, path_type=Path),
    default=Path('data/parquet'),
    help='Path to data files (Parquet or CSV)'
)
@click.option(
    '--db-path',
    type=click.Path(path_type=Path),
    default=Path('data/duckdb/olap_demo.db'),
    help='Path to DuckDB database file'
)
@click.option(
    '--query',
    type=str,
    help='SQL query to execute (or use --interactive)'
)
@click.option(
    '--interactive',
    is_flag=True,
    help='Enter interactive query mode'
)
@click.option(
    '--list-tables',
    is_flag=True,
    help='List all available tables'
)
@click.option(
    '--describe',
    type=str,
    help='Describe table schema'
)
@click.option(
    '--sample',
    type=str,
    help='Show sample records from table'
)
@click.option(
    '--partitions',
    type=str,
    help='Show partitions for table'
)
@click.option(
    '--max-rows',
    type=int,
    default=20,
    help='Maximum rows to display'
)
@click.option(
    '--profile',
    type=str,
    help='Show execution plan and profile for query'
)
def main(
    data_path: Path,
    db_path: Path,
    query: str,
    interactive: bool,
    list_tables: bool,
    describe: str,
    sample: str,
    partitions: str,
    max_rows: int,
    profile: str
):
    """Analyze OLAP dataset with ad-hoc queries.

    Provides interactive exploration and analysis of dimensional
    and fact data using SQL queries.
    """
    click.echo("=" * 80)
    click.echo("OLAP Data Analysis")
    click.echo("=" * 80)
    click.echo("")

    # Initialize DuckDB
    db_path.parent.mkdir(parents=True, exist_ok=True)

    loader = DuckDBLoader(db_path)
    loader.connect()

    # Load data if database is empty
    tables = loader.list_tables()

    if not tables:
        click.echo("Loading data into DuckDB...")

        dimension_tables = ['dim_time', 'dim_geography', 'dim_product', 'dim_customer', 'dim_payment']

        row_counts = loader.bulk_load_star_schema(
            data_path,
            dimension_tables,
            'fact_sales'
        )

        for table, count in row_counts.items():
            click.echo(f"  ✓ {table}: {count:,} rows")

        click.echo("")

    # Initialize query executor
    conn_manager = ConnectionManager(db_path)
    executor = QueryExecutor(conn_manager)

    # List tables
    if list_tables:
        click.echo("Available Tables:")
        click.echo("-" * 80)

        for table in loader.list_tables():
            metadata = loader.get_table_info(table)
            click.echo(f"\n{table}")
            click.echo(f"  Rows: {metadata['row_count']:,}")
            click.echo(f"  Columns: {metadata['num_columns']}")

        click.echo("")
        return

    # Describe table
    if describe:
        click.echo(f"Table Schema: {describe}")
        click.echo("-" * 80)
        click.echo("")

        metadata = loader.get_table_info(describe)
        click.echo(ResultFormatter.format_table_metadata(metadata))
        click.echo("")
        return

    # Sample records
    if sample:
        click.echo(f"Sample Records: {sample}")
        click.echo("-" * 80)
        click.echo("")

        sample_query = f"SELECT * FROM {sample} LIMIT {max_rows}"
        result = executor.execute(sample_query)

        click.echo(ResultFormatter.format_dataframe(result.data, max_rows=max_rows))
        click.echo("")
        click.echo(f"Execution Time: {ResultFormatter.format_execution_time(result.execution_time_ms)}")
        click.echo("")
        return

    # Show partitions
    if partitions:
        click.echo(f"Partitions: {partitions}")
        click.echo("-" * 80)
        click.echo("")

        partition_stats = PartitionManager.get_partition_statistics(data_path, partitions)

        click.echo(f"Total Partitions: {partition_stats['num_partitions']}")
        click.echo(f"Partition Keys: {', '.join(partition_stats['partition_keys'])}")
        click.echo("")

        if partition_stats['partitions']:
            click.echo("Partition List:")
            for partition in partition_stats['partitions']:
                partition_str = ', '.join([f"{k}={v}" for k, v in partition.items()])
                click.echo(f"  - {partition_str}")

        click.echo("")
        return

    # Profile query execution
    if profile:
        click.echo("Query Profiling:")
        click.echo("-" * 80)
        click.echo(profile)
        click.echo("")

        # Get execution plan
        explain_query = f"EXPLAIN ANALYZE {profile}"
        plan_result = executor.execute(explain_query)

        click.echo("Execution Plan:")
        click.echo("-" * 80)
        for line in plan_result.data.iloc[:, 0]:
            # Highlight window functions
            if 'WINDOW' in line:
                click.echo(click.style(line, fg='yellow', bold=True))
            # Highlight partition filters
            elif 'Filters:' in line or 'partition' in line.lower():
                click.echo(click.style(line, fg='green', bold=True))
            else:
                click.echo(line)
        click.echo("")

        # Execute query
        result = executor.execute(profile)

        click.echo("Results:")
        click.echo("-" * 80)
        click.echo(ResultFormatter.format_dataframe(result.data, max_rows=max_rows))
        click.echo("")
        click.echo(f"Rows: {result.row_count}")
        click.echo(f"Execution Time: {ResultFormatter.format_execution_time(result.execution_time_ms)}")
        click.echo("")
        return

    # Execute single query
    if query:
        click.echo("Executing Query:")
        click.echo("-" * 80)
        click.echo(query)
        click.echo("")

        result = executor.execute(query)

        click.echo("Results:")
        click.echo("-" * 80)
        click.echo(ResultFormatter.format_dataframe(result.data, max_rows=max_rows))
        click.echo("")
        click.echo(f"Rows: {result.row_count}")
        click.echo(f"Execution Time: {ResultFormatter.format_execution_time(result.execution_time_ms)}")
        click.echo("")
        return

    # Interactive mode
    if interactive:
        click.echo("Interactive Query Mode")
        click.echo("-" * 80)
        click.echo("Enter SQL queries (type 'exit' or 'quit' to exit)")
        click.echo("Commands: .tables, .schema <table>, .sample <table>, .partitions <table>")
        click.echo("")

        while True:
            try:
                user_query = click.prompt("SQL>", type=str)

                if user_query.lower() in ['exit', 'quit']:
                    break

                # Handle special commands
                if user_query == '.tables':
                    for table in loader.list_tables():
                        click.echo(f"  {table}")
                    click.echo("")
                    continue

                if user_query.startswith('.schema '):
                    table_name = user_query.split(' ', 1)[1].strip()
                    metadata = loader.get_table_info(table_name)
                    click.echo(ResultFormatter.format_table_metadata(metadata))
                    click.echo("")
                    continue

                if user_query.startswith('.sample '):
                    table_name = user_query.split(' ', 1)[1].strip()
                    sample_query = f"SELECT * FROM {table_name} LIMIT {max_rows}"
                    result = executor.execute(sample_query)
                    click.echo(ResultFormatter.format_dataframe(result.data, max_rows=max_rows))
                    click.echo("")
                    continue

                if user_query.startswith('.partitions '):
                    table_name = user_query.split(' ', 1)[1].strip()
                    partition_stats = PartitionManager.get_partition_statistics(data_path, table_name)
                    click.echo(f"Partitions: {partition_stats['num_partitions']}")
                    for partition in partition_stats['partitions'][:10]:
                        partition_str = ', '.join([f"{k}={v}" for k, v in partition.items()])
                        click.echo(f"  {partition_str}")
                    click.echo("")
                    continue

                # Execute SQL query
                result = executor.execute(user_query)

                click.echo(ResultFormatter.format_dataframe(result.data, max_rows=max_rows))
                click.echo("")
                click.echo(f"Rows: {result.row_count} | "
                          f"Time: {ResultFormatter.format_execution_time(result.execution_time_ms)}")
                click.echo("")

            except KeyboardInterrupt:
                click.echo("\nInterrupted. Type 'exit' to quit.")
            except Exception as e:
                click.echo(f"Error: {str(e)}")
                click.echo("")

        click.echo("Exiting interactive mode.")
        return

    # No options provided, show help
    click.echo("No analysis options provided. Use --help for usage information.")
    click.echo("")
    click.echo("Quick Start Examples:")
    click.echo("  List tables:          olap-analyze --list-tables")
    click.echo("  Describe table:       olap-analyze --describe dim_product")
    click.echo("  Sample data:          olap-analyze --sample fact_sales")
    click.echo("  Run query:            olap-analyze --query 'SELECT * FROM dim_time LIMIT 10'")
    click.echo("  Interactive mode:     olap-analyze --interactive")
    click.echo("")

    loader.disconnect()


if __name__ == '__main__':
    main()
