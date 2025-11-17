"""Data generation CLI command.

This module provides the command-line interface for generating
synthetic OLAP datasets in Parquet and CSV formats.
"""

import click
from pathlib import Path
from datetime import date, timedelta
import pandas as pd

from src.datagen.generator import (
    generate_dim_time,
    generate_dim_geography,
    generate_dim_product,
    generate_dim_customer,
    generate_dim_payment,
    generate_sales_fact,
)
from src.datagen.schemas import (
    validate_schema,
    check_referential_integrity,
)
from src.storage.parquet_handler import ParquetHandler
from src.storage.csv_handler import CSVHandler
from src.storage.partition_manager import PartitionManager


@click.command()
@click.option(
    '--output-format',
    type=click.Choice(['parquet', 'csv', 'both'], case_sensitive=False),
    default='both',
    help='Output storage format'
)
@click.option(
    '--num-transactions',
    type=int,
    default=100000,
    help='Number of transactions to generate'
)
@click.option(
    '--num-customers',
    type=int,
    default=10000,
    help='Number of customers to generate'
)
@click.option(
    '--num-products',
    type=int,
    default=1000,
    help='Number of products to generate'
)
@click.option(
    '--parquet-path',
    type=click.Path(path_type=Path),
    default=Path('data/parquet'),
    help='Output path for Parquet files'
)
@click.option(
    '--csv-path',
    type=click.Path(path_type=Path),
    default=Path('data/csv'),
    help='Output path for CSV files'
)
@click.option(
    '--seed',
    type=int,
    default=42,
    help='Random seed for reproducibility'
)
@click.option(
    '--validate/--no-validate',
    default=True,
    help='Validate generated data'
)
@click.option(
    '--verbose',
    is_flag=True,
    help='Enable verbose output'
)
def main(
    output_format: str,
    num_transactions: int,
    num_customers: int,
    num_products: int,
    parquet_path: Path,
    csv_path: Path,
    seed: int,
    validate: bool,
    verbose: bool
):
    """Generate synthetic OLAP dataset with star schema.

    Creates dimension and fact tables with realistic e-commerce sales data.
    Supports Parquet (columnar) and CSV (row-based) output formats.
    """
    click.echo("=" * 80)
    click.echo("OLAP Data Generation")
    click.echo("=" * 80)
    click.echo("")

    # Display configuration
    click.echo("Configuration:")
    click.echo(f"  Output Format: {output_format.upper()}")
    click.echo(f"  Transactions: {num_transactions:,}")
    click.echo(f"  Customers: {num_customers:,}")
    click.echo(f"  Products: {num_products:,}")
    click.echo(f"  Random Seed: {seed}")
    click.echo(f"  Validation: {'Enabled' if validate else 'Disabled'}")
    click.echo("")

    # Generate dimension tables
    click.echo("Generating dimension tables...")
    click.echo("")

    # Time dimension (3 years)
    if verbose:
        click.echo("  - Generating time dimension...")
    end_date = date.today()
    start_date = end_date - timedelta(days=3*365)
    dim_time = generate_dim_time(start_date, end_date, seed)
    click.echo(f"  ✓ Time dimension: {len(dim_time):,} records")

    # Geography dimension
    if verbose:
        click.echo("  - Generating geography dimension...")
    dim_geography = generate_dim_geography(
        num_countries=3,
        num_regions_per_country=5,
        num_cities_per_region=10,
        seed=seed
    )
    click.echo(f"  ✓ Geography dimension: {len(dim_geography):,} records")

    # Product dimension (with SCD Type 2)
    if verbose:
        click.echo("  - Generating product dimension...")
    dim_product = generate_dim_product(
        num_products=num_products,
        change_rate=0.1,
        seed=seed
    )
    click.echo(f"  ✓ Product dimension: {len(dim_product):,} records")

    # Customer dimension
    if verbose:
        click.echo("  - Generating customer dimension...")
    dim_customer = generate_dim_customer(num_customers, seed)
    click.echo(f"  ✓ Customer dimension: {len(dim_customer):,} records")

    # Payment dimension
    if verbose:
        click.echo("  - Generating payment dimension...")
    dim_payment = generate_dim_payment(seed)
    click.echo(f"  ✓ Payment dimension: {len(dim_payment):,} records")

    click.echo("")

    # Generate fact table
    click.echo("Generating fact table...")
    if verbose:
        click.echo("  - Generating sales transactions...")

    fact_sales = generate_sales_fact(
        num_transactions=num_transactions,
        time_df=dim_time,
        geo_df=dim_geography,
        product_df=dim_product,
        customer_df=dim_customer,
        payment_df=dim_payment,
        seed=seed
    )
    click.echo(f"  ✓ Sales fact: {len(fact_sales):,} records")
    click.echo("")

    # Validate data if requested
    if validate:
        click.echo("Validating data integrity...")

        # Check referential integrity
        dimension_dfs = {
            'dim_time': dim_time,
            'dim_geography': dim_geography,
            'dim_product': dim_product,
            'dim_customer': dim_customer,
            'dim_payment': dim_payment,
        }

        foreign_keys = {
            'time_key': 'time_key',
            'geo_key': 'geo_key',
            'product_key': 'product_key',
            'customer_key': 'customer_key',
            'payment_key': 'payment_key',
        }

        integrity_result = check_referential_integrity(
            fact_sales,
            dimension_dfs,
            foreign_keys
        )

        if integrity_result['valid']:
            click.echo("  ✓ Referential integrity validated")
        else:
            click.echo("  ✗ Referential integrity violations found:")
            for fk, count in integrity_result['orphan_counts'].items():
                click.echo(f"    - {fk}: {count} orphan records")

        click.echo("")

    # Add partition columns to fact table
    fact_sales_partitioned = PartitionManager.add_partition_columns(
        fact_sales,
        date_column='transaction_date'
    )

    # Write Parquet files
    if output_format in ['parquet', 'both']:
        click.echo("Writing Parquet files...")

        parquet_handler = ParquetHandler(parquet_path)

        # Write dimensions (unpartitioned)
        parquet_handler.write(dim_time, 'dim_time')
        click.echo("  ✓ dim_time.parquet")

        parquet_handler.write(dim_geography, 'dim_geography')
        click.echo("  ✓ dim_geography.parquet")

        parquet_handler.write(dim_product, 'dim_product')
        click.echo("  ✓ dim_product.parquet")

        parquet_handler.write(dim_customer, 'dim_customer')
        click.echo("  ✓ dim_customer.parquet")

        parquet_handler.write(dim_payment, 'dim_payment')
        click.echo("  ✓ dim_payment.parquet")

        # Write fact table (partitioned by year/quarter)
        parquet_handler.write_partitioned(
            fact_sales_partitioned,
            'fact_sales',
            partition_cols=['year', 'quarter']
        )
        click.echo("  ✓ fact_sales (partitioned by year/quarter)")

        # Get compression statistics
        compression_ratio = parquet_handler.estimate_compression_ratio(
            fact_sales.head(10000),
            'fact_sales_sample'
        )
        click.echo(f"  Estimated compression ratio: {compression_ratio:.2f}:1")
        click.echo("")

    # Write CSV files
    if output_format in ['csv', 'both']:
        click.echo("Writing CSV files...")

        csv_handler = CSVHandler(csv_path)

        # Write dimensions
        csv_handler.write(dim_time, 'dim_time')
        click.echo("  ✓ dim_time.csv")

        csv_handler.write(dim_geography, 'dim_geography')
        click.echo("  ✓ dim_geography.csv")

        csv_handler.write(dim_product, 'dim_product')
        click.echo("  ✓ dim_product.csv")

        csv_handler.write(dim_customer, 'dim_customer')
        click.echo("  ✓ dim_customer.csv")

        csv_handler.write(dim_payment, 'dim_payment')
        click.echo("  ✓ dim_payment.csv")

        # Write fact table (partitioned for comparison)
        csv_handler.write_partitioned(
            fact_sales_partitioned,
            'fact_sales',
            partition_cols=['year', 'quarter']
        )
        click.echo("  ✓ fact_sales (partitioned by year/quarter)")
        click.echo("")

    # Summary
    click.echo("=" * 80)
    click.echo("Data Generation Complete")
    click.echo("=" * 80)
    click.echo("")

    click.echo("Summary:")
    click.echo(f"  Total Dimensions: 5")
    click.echo(f"  Total Dimension Records: {len(dim_time) + len(dim_geography) + len(dim_product) + len(dim_customer) + len(dim_payment):,}")
    click.echo(f"  Total Fact Records: {len(fact_sales):,}")

    if output_format in ['parquet', 'both']:
        click.echo(f"  Parquet Output: {parquet_path}")

    if output_format in ['csv', 'both']:
        click.echo(f"  CSV Output: {csv_path}")

    click.echo("")
    click.echo("Next Steps:")
    click.echo("  1. Run 'olap-benchmark' to measure query performance")
    click.echo("  2. Run 'olap-analyze' to explore the data")


if __name__ == '__main__':
    main()
