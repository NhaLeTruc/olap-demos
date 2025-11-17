"""Pytest configuration and shared fixtures.

This module provides reusable fixtures for testing data generation,
storage, and query operations.
"""

import pytest
from pathlib import Path
from datetime import date, timedelta
import tempfile
import shutil
import pandas as pd

from src.datagen.generator import (
    generate_dim_time,
    generate_dim_geography,
    generate_dim_product,
    generate_dim_customer,
    generate_dim_payment,
    generate_sales_fact,
)
from src.storage.parquet_handler import ParquetHandler
from src.storage.csv_handler import CSVHandler
from src.query.duckdb_loader import DuckDBLoader
from src.query.connection import ConnectionManager
from src.query.executor import QueryExecutor
from src.query.profiler import QueryProfiler


# Test configuration
SEED = 42
SMALL_DATASET_SIZE = 100  # For unit tests
MEDIUM_DATASET_SIZE = 1000  # For integration tests


@pytest.fixture(scope="session")
def test_seed():
    """Provide consistent random seed for all tests."""
    return SEED


@pytest.fixture(scope="session")
def temp_dir():
    """Create temporary directory for test data."""
    temp_path = Path(tempfile.mkdtemp(prefix="olap_test_"))
    yield temp_path

    # Cleanup after all tests
    if temp_path.exists():
        shutil.rmtree(temp_path)


@pytest.fixture(scope="session")
def sample_dim_time(test_seed):
    """Generate small time dimension for testing."""
    end_date = date.today()
    start_date = end_date - timedelta(days=365)  # 1 year
    return generate_dim_time(start_date, end_date, test_seed)


@pytest.fixture(scope="session")
def sample_dim_geography(test_seed):
    """Generate small geography dimension for testing."""
    return generate_dim_geography(
        num_countries=2,
        num_regions_per_country=3,
        num_cities_per_region=5,
        seed=test_seed
    )


@pytest.fixture(scope="session")
def sample_dim_product(test_seed):
    """Generate small product dimension for testing."""
    return generate_dim_product(
        num_products=50,
        change_rate=0.1,
        seed=test_seed
    )


@pytest.fixture(scope="session")
def sample_dim_customer(test_seed):
    """Generate small customer dimension for testing."""
    return generate_dim_customer(500, test_seed)


@pytest.fixture(scope="session")
def sample_dim_payment(test_seed):
    """Generate payment dimension for testing."""
    return generate_dim_payment(test_seed)


@pytest.fixture(scope="session")
def sample_fact_sales(
    test_seed,
    sample_dim_time,
    sample_dim_geography,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_payment
):
    """Generate small sales fact table for testing."""
    return generate_sales_fact(
        num_transactions=SMALL_DATASET_SIZE,
        time_df=sample_dim_time,
        geo_df=sample_dim_geography,
        product_df=sample_dim_product,
        customer_df=sample_dim_customer,
        payment_df=sample_dim_payment,
        seed=test_seed
    )


@pytest.fixture
def parquet_handler(temp_dir):
    """Create Parquet handler with temp directory."""
    parquet_path = temp_dir / "parquet"
    parquet_path.mkdir(exist_ok=True)
    return ParquetHandler(parquet_path)


@pytest.fixture
def csv_handler(temp_dir):
    """Create CSV handler with temp directory."""
    csv_path = temp_dir / "csv"
    csv_path.mkdir(exist_ok=True)
    return CSVHandler(csv_path)


@pytest.fixture
def duckdb_loader(temp_dir):
    """Create DuckDB loader with temp database."""
    db_path = temp_dir / "test.db"
    loader = DuckDBLoader(db_path)
    yield loader
    loader.disconnect()


@pytest.fixture
def connection_manager(temp_dir):
    """Create DuckDB connection manager with temp database."""
    db_path = temp_dir / "test.db"
    manager = ConnectionManager(db_path)
    yield manager
    manager.close()


@pytest.fixture
def query_executor(connection_manager):
    """Create query executor with connection manager."""
    return QueryExecutor(connection_manager)


@pytest.fixture
def query_profiler(query_executor):
    """Create query profiler with executor."""
    return QueryProfiler(query_executor)


@pytest.fixture
def loaded_duckdb(
    duckdb_loader,
    parquet_handler,
    sample_dim_time,
    sample_dim_geography,
    sample_dim_product,
    sample_dim_customer,
    sample_dim_payment,
    sample_fact_sales
):
    """Provide DuckDB loaded with test data."""
    # Write test data to Parquet
    parquet_handler.write(sample_dim_time, 'dim_time')
    parquet_handler.write(sample_dim_geography, 'dim_geography')
    parquet_handler.write(sample_dim_product, 'dim_product')
    parquet_handler.write(sample_dim_customer, 'dim_customer')
    parquet_handler.write(sample_dim_payment, 'dim_payment')
    parquet_handler.write(sample_fact_sales, 'fact_sales')

    # Load into DuckDB
    dimension_tables = ['dim_time', 'dim_geography', 'dim_product', 'dim_customer', 'dim_payment']

    duckdb_loader.bulk_load_star_schema(
        parquet_handler.base_path,
        dimension_tables,
        'fact_sales'
    )

    return duckdb_loader


# Benchmark fixtures
@pytest.fixture(scope="function")
def benchmark_dataframe():
    """Generate medium-sized DataFrame for benchmarks."""
    return pd.DataFrame({
        'id': range(MEDIUM_DATASET_SIZE),
        'value': [i * 2.5 for i in range(MEDIUM_DATASET_SIZE)],
        'category': [f'cat_{i % 10}' for i in range(MEDIUM_DATASET_SIZE)],
    })


@pytest.fixture
def benchmark_query_simple():
    """Simple aggregation query for benchmarking."""
    return "SELECT COUNT(*), SUM(revenue), AVG(profit) FROM fact_sales"


@pytest.fixture
def benchmark_query_complex():
    """Complex multi-join query for benchmarking."""
    return """
    SELECT
        dt.year,
        dg.country,
        dp.category,
        SUM(fs.revenue) as total_revenue
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    JOIN dim_geography dg ON fs.geo_key = dg.geo_key
    JOIN dim_product dp ON fs.product_key = dp.product_key
    GROUP BY dt.year, dg.country, dp.category
    """
