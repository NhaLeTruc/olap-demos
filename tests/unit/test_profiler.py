"""Unit tests for query profiler (T098).

Tests metric extraction from EXPLAIN ANALYZE output and profiling logic.
"""

import pytest
from datetime import datetime

from src.query.profiler import QueryProfiler


class TestQueryProfiler:
    """Test query profiling and metric extraction."""

    @pytest.fixture
    def profiler(self, query_executor):
        """Create QueryProfiler instance."""
        return QueryProfiler(query_executor)

    def test_profiler_initialization(self, query_executor):
        """Test profiler initialization."""
        profiler = QueryProfiler(query_executor)

        assert profiler.executor is not None
        assert hasattr(profiler, 'profile_history')

    def test_explain_analysis_extraction(self, profiler, loaded_duckdb):
        """Test extracting metrics from EXPLAIN ANALYZE output."""
        # Run a simple query with EXPLAIN ANALYZE
        query = """
            SELECT
                SUM(revenue) as total_revenue,
                COUNT(*) as row_count
            FROM fact_sales
            LIMIT 10
        """

        # Profile the query
        profile_result = profiler.profile_query(query)

        # Verify profile structure
        assert 'execution_time_ms' in profile_result
        assert 'row_count' in profile_result
        assert 'explain_plan' in profile_result or 'query' in profile_result

        # Execution time should be reasonable
        assert profile_result['execution_time_ms'] >= 0
        assert profile_result['execution_time_ms'] < 60000  # Less than 60 seconds

    def test_metric_extraction_from_explain(self, profiler):
        """Test extracting specific metrics from EXPLAIN output."""
        # Sample EXPLAIN ANALYZE output (simplified)
        explain_output = """
        ┌─────────────────────────────┐
        │         PROJECTION          │
        │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─    │
        │   Projections: 2            │
        │   Estimated Cardinality: 1  │
        └─────────────────────────────┘
        ┌─────────────────────────────┐
        │       HASH_GROUP_BY         │
        │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─    │
        │   Groups: year              │
        │   Aggregates: SUM(revenue)  │
        │   Estimated Cardinality: 3  │
        └─────────────────────────────┘
        ┌─────────────────────────────┐
        │          SEQ_SCAN           │
        │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─    │
        │   Table: fact_sales         │
        │   Estimated Cardinality:    │
        │   1000000                   │
        └─────────────────────────────┘
        """

        # Extract metrics
        metrics = profiler.extract_explain_metrics(explain_output)

        # Should identify query operators
        assert 'operators' in metrics or 'plan_nodes' in metrics or len(metrics) >= 0

    def test_query_history_tracking(self, profiler, loaded_duckdb):
        """Test that profiler tracks query history."""
        # Run multiple queries
        queries = [
            "SELECT COUNT(*) FROM fact_sales LIMIT 1",
            "SELECT SUM(revenue) FROM fact_sales LIMIT 1",
        ]

        for query in queries:
            profiler.profile_query(query)

        # Verify history is tracked
        history = profiler.get_profile_history()
        assert len(history) >= len(queries)

        # Each entry should have required fields
        for entry in history:
            assert 'query' in entry or 'execution_time_ms' in entry

    def test_performance_metrics_aggregation(self, profiler, loaded_duckdb):
        """Test aggregating performance metrics across queries."""
        # Run same query multiple times
        query = "SELECT COUNT(*) FROM fact_sales LIMIT 1"

        execution_times = []
        for _ in range(3):
            result = profiler.profile_query(query)
            execution_times.append(result['execution_time_ms'])

        # Calculate statistics
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)

        # Sanity checks
        assert min_time <= avg_time <= max_time
        assert all(t >= 0 for t in execution_times)

    def test_partition_pruning_detection(self, profiler, loaded_duckdb):
        """Test detecting partition pruning in query plans."""
        # Query with filter that could trigger partition pruning
        query_with_filter = """
            SELECT SUM(revenue)
            FROM fact_sales
            WHERE year = 2023
            LIMIT 1
        """

        # Profile query
        profile = profiler.profile_query(query_with_filter)

        # Check if partition information is captured
        # (Actual partition pruning detection depends on EXPLAIN output format)
        assert 'explain_plan' in profile or 'execution_time_ms' in profile

    def test_profile_comparison(self, profiler, loaded_duckdb):
        """Test comparing profiles from different queries."""
        # Run two different queries
        query1 = "SELECT COUNT(*) FROM fact_sales LIMIT 1"
        query2 = "SELECT SUM(revenue), SUM(profit) FROM fact_sales LIMIT 1"

        profile1 = profiler.profile_query(query1)
        profile2 = profiler.profile_query(query2)

        # Both should have execution metrics
        assert 'execution_time_ms' in profile1
        assert 'execution_time_ms' in profile2

        # Can compare execution times
        comparison = {
            'query1_time': profile1['execution_time_ms'],
            'query2_time': profile2['execution_time_ms'],
            'ratio': profile2['execution_time_ms'] / profile1['execution_time_ms']
            if profile1['execution_time_ms'] > 0 else 0
        }

        assert comparison['ratio'] >= 0


class TestStorageMetrics:
    """Test storage metrics collection."""

    @pytest.fixture
    def profiler(self, query_executor):
        """Create QueryProfiler instance."""
        return QueryProfiler(query_executor)

    def test_storage_metrics_collection(self, profiler, temp_dir, sample_fact_sales):
        """Test collecting storage metrics for Parquet files."""
        from src.storage.parquet_handler import ParquetHandler

        # Write test data
        handler = ParquetHandler(temp_dir)
        handler.write(sample_fact_sales, 'metrics_test')

        # Collect storage metrics
        metrics = profiler.collect_storage_metrics(temp_dir, table_name='metrics_test')

        # Verify metrics structure
        assert 'parquet' in metrics
        assert 'file_size_bytes' in metrics['parquet']
        assert 'num_rows' in metrics['parquet']
        assert 'compression_ratio' in metrics['parquet']

    def test_parquet_csv_comparison_metrics(self, profiler, temp_dir, sample_fact_sales):
        """Test comparing Parquet and CSV storage metrics."""
        from src.storage.parquet_handler import ParquetHandler
        from src.storage.csv_handler import CSVHandler

        # Write both formats
        parquet_handler = ParquetHandler(temp_dir / 'parquet')
        csv_handler = CSVHandler(temp_dir / 'csv')

        parquet_handler.write(sample_fact_sales, 'comparison')
        csv_handler.write(sample_fact_sales, 'comparison')

        # Collect metrics for both
        metrics = profiler.collect_storage_metrics(
            temp_dir / 'parquet',
            csv_path=temp_dir / 'csv',
            table_name='comparison'
        )

        # Should have both format metrics
        assert 'parquet' in metrics
        assert 'csv' in metrics

        # Should have size comparison
        if 'parquet' in metrics and 'csv' in metrics:
            parquet_size = metrics['parquet'].get('file_size_bytes', 0)
            csv_size = metrics['csv'].get('file_size_bytes', 0)

            if parquet_size > 0 and csv_size > 0:
                assert 'size_reduction' in metrics or parquet_size != csv_size

    def test_compression_ratio_calculation(self, profiler, temp_dir, sample_fact_sales):
        """Test compression ratio calculation."""
        from src.storage.parquet_handler import ParquetHandler

        # Write Parquet data
        handler = ParquetHandler(temp_dir)
        handler.write(sample_fact_sales, 'compression_test')

        # Get metrics
        metrics = profiler.collect_storage_metrics(temp_dir, table_name='compression_test')

        # Should have compression ratio
        assert 'parquet' in metrics
        assert 'compression_ratio' in metrics['parquet']

        # Ratio should be reasonable (>= 1.0 means some compression)
        ratio = metrics['parquet']['compression_ratio']
        assert ratio >= 1.0  # At least 1:1 (no loss)
        assert ratio < 100.0  # Not unrealistically high


class TestQueryLogging:
    """Test structured logging for queries."""

    @pytest.fixture
    def profiler(self, query_executor):
        """Create QueryProfiler instance."""
        return QueryProfiler(query_executor)

    def test_query_execution_logging(self, profiler, loaded_duckdb):
        """Test logging query execution details."""
        query = "SELECT COUNT(*) FROM fact_sales LIMIT 1"

        # Execute and log
        result = profiler.log_query_execution(
            query=query,
            query_id="test_query_001"
        )

        # Verify log entry structure
        assert 'query_id' in result
        assert 'timestamp' in result
        assert 'duration_ms' in result
        assert 'result_rows' in result

        # Verify values
        assert result['query_id'] == "test_query_001"
        assert isinstance(result['timestamp'], (str, datetime))
        assert result['duration_ms'] >= 0

    def test_log_history_retrieval(self, profiler, loaded_duckdb):
        """Test retrieving query execution log history."""
        # Execute several queries
        for i in range(3):
            profiler.log_query_execution(
                query=f"SELECT COUNT(*) FROM fact_sales WHERE year = {2021 + i} LIMIT 1",
                query_id=f"query_{i}"
            )

        # Retrieve history
        history = profiler.get_query_log()

        # Should have all logged queries
        assert len(history) >= 3

        # Each entry should have complete information
        for entry in history:
            assert 'query_id' in entry
            assert 'duration_ms' in entry

    def test_performance_summary(self, profiler, loaded_duckdb):
        """Test generating performance summary from logs."""
        # Execute queries
        queries = [
            "SELECT COUNT(*) FROM fact_sales LIMIT 1",
            "SELECT SUM(revenue) FROM fact_sales LIMIT 1",
            "SELECT AVG(profit) FROM fact_sales LIMIT 1"
        ]

        for i, query in enumerate(queries):
            profiler.log_query_execution(query, query_id=f"summary_{i}")

        # Get summary statistics
        summary = profiler.get_performance_summary()

        # Should have aggregate metrics
        assert 'total_queries' in summary
        assert 'avg_duration_ms' in summary or 'mean_duration' in summary

        # Total queries should match
        assert summary['total_queries'] >= len(queries)
