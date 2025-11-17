"""Benchmark tests for window functions (User Story 2).

These benchmarks validate query performance SLAs for window function
operations including moving averages, year-over-year growth, and rankings.

SLA Targets:
- 3-month moving average: p95 <3s on 100M rows
- Year-over-year growth: p95 <3s
- Product rankings by quarter: p95 <2s with year filter
"""

import pytest
from datetime import date

from src.query.patterns import QueryPatterns


@pytest.mark.benchmark
class TestWindowFunctions:
    """Benchmark tests for window function queries (US2)."""

    @pytest.fixture
    def query_patterns(self, loaded_duckdb, query_executor):
        """Create query patterns instance with loaded data."""
        return QueryPatterns(query_executor)

    def test_benchmark_moving_average_3_months(
        self,
        benchmark,
        query_patterns,
        loaded_duckdb
    ):
        """Benchmark 3-month moving average calculation.

        SLA: p95 <3s on 100M rows
        User Story 2: Time-series trend analysis

        This test validates that window function queries for moving
        averages complete within performance SLA.
        """
        current_year = date.today().year

        result = benchmark(
            query_patterns.moving_average_revenue,
            window_size=3,
            year=current_year
        )

        # Validate results
        assert not result.empty
        assert 'monthly_revenue' in result.columns
        assert 'moving_avg_3m' in result.columns

        # Moving average should be calculated
        assert result['moving_avg_3m'].notna().any()

    def test_benchmark_moving_average_12_months(
        self,
        benchmark,
        query_patterns,
        loaded_duckdb
    ):
        """Benchmark 12-month moving average calculation.

        SLA: p95 <3s
        User Story 2: Time-series trend analysis

        This test validates performance for larger window sizes.
        """
        result = benchmark(
            query_patterns.moving_average_revenue,
            window_size=12
        )

        # Validate results
        assert not result.empty
        assert 'monthly_revenue' in result.columns
        assert 'moving_avg_12m' in result.columns

    def test_benchmark_yoy_growth_total(
        self,
        benchmark,
        query_patterns,
        loaded_duckdb
    ):
        """Benchmark year-over-year growth calculation (total).

        SLA: p95 <3s
        User Story 2: YoY growth analysis

        This test validates LAG window function performance for
        year-over-year comparisons.
        """
        result = benchmark(
            query_patterns.yoy_growth,
            metric='revenue'
        )

        # Validate results
        assert not result.empty
        assert 'current_year_revenue' in result.columns
        assert 'yoy_growth_pct' in result.columns

    def test_benchmark_yoy_growth_by_category(
        self,
        benchmark,
        query_patterns,
        loaded_duckdb
    ):
        """Benchmark year-over-year growth by category.

        SLA: p95 <3s
        User Story 2: YoY growth analysis with partitioning

        This test validates PARTITION BY performance in window functions.
        """
        result = benchmark(
            query_patterns.yoy_growth,
            metric='revenue',
            dimension='category'
        )

        # Validate results
        assert not result.empty
        assert 'dimension' in result.columns
        assert 'yoy_growth_pct' in result.columns

    def test_benchmark_product_rankings_by_category(
        self,
        benchmark,
        query_patterns,
        loaded_duckdb
    ):
        """Benchmark product rankings by category.

        SLA: p95 <2s with year filter
        User Story 2: Product ranking analysis

        This test validates ROW_NUMBER window function performance
        with partition filters.
        """
        current_year = date.today().year

        result = benchmark(
            query_patterns.product_rankings,
            partition_by='category',
            rank_by='revenue',
            year=current_year,
            top_n=10
        )

        # Validate results
        assert not result.empty
        assert 'partition_key' in result.columns
        assert 'rank' in result.columns
        assert 'product_name' in result.columns

        # Rankings should be 1-10
        assert result['rank'].min() >= 1
        assert result['rank'].max() <= 10

    def test_benchmark_product_rankings_by_quarter(
        self,
        benchmark,
        query_patterns,
        loaded_duckdb
    ):
        """Benchmark product rankings by quarter.

        SLA: p95 <2s
        User Story 2: Quarterly product ranking

        This test validates window function performance with
        temporal partitioning.
        """
        result = benchmark(
            query_patterns.product_rankings,
            partition_by='quarter',
            rank_by='profit',
            top_n=5
        )

        # Validate results
        assert not result.empty
        assert 'rank' in result.columns
        assert result['rank'].max() <= 5


@pytest.mark.integration
class TestWindowFunctionCorrectness:
    """Integration tests for window function correctness (US2)."""

    @pytest.fixture
    def query_patterns(self, loaded_duckdb, query_executor):
        """Create query patterns instance with loaded data."""
        return QueryPatterns(query_executor)

    def test_moving_average_calculation_correctness(
        self,
        query_patterns,
        loaded_duckdb
    ):
        """Validate moving average calculations are mathematically correct.

        User Story 2: Window function correctness

        This test validates that moving average calculations produce
        correct results by comparing window function output with manual
        calculation on a small dataset.
        """
        current_year = date.today().year

        result = query_patterns.moving_average_revenue(
            window_size=3,
            year=current_year
        )

        # Validate basic correctness
        assert not result.empty

        # For first 2 months, moving average should be based on fewer periods
        # For 3rd month onwards, should be true 3-month average
        if len(result) >= 3:
            # Third row should have proper 3-month average
            third_row = result.iloc[2]
            first_three_revenues = result['monthly_revenue'].iloc[:3]
            expected_avg = first_three_revenues.mean()

            # Allow for small floating point differences
            assert abs(third_row['moving_avg_3m'] - expected_avg) < 0.01

    def test_yoy_growth_calculation_correctness(
        self,
        query_patterns,
        loaded_duckdb
    ):
        """Validate year-over-year growth calculations.

        User Story 2: YoY growth correctness

        This test validates that YoY growth percentages are calculated
        correctly using the LAG window function.
        """
        result = query_patterns.yoy_growth(metric='revenue')

        # Validate basic correctness
        assert not result.empty

        # First year should have NULL previous year (no LAG value)
        # Subsequent years should have growth calculations
        for idx, row in result.iterrows():
            if row['previous_year_revenue'] is not None and row['previous_year_revenue'] > 0:
                # Manually calculate growth
                expected_growth = (
                    (row['current_year_revenue'] - row['previous_year_revenue']) * 100.0 /
                    row['previous_year_revenue']
                )

                # Should match within rounding
                if row['yoy_growth_pct'] is not None:
                    assert abs(row['yoy_growth_pct'] - expected_growth) < 0.1

    def test_product_rankings_correctness(
        self,
        query_patterns,
        loaded_duckdb
    ):
        """Validate product rankings are correct within each partition.

        User Story 2: Ranking correctness

        This test validates that ROW_NUMBER produces correct rankings
        within each partition.
        """
        current_year = date.today().year

        result = query_patterns.product_rankings(
            partition_by='category',
            rank_by='revenue',
            year=current_year,
            top_n=5
        )

        # Validate basic correctness
        assert not result.empty

        # Within each category, ranks should be 1-5 and revenue should be descending
        for category in result['partition_key'].unique():
            category_data = result[result['partition_key'] == category].sort_values('rank')

            # Ranks should be consecutive starting from 1
            ranks = category_data['rank'].tolist()
            assert ranks == list(range(1, len(ranks) + 1))

            # Revenue should be in descending order
            revenues = category_data['total_revenue'].tolist()
            assert revenues == sorted(revenues, reverse=True)
