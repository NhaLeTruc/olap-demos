"""Predefined query patterns for OLAP demonstrations.

This module provides reusable query patterns for common OLAP operations
including multi-dimensional aggregations, drill-downs, window functions,
and partition pruning demonstrations.
"""

from typing import Dict, Any, Optional, List
import pandas as pd
from .executor import QueryExecutor


class QueryPatterns:
    """Collection of predefined OLAP query patterns.

    Provides standardized query patterns for demonstrating OLAP capabilities
    including multi-dimensional aggregations, time-series analysis, and
    storage efficiency comparisons.
    """

    def __init__(self, executor: QueryExecutor):
        """Initialize query patterns.

        Args:
            executor: Query executor instance
        """
        self.executor = executor

    # User Story 1: Multi-Dimensional Aggregations

    def revenue_by_dimensions(
        self,
        dimensions: List[str],
        filters: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Execute multi-dimensional revenue aggregation.

        Args:
            dimensions: List of dimension columns to group by
                (e.g., ['year', 'country', 'category'])
            filters: Optional filter conditions (e.g., {'year': 2024})
            limit: Optional limit on results

        Returns:
            DataFrame with aggregated results
        """
        # Build dimension columns
        dim_cols = []
        for dim in dimensions:
            if '.' not in dim:
                # Auto-prefix common dimensions
                if dim in ['year', 'quarter', 'month', 'month_name']:
                    dim_cols.append(f'dt.{dim}')
                elif dim in ['country', 'region', 'city']:
                    dim_cols.append(f'dg.{dim}')
                elif dim in ['category', 'subcategory', 'brand']:
                    dim_cols.append(f'dp.{dim}')
                elif dim in ['customer_segment', 'income_segment']:
                    dim_cols.append(f'dc.{dim}')
                else:
                    dim_cols.append(dim)
            else:
                dim_cols.append(dim)

        group_by_clause = ', '.join(dim_cols)
        select_clause = ', '.join(dim_cols)

        # Build WHERE clause
        where_clause = ""
        if filters:
            conditions = []
            for col, value in filters.items():
                # Add table prefix if needed
                if '.' not in col:
                    if col in ['year', 'quarter', 'month']:
                        col = f'dt.{col}'
                    elif col in ['country', 'region']:
                        col = f'dg.{col}'
                    elif col in ['category']:
                        col = f'dp.{col}'

                if isinstance(value, str):
                    conditions.append(f"{col} = '{value}'")
                else:
                    conditions.append(f"{col} = {value}")

            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)

        # Build LIMIT clause
        limit_clause = f"LIMIT {limit}" if limit else ""

        query = f"""
        SELECT
            {select_clause},
            SUM(fs.revenue) as total_revenue,
            SUM(fs.profit) as total_profit,
            SUM(fs.quantity) as total_quantity,
            COUNT(*) as transaction_count,
            AVG(fs.revenue) as avg_revenue
        FROM fact_sales fs
        JOIN dim_time dt ON fs.time_key = dt.time_key
        JOIN dim_geography dg ON fs.geo_key = dg.geo_key
        JOIN dim_product dp ON fs.product_key = dp.product_key
        JOIN dim_customer dc ON fs.customer_key = dc.customer_key
        {where_clause}
        GROUP BY {group_by_clause}
        ORDER BY total_revenue DESC
        {limit_clause}
        """

        result = self.executor.execute(query)
        return result.data

    def drill_down_time_hierarchy(
        self,
        year: int,
        quarter: Optional[str] = None,
        month: Optional[int] = None
    ) -> pd.DataFrame:
        """Execute hierarchical drill-down through time dimension.

        Args:
            year: Year to drill into
            quarter: Optional quarter filter (Q1-Q4)
            month: Optional month filter (1-12)

        Returns:
            DataFrame with drill-down results
        """
        # Determine drill-down level
        if month:
            # Month level - show daily results
            query = f"""
            SELECT
                dt.date,
                dt.day_name,
                SUM(fs.revenue) as daily_revenue,
                COUNT(*) as transaction_count
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            WHERE dt.year = {year}
              AND dt.quarter = '{quarter}'
              AND dt.month = {month}
            GROUP BY dt.date, dt.day_name
            ORDER BY dt.date
            """
        elif quarter:
            # Quarter level - show monthly results
            query = f"""
            SELECT
                dt.month,
                dt.month_name,
                SUM(fs.revenue) as monthly_revenue,
                COUNT(*) as transaction_count
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            WHERE dt.year = {year}
              AND dt.quarter = '{quarter}'
            GROUP BY dt.month, dt.month_name
            ORDER BY dt.month
            """
        else:
            # Year level - show quarterly results
            query = f"""
            SELECT
                dt.quarter,
                SUM(fs.revenue) as quarterly_revenue,
                COUNT(*) as transaction_count
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            WHERE dt.year = {year}
            GROUP BY dt.quarter
            ORDER BY dt.quarter
            """

        result = self.executor.execute(query)
        return result.data

    def partition_pruning_comparison(
        self,
        with_filter: bool = True,
        year: Optional[int] = None,
        quarter: Optional[str] = None
    ) -> Dict[str, Any]:
        """Demonstrate partition pruning effectiveness.

        Args:
            with_filter: Whether to apply partition filter
            year: Optional year filter
            quarter: Optional quarter filter

        Returns:
            Dictionary with query results and execution metrics
        """
        if with_filter and year:
            filter_clause = f"WHERE year = {year}"
            if quarter:
                filter_clause += f" AND quarter = '{quarter}'"
        else:
            filter_clause = ""

        query = f"""
        SELECT
            SUM(revenue) as total_revenue,
            COUNT(*) as row_count
        FROM fact_sales
        {filter_clause}
        """

        # Get execution plan
        explain_query = f"EXPLAIN {query}"
        plan_result = self.executor.execute(explain_query)

        # Execute query
        result = self.executor.execute(query)

        return {
            'data': result.data,
            'execution_time_ms': result.execution_time_ms,
            'execution_plan': plan_result.data,
            'with_filter': with_filter,
        }

    # User Story 2: Window Functions

    def moving_average_revenue(
        self,
        window_size: int = 3,
        year: Optional[int] = None
    ) -> pd.DataFrame:
        """Calculate moving average of revenue over time.

        Args:
            window_size: Number of periods for moving average (default: 3 months)
            year: Optional year filter

        Returns:
            DataFrame with moving average results
        """
        where_clause = f"WHERE dt.year = {year}" if year else ""

        query = f"""
        SELECT
            dt.year,
            dt.month,
            dt.month_name,
            SUM(fs.revenue) as monthly_revenue,
            AVG(SUM(fs.revenue)) OVER (
                ORDER BY dt.year, dt.month
                ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW
            ) as moving_avg_{window_size}m
        FROM fact_sales fs
        JOIN dim_time dt ON fs.time_key = dt.time_key
        {where_clause}
        GROUP BY dt.year, dt.month, dt.month_name
        ORDER BY dt.year, dt.month
        """

        result = self.executor.execute(query)
        return result.data

    def yoy_growth(
        self,
        metric: str = 'revenue',
        dimension: Optional[str] = None
    ) -> pd.DataFrame:
        """Calculate year-over-year growth.

        Args:
            metric: Metric to calculate growth for (revenue, profit, quantity)
            dimension: Optional dimension to group by (category, region, etc.)

        Returns:
            DataFrame with YoY growth calculations
        """
        metric_col = f'fs.{metric}'

        if dimension:
            # Add dimension to SELECT and GROUP BY
            if dimension in ['category', 'subcategory']:
                dim_col = f'dp.{dimension}'
                join_clause = "JOIN dim_product dp ON fs.product_key = dp.product_key"
            elif dimension in ['country', 'region']:
                dim_col = f'dg.{dimension}'
                join_clause = "JOIN dim_geography dg ON fs.geo_key = dg.geo_key"
            else:
                dim_col = dimension
                join_clause = ""

            query = f"""
            SELECT
                dt.year,
                {dim_col} as dimension,
                SUM({metric_col}) as current_year_{metric},
                LAG(SUM({metric_col}), 1) OVER (
                    PARTITION BY {dim_col}
                    ORDER BY dt.year
                ) as previous_year_{metric},
                ROUND(
                    (SUM({metric_col}) - LAG(SUM({metric_col}), 1) OVER (
                        PARTITION BY {dim_col}
                        ORDER BY dt.year
                    )) * 100.0 / LAG(SUM({metric_col}), 1) OVER (
                        PARTITION BY {dim_col}
                        ORDER BY dt.year
                    ),
                    2
                ) as yoy_growth_pct
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            {join_clause}
            GROUP BY dt.year, {dim_col}
            ORDER BY dt.year, dimension
            """
        else:
            query = f"""
            SELECT
                dt.year,
                SUM({metric_col}) as current_year_{metric},
                LAG(SUM({metric_col}), 1) OVER (ORDER BY dt.year) as previous_year_{metric},
                ROUND(
                    (SUM({metric_col}) - LAG(SUM({metric_col}), 1) OVER (ORDER BY dt.year)) * 100.0 /
                    LAG(SUM({metric_col}), 1) OVER (ORDER BY dt.year),
                    2
                ) as yoy_growth_pct
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            GROUP BY dt.year
            ORDER BY dt.year
            """

        result = self.executor.execute(query)
        return result.data

    def product_rankings(
        self,
        partition_by: str = 'category',
        rank_by: str = 'revenue',
        year: Optional[int] = None,
        top_n: int = 10
    ) -> pd.DataFrame:
        """Rank products by metric within partitions.

        Args:
            partition_by: Column to partition rankings by (category, quarter, etc.)
            rank_by: Metric to rank by (revenue, profit, quantity)
            year: Optional year filter
            top_n: Top N products to return per partition

        Returns:
            DataFrame with product rankings
        """
        rank_col = f'fs.{rank_by}'
        where_clause = f"WHERE dt.year = {year}" if year else ""

        if partition_by in ['category', 'subcategory', 'brand']:
            partition_col = f'dp.{partition_by}'
        elif partition_by in ['quarter', 'month']:
            partition_col = f'dt.{partition_by}'
        else:
            partition_col = partition_by

        query = f"""
        WITH ranked_products AS (
            SELECT
                {partition_col} as partition_key,
                dp.product_name,
                SUM({rank_col}) as total_{rank_by},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_col}
                    ORDER BY SUM({rank_col}) DESC
                ) as rank
            FROM fact_sales fs
            JOIN dim_time dt ON fs.time_key = dt.time_key
            JOIN dim_product dp ON fs.product_key = dp.product_key
            {where_clause}
            GROUP BY {partition_col}, dp.product_name
        )
        SELECT *
        FROM ranked_products
        WHERE rank <= {top_n}
        ORDER BY partition_key, rank
        """

        result = self.executor.execute(query)
        return result.data

    # Storage Comparison

    def same_query_both_formats(
        self,
        query_pattern: str,
        dimensions: List[str]
    ) -> Dict[str, Any]:
        """Execute same query on Parquet and CSV formats for comparison.

        Args:
            query_pattern: Query pattern to execute (e.g., 'revenue_by_dimensions')
            dimensions: Dimensions for the query

        Returns:
            Dictionary with results from both formats
        """
        # Execute on Parquet (fact_sales)
        parquet_result = self.revenue_by_dimensions(dimensions)
        parquet_time = self.executor.query_history[-1].execution_time_ms

        # Execute on CSV (fact_sales_csv) if available
        # This would require the CSV table to be loaded
        # For now, return Parquet results
        return {
            'parquet': {
                'data': parquet_result,
                'execution_time_ms': parquet_time,
            },
            'format_comparison': 'CSV table not loaded',
        }
