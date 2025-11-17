"""Data generation functions for OLAP dimensional model.

This module provides deterministic data generators for all dimension and fact tables
using fixed random seeds for reproducibility.
"""

from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Tuple
import pandas as pd
from faker import Faker
import random


# Global seed for reproducibility
SEED = 42


class DataGenerator:
    """Base class for deterministic data generation.

    Provides seed management and utilities for generating consistent
    synthetic data across all dimensions and facts.
    """

    def __init__(self, seed: int = SEED):
        """Initialize generator with fixed seed.

        Args:
            seed: Random seed for reproducibility (default: 42)
        """
        self.seed = seed
        self.faker = Faker()
        Faker.seed(seed)
        random.seed(seed)

    def reset_seed(self):
        """Reset random seed to ensure reproducibility."""
        Faker.seed(self.seed)
        random.seed(self.seed)


def generate_dim_time(
    start_date: date,
    end_date: date,
    seed: int = SEED
) -> pd.DataFrame:
    """Generate time dimension with complete calendar data.

    Creates one record per day with temporal attributes including
    year, quarter, month, week, day of week, weekend flags, and holiday markers.

    Args:
        start_date: Start date for time dimension
        end_date: End date for time dimension (inclusive)
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: time_key, date, year, quarter, month,
        month_name, week, day_of_month, day_of_week, day_name,
        is_weekend, is_holiday, fiscal_year, fiscal_quarter, fiscal_period
    """
    generator = DataGenerator(seed)

    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    # US Federal holidays for simple holiday detection
    # In practice, use a proper holiday library
    us_holidays = {
        (1, 1),   # New Year's Day
        (7, 4),   # Independence Day
        (12, 25), # Christmas
        (11, 24), # Thanksgiving (approximate)
    }

    records = []
    for dt in date_range:
        # Calculate fiscal year (assume fiscal year starts in February)
        fiscal_year = dt.year if dt.month >= 2 else dt.year - 1
        fiscal_month = ((dt.month - 2) % 12) + 1
        fiscal_quarter = f"FY-Q{((fiscal_month - 1) // 3) + 1}"
        fiscal_period = f"FY{fiscal_year}-P{fiscal_month:02d}"

        record = {
            'time_key': dt.year * 10000 + dt.month * 100 + dt.day,
            'date': dt.date(),
            'year': dt.year,
            'quarter': f"Q{((dt.month - 1) // 3) + 1}",
            'month': dt.month,
            'month_name': dt.strftime('%B'),
            'week': dt.isocalendar()[1],
            'day_of_month': dt.day,
            'day_of_week': dt.dayofweek + 1,  # 1=Monday, 7=Sunday
            'day_name': dt.strftime('%A'),
            'is_weekend': dt.dayofweek >= 5,
            'is_holiday': (dt.month, dt.day) in us_holidays,
            'fiscal_year': fiscal_year,
            'fiscal_quarter': fiscal_quarter,
            'fiscal_period': fiscal_period,
        }
        records.append(record)

    return pd.DataFrame(records)


def generate_dim_geography(
    num_countries: int = 3,
    num_regions_per_country: int = 5,
    num_cities_per_region: int = 10,
    seed: int = SEED
) -> pd.DataFrame:
    """Generate geography dimension with hierarchical location data.

    Creates a three-level hierarchy: Country -> Region -> City
    with realistic geographic attributes.

    Args:
        num_countries: Number of countries to generate
        num_regions_per_country: Number of regions per country
        num_cities_per_region: Number of cities per region
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: geo_key, city, region, country,
        country_code, latitude, longitude, population_segment, timezone
    """
    generator = DataGenerator(seed)

    # Define country data
    countries_data = [
        {
            'name': 'United States',
            'code': 'US',
            'timezone': 'America/New_York',
            'regions': ['Northeast', 'Southeast', 'Midwest', 'Southwest', 'West'],
        },
        {
            'name': 'United Kingdom',
            'code': 'GB',
            'timezone': 'Europe/London',
            'regions': ['England', 'Scotland', 'Wales', 'Northern Ireland', 'Greater London'],
        },
        {
            'name': 'Canada',
            'code': 'CA',
            'timezone': 'America/Toronto',
            'regions': ['Ontario', 'Quebec', 'British Columbia', 'Alberta', 'Manitoba'],
        },
    ]

    population_segments = ['Small (<100k)', 'Medium (100k-500k)', 'Large (500k-1M)', 'Metro (>1M)']

    records = []
    geo_key = 1

    for country_idx in range(min(num_countries, len(countries_data))):
        country = countries_data[country_idx]

        for region_idx in range(min(num_regions_per_country, len(country['regions']))):
            region = country['regions'][region_idx]

            for city_idx in range(num_cities_per_region):
                # Generate city name
                city = generator.faker.city()

                # Generate realistic lat/long based on country
                if country['code'] == 'US':
                    latitude = float(generator.faker.latitude())
                    longitude = float(generator.faker.longitude())
                elif country['code'] == 'GB':
                    latitude = random.uniform(50.0, 58.0)
                    longitude = random.uniform(-5.0, 2.0)
                else:  # Canada
                    latitude = random.uniform(42.0, 60.0)
                    longitude = random.uniform(-141.0, -52.0)

                record = {
                    'geo_key': geo_key,
                    'city': city,
                    'region': region,
                    'country': country['name'],
                    'country_code': country['code'],
                    'latitude': float(round(latitude, 6)),
                    'longitude': float(round(longitude, 6)),
                    'population_segment': random.choice(population_segments),
                    'timezone': country['timezone'],
                }
                records.append(record)
                geo_key += 1

    return pd.DataFrame(records)


def generate_dim_product(
    num_products: int = 1000,
    change_rate: float = 0.1,
    seed: int = SEED
) -> pd.DataFrame:
    """Generate product dimension with SCD Type 2 support.

    Creates product records with temporal tracking for price and category changes.
    Implements Slowly Changing Dimension Type 2 with effective/expiration dates.

    Args:
        num_products: Number of distinct products
        change_rate: Proportion of products with historical changes (0.0-1.0)
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: product_key, product_id, product_name,
        category, subcategory, brand, unit_cost, unit_price,
        effective_date, expiration_date, is_current
    """
    generator = DataGenerator(seed)

    # Product categories
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Mens', 'Womens', 'Kids', 'Accessories'],
        'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Outdoor'],
        'Sports': ['Equipment', 'Apparel', 'Footwear', 'Accessories'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics'],
    }

    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'BrandF', 'BrandG']

    records = []
    product_key = 1

    # Reference dates for SCD
    historical_date = date(2022, 1, 1)
    current_date = date(2024, 1, 1)
    far_future = date(9999, 12, 31)

    for product_idx in range(num_products):
        product_id = f"PROD-{product_idx + 1:05d}"
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        brand = random.choice(brands)

        # Generate initial product version
        product_name = f"{brand} {subcategory} {generator.faker.color_name()}"
        unit_cost = round(random.uniform(5.0, 200.0), 2)
        unit_price = round(unit_cost * random.uniform(1.3, 2.5), 2)

        # Should this product have a historical change?
        has_change = random.random() < change_rate

        if has_change:
            # Create historical version (expired)
            old_price = round(unit_price * random.uniform(0.8, 1.2), 2)
            old_cost = round(old_price / random.uniform(1.3, 2.5), 2)

            historical_record = {
                'product_key': product_key,
                'product_id': product_id,
                'product_name': product_name,
                'category': category,
                'subcategory': subcategory,
                'brand': brand,
                'unit_cost': old_cost,
                'unit_price': old_price,
                'effective_date': historical_date,
                'expiration_date': current_date,
                'is_current': False,
            }
            records.append(historical_record)
            product_key += 1

        # Create current version
        current_record = {
            'product_key': product_key,
            'product_id': product_id,
            'product_name': product_name,
            'category': category,
            'subcategory': subcategory,
            'brand': brand,
            'unit_cost': unit_cost,
            'unit_price': unit_price,
            'effective_date': current_date if has_change else historical_date,
            'expiration_date': far_future,
            'is_current': True,
        }
        records.append(current_record)
        product_key += 1

    return pd.DataFrame(records)


def generate_dim_customer(
    num_customers: int = 10000,
    seed: int = SEED
) -> pd.DataFrame:
    """Generate customer dimension with realistic demographic data.

    Creates customer records with demographic attributes and segmentation.

    Args:
        num_customers: Number of customers to generate
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: customer_key, customer_id, first_name,
        last_name, email, phone, date_of_birth, gender, income_segment,
        customer_segment, registration_date, is_active
    """
    generator = DataGenerator(seed)

    income_segments = ['Low (<30k)', 'Medium (30k-75k)', 'High (75k-150k)', 'Premium (>150k)']
    customer_segments = ['Bronze', 'Silver', 'Gold', 'Platinum']
    genders = ['M', 'F', 'Other']

    records = []

    for customer_idx in range(num_customers):
        # Generate registration date (spread over 5 years)
        days_ago = random.randint(0, 365 * 5)
        registration_date = date.today() - timedelta(days=days_ago)

        # Generate birth date (18-80 years old)
        age_years = random.randint(18, 80)
        date_of_birth = date.today() - timedelta(days=age_years * 365)

        # Customer segment correlates with income
        income_segment = random.choice(income_segments)
        if 'Premium' in income_segment:
            customer_segment = random.choice(['Gold', 'Platinum'])
        elif 'High' in income_segment:
            customer_segment = random.choice(['Silver', 'Gold'])
        else:
            customer_segment = random.choice(['Bronze', 'Silver'])

        # Older customers more likely to be inactive
        is_active = random.random() > (days_ago / (365 * 5 * 2))

        record = {
            'customer_key': customer_idx + 1,
            'customer_id': f"CUST-{customer_idx + 1:06d}",
            'first_name': generator.faker.first_name(),
            'last_name': generator.faker.last_name(),
            'email': generator.faker.email(),
            'phone': generator.faker.phone_number(),
            'date_of_birth': date_of_birth,
            'gender': random.choice(genders),
            'income_segment': income_segment,
            'customer_segment': customer_segment,
            'registration_date': registration_date,
            'is_active': is_active,
        }
        records.append(record)

    return pd.DataFrame(records)


def generate_dim_payment(seed: int = SEED) -> pd.DataFrame:
    """Generate payment method dimension.

    Creates reference data for payment methods with associated metadata.

    Args:
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: payment_key, payment_method,
        payment_type, processing_fee_pct, is_digital
    """
    generator = DataGenerator(seed)

    payment_methods = [
        {
            'payment_method': 'Credit Card',
            'payment_type': 'Card',
            'processing_fee_pct': 2.9,
            'is_digital': True,
        },
        {
            'payment_method': 'Debit Card',
            'payment_type': 'Card',
            'processing_fee_pct': 1.5,
            'is_digital': True,
        },
        {
            'payment_method': 'PayPal',
            'payment_type': 'Digital Wallet',
            'processing_fee_pct': 3.5,
            'is_digital': True,
        },
        {
            'payment_method': 'Apple Pay',
            'payment_type': 'Digital Wallet',
            'processing_fee_pct': 2.5,
            'is_digital': True,
        },
        {
            'payment_method': 'Bank Transfer',
            'payment_type': 'Electronic',
            'processing_fee_pct': 0.5,
            'is_digital': True,
        },
        {
            'payment_method': 'Cash',
            'payment_type': 'Physical',
            'processing_fee_pct': 0.0,
            'is_digital': False,
        },
        {
            'payment_method': 'Check',
            'payment_type': 'Physical',
            'processing_fee_pct': 0.0,
            'is_digital': False,
        },
    ]

    for idx, method in enumerate(payment_methods):
        method['payment_key'] = idx + 1

    return pd.DataFrame(payment_methods)


def generate_sales_fact(
    num_transactions: int,
    time_df: pd.DataFrame,
    geo_df: pd.DataFrame,
    product_df: pd.DataFrame,
    customer_df: pd.DataFrame,
    payment_df: pd.DataFrame,
    pareto_factor: float = 0.8,
    seed: int = SEED
) -> pd.DataFrame:
    """Generate sales fact table with realistic transaction patterns.

    Uses Pareto distribution (80/20 rule) for product popularity and
    customer purchase frequency. Generates multi-line transactions with
    calculated measures.

    Args:
        num_transactions: Number of transactions to generate
        time_df: Time dimension DataFrame
        geo_df: Geography dimension DataFrame
        product_df: Product dimension DataFrame (current versions only)
        customer_df: Customer dimension DataFrame
        payment_df: Payment dimension DataFrame
        pareto_factor: Pareto distribution factor (0.8 = 80/20 rule)
        seed: Random seed for reproducibility

    Returns:
        DataFrame with columns: transaction_id, line_item_id,
        transaction_date, transaction_timestamp, time_key, geo_key,
        product_key, customer_key, payment_key, quantity, unit_price,
        revenue, cost, discount_amount, profit
    """
    generator = DataGenerator(seed)

    # Filter to current products only for fact generation
    current_products = product_df[product_df['is_current'] == True].copy()

    # Create weighted distributions for Pareto effect
    num_products = len(current_products)
    num_customers = len(customer_df)

    # Pareto weights: top 20% get 80% of selections
    pareto_products = _create_pareto_weights(num_products, pareto_factor)
    pareto_customers = _create_pareto_weights(num_customers, pareto_factor)

    records = []
    transaction_id = 1

    # Generate transactions
    for _ in range(num_transactions):
        # Select transaction date (weighted toward recent dates)
        time_record = time_df.sample(n=1, weights=_create_recency_weights(len(time_df))).iloc[0]
        transaction_date = time_record['date']
        time_key = time_record['time_key']

        # Add random time to date
        hour = random.randint(8, 21)  # Business hours 8am-9pm
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        transaction_timestamp = datetime.combine(
            transaction_date,
            datetime.min.time()
        ).replace(hour=hour, minute=minute, second=second)

        # Select dimension keys
        geo_key = geo_df.sample(n=1).iloc[0]['geo_key']

        # Use Pareto distribution for customer selection
        customer_idx = random.choices(range(num_customers), weights=pareto_customers, k=1)[0]
        customer_key = customer_df.iloc[customer_idx]['customer_key']

        payment_key = payment_df.sample(n=1).iloc[0]['payment_key']

        # Generate 1-5 line items per transaction
        num_line_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 15, 10, 5], k=1)[0]

        for line_item_id in range(1, num_line_items + 1):
            # Use Pareto distribution for product selection
            product_idx = random.choices(range(num_products), weights=pareto_products, k=1)[0]
            product_record = current_products.iloc[product_idx]
            product_key = product_record['product_key']

            # Use product's unit price with small random variation
            base_unit_price = product_record['unit_price']
            unit_price = round(base_unit_price * random.uniform(0.95, 1.05), 2)

            # Use product's unit cost
            cost_per_unit = product_record['unit_cost']

            # Generate quantity (most purchases are 1-2 items)
            quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3], k=1)[0]

            # Calculate base revenue
            revenue = round(quantity * unit_price, 2)
            total_cost = round(quantity * cost_per_unit, 2)

            # Apply discount (20% of transactions get 5-25% discount)
            discount_amount = 0.0
            if random.random() < 0.2:
                discount_pct = random.uniform(0.05, 0.25)
                discount_amount = round(revenue * discount_pct, 2)

            # Calculate final revenue and profit
            final_revenue = round(revenue - discount_amount, 2)
            profit = round(final_revenue - total_cost, 2)

            record = {
                'transaction_id': transaction_id,
                'line_item_id': line_item_id,
                'transaction_date': transaction_date,
                'transaction_timestamp': transaction_timestamp,
                'time_key': time_key,
                'geo_key': geo_key,
                'product_key': product_key,
                'customer_key': customer_key,
                'payment_key': payment_key,
                'quantity': quantity,
                'unit_price': unit_price,
                'revenue': final_revenue,
                'cost': total_cost,
                'discount_amount': discount_amount,
                'profit': profit,
            }
            records.append(record)

        transaction_id += 1

    return pd.DataFrame(records)


def _create_pareto_weights(n: int, factor: float = 0.8) -> List[float]:
    """Create Pareto distribution weights (80/20 rule).

    Args:
        n: Number of items
        factor: Pareto factor (0.8 = top 20% get 80% of weight)

    Returns:
        List of weights for random.choices()
    """
    # Top 20% of items get 80% of the total weight
    top_20_pct = max(1, int(n * 0.2))

    weights = []
    for i in range(n):
        if i < top_20_pct:
            # Top 20% share 80% of weight
            weights.append(factor / top_20_pct)
        else:
            # Bottom 80% share 20% of weight
            weights.append((1 - factor) / (n - top_20_pct))

    return weights


def _create_recency_weights(n: int) -> List[float]:
    """Create recency weights favoring recent dates.

    Args:
        n: Number of dates

    Returns:
        List of weights favoring recent dates
    """
    # Linear decay: most recent date gets highest weight
    return [i + 1 for i in range(n)]
