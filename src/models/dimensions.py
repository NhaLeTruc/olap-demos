"""Dimension table schemas for the OLAP star schema.

All dimensions follow the Kimball dimensional modeling methodology.
"""

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class DimTime:
    """Time dimension with calendar hierarchy.

    Type: Conformed dimension (Type 0 - static, dates don't change)
    Grain: One row per calendar day
    Size: ~1,095 rows (3 years: 2021-2023)
    """

    time_key: int  # Surrogate key (YYYYMMDD format)
    date: date  # Calendar date
    year: int  # Year (2021-2023)
    quarter: str  # Quarter (Q1-Q4)
    quarter_number: int  # Quarter as number (1-4)
    month: int  # Month number (1-12)
    month_name: str  # Month name (January-December)
    day_of_month: int  # Day of month (1-31)
    day_of_week: int  # Day of week (1=Monday, 7=Sunday)
    day_name: str  # Day name (Monday-Sunday)
    week_of_year: int  # ISO week number (1-53)
    is_weekend: bool  # Weekend indicator
    is_holiday: bool  # Holiday indicator (US federal holidays)
    fiscal_year: int  # Fiscal year (if different from calendar)
    fiscal_quarter: str  # Fiscal quarter


@dataclass
class DimGeography:
    """Geography dimension with location hierarchy.

    Type: Conformed dimension (Type 1 - overwrite changes)
    Grain: One row per city
    Size: ~5,000 rows (multi-level hierarchy)
    Hierarchy: Region → Country → State/Province → City
    """

    geo_key: int  # Surrogate key (auto-increment)
    region: str  # Geographic region (North America, Europe, Asia, etc.)
    country: str  # Country name (United States, United Kingdom, etc.)
    country_code: str  # ISO 3166-1 alpha-3 code (USA, GBR, etc.)
    state_province: str  # State or province name
    state_code: str  # State abbreviation (CA, NY, etc.)
    city: str  # City name
    postal_code: str  # ZIP/postal code
    latitude: float  # Latitude coordinate (decimal degrees)
    longitude: float  # Longitude coordinate (decimal degrees)
    timezone: str  # Time zone (America/Los_Angeles, etc.)
    population_tier: str  # City size category (Large, Medium, Small)


@dataclass
class DimProduct:
    """Product dimension with category hierarchy and SCD Type 2.

    Type: Slowly Changing Dimension Type 2 (preserve history)
    Grain: One row per product version
    Size: ~10,000 current products + historical records (~15,000 total)
    Hierarchy: Category → Subcategory → Product
    SCD Type 2: Track price changes, category changes over time
    """

    product_key: int  # Surrogate key (auto-increment, unique per version)
    product_id: str  # Natural key (business key, same across versions)
    product_name: str  # Product name
    product_sku: str  # Stock keeping unit
    category: str  # Top-level category (Electronics, Clothing, etc.)
    subcategory: str  # Product subcategory (Audio, Apparel, etc.)
    brand: str  # Brand name
    supplier: str  # Supplier name
    unit_cost: float  # Cost to company (USD)
    list_price: float  # Standard retail price (USD)
    is_active: bool  # Currently available for sale

    # SCD Type 2 fields
    effective_date: date  # Version effective date
    expiration_date: date  # Version expiration date (2999-12-31 for current)
    is_current: bool  # Current version flag


@dataclass
class DimCustomer:
    """Customer dimension with segmentation attributes.

    Type: Conformed dimension (Type 1 - overwrite most attributes)
    Grain: One row per customer
    Size: ~1,000,000 customers
    """

    customer_key: int  # Surrogate key (auto-increment)
    customer_id: str  # Natural key (CUST-######)
    customer_segment: str  # Customer segmentation (Premium, Standard, Budget)
    acquisition_channel: str  # How customer acquired (Online, Retail, Partner, Referral)
    customer_lifetime_value_tier: str  # CLV classification (High, Medium, Low)
    signup_date: date  # Account creation date
    country_code: str  # Customer country (USA, GBR, CAN, etc.)
    is_business_customer: bool  # B2B vs B2C indicator
    preferred_contact_method: str  # Contact preference (Email, SMS, Phone)


@dataclass
class DimPayment:
    """Payment method dimension.

    Type: Conformed dimension (Type 1 - static)
    Grain: One row per payment method
    Size: ~20 payment methods
    """

    payment_key: int  # Surrogate key (auto-increment)
    payment_method_id: str  # Natural key (PM-###)
    payment_type: str  # Payment type (Credit Card, Debit Card, PayPal, etc.)
    payment_provider: str  # Payment processor (Visa, Mastercard, PayPal, Stripe, etc.)
    processing_fee_percent: float  # Fee percentage (e.g., 0.029 = 2.9%)
    is_instant: bool  # Immediate settlement indicator
    requires_verification: bool  # Additional auth required


# Schema validation helpers

def validate_dim_time(record: DimTime) -> bool:
    """Validate DimTime record constraints."""
    assert 2021 <= record.year <= 2023, f"Year must be 2021-2023, got {record.year}"
    assert 1 <= record.quarter_number <= 4, f"Quarter must be 1-4, got {record.quarter_number}"
    assert 1 <= record.month <= 12, f"Month must be 1-12, got {record.month}"
    assert 1 <= record.day_of_week <= 7, f"Day of week must be 1-7, got {record.day_of_week}"
    assert record.time_key == int(record.date.strftime("%Y%m%d")), "time_key format mismatch"
    return True


def validate_dim_geography(record: DimGeography) -> bool:
    """Validate DimGeography record constraints."""
    assert -90 <= record.latitude <= 90, f"Latitude must be -90 to 90, got {record.latitude}"
    assert -180 <= record.longitude <= 180, f"Longitude must be -180 to 180, got {record.longitude}"
    assert len(record.country_code) == 3, f"Country code must be 3 chars, got {record.country_code}"
    return True


def validate_dim_product(record: DimProduct) -> bool:
    """Validate DimProduct record constraints including SCD Type 2."""
    assert record.unit_cost > 0, f"Unit cost must be positive, got {record.unit_cost}"
    assert record.list_price > 0, f"List price must be positive, got {record.list_price}"
    assert record.effective_date <= record.expiration_date, "Effective date must be <= expiration date"
    if record.is_current:
        assert record.expiration_date.year == 2999, "Current records must have expiration year 2999"
    return True


def validate_dim_customer(record: DimCustomer) -> bool:
    """Validate DimCustomer record constraints."""
    assert record.customer_segment in ["Premium", "Standard", "Budget"], \
        f"Invalid segment: {record.customer_segment}"
    assert record.customer_lifetime_value_tier in ["High", "Medium", "Low"], \
        f"Invalid LTV tier: {record.customer_lifetime_value_tier}"
    return True


def validate_dim_payment(record: DimPayment) -> bool:
    """Validate DimPayment record constraints."""
    assert 0 <= record.processing_fee_percent <= 0.1, \
        f"Processing fee must be 0-10%, got {record.processing_fee_percent}"
    return True
