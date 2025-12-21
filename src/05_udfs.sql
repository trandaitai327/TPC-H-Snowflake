-- ============================================================================
-- PHẦN 5: USER-DEFINED FUNCTIONS (UDFs)
-- ============================================================================
-- TPC-H Analytics Project - SQL UDFs và Python UDFs
-- ============================================================================

USE DATABASE TPCH_ANALYTICS_DB;
USE SCHEMA TPCH_ANALYTICS_DB.UDFS;

-- ============================================================================
-- 5.1 SQL UDFs
-- ============================================================================

-- UDF 1: Phân loại khách hàng theo revenue
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_CUSTOMER_TIER(TOTAL_REVENUE NUMBER)
RETURNS VARCHAR(20)
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN TOTAL_REVENUE >= 500000 THEN 'VIP'
        WHEN TOTAL_REVENUE >= 200000 THEN 'GOLD'
        WHEN TOTAL_REVENUE >= 100000 THEN 'SILVER'
        WHEN TOTAL_REVENUE >= 50000 THEN 'BRONZE'
        ELSE 'STANDARD'
    END
$$;

-- UDF 2: Validate phone number format
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_VALIDATE_PHONE(PHONE_NUMBER VARCHAR)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN PHONE_NUMBER IS NULL THEN FALSE
        WHEN REGEXP_LIKE(PHONE_NUMBER, '^[0-9]{2}-[0-9]{3}-[0-9]{3}-[0-9]{4}$') THEN TRUE
        ELSE FALSE
    END
$$;

-- UDF 3: Validate email format
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_VALIDATE_EMAIL(EMAIL_ADDRESS VARCHAR)
RETURNS BOOLEAN
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN EMAIL_ADDRESS IS NULL THEN FALSE
        WHEN REGEXP_LIKE(EMAIL_ADDRESS, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$') THEN TRUE
        ELSE FALSE
    END
$$;

-- UDF 4: Calculate days between dates (with NULL handling)
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_DAYS_BETWEEN(DATE1 DATE, DATE2 DATE)
RETURNS NUMBER
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN DATE1 IS NULL OR DATE2 IS NULL THEN NULL
        ELSE DATEDIFF('day', DATE1, DATE2)
    END
$$;

-- UDF 5: Format currency
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_FORMAT_CURRENCY(AMOUNT NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN AMOUNT IS NULL THEN NULL
        ELSE '$' || TO_CHAR(ROUND(AMOUNT, 2), '999,999,999,999.00')
    END
$$;

-- UDF 6: Extract year quarter from date
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_YEAR_QUARTER(ORDER_DATE DATE)
RETURNS VARCHAR
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN ORDER_DATE IS NULL THEN NULL
        ELSE TO_CHAR(YEAR(ORDER_DATE)) || '-Q' || TO_CHAR(QUARTER(ORDER_DATE))
    END
$$;

-- UDF 7: Calculate discount percentage
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_DISCOUNT_PERCENTAGE(ORIGINAL_PRICE NUMBER, DISCOUNT_AMOUNT NUMBER)
RETURNS NUMBER(5,2)
LANGUAGE SQL
IMMUTABLE
AS
$$
    CASE 
        WHEN ORIGINAL_PRICE IS NULL OR ORIGINAL_PRICE = 0 THEN NULL
        WHEN DISCOUNT_AMOUNT IS NULL THEN 0
        ELSE ROUND((DISCOUNT_AMOUNT / ORIGINAL_PRICE) * 100, 2)
    END
$$;

-- ============================================================================
-- 5.2 TEST SQL UDFs
-- ============================================================================

-- Test UDF 1: Customer Tier
SELECT 
    C_CUSTKEY,
    TOTAL_SPENT,
    TPCH_ANALYTICS_DB.UDFS.FN_CUSTOMER_TIER(TOTAL_SPENT) AS CUSTOMER_TIER
FROM TPCH_ANALYTICS_DB.GOLD.CUSTOMER_LTV
ORDER BY TOTAL_SPENT DESC
LIMIT 10;

-- Test UDF 2: Validate Phone
SELECT 
    C_CUSTKEY,
    C_PHONE,
    TPCH_ANALYTICS_DB.UDFS.FN_VALIDATE_PHONE(C_PHONE) AS IS_VALID_PHONE
FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER
LIMIT 10;

-- Test UDF 3: Validate Email
SELECT 
    C_CUSTKEY,
    EMAIL,
    TPCH_ANALYTICS_DB.UDFS.FN_VALIDATE_EMAIL(EMAIL) AS IS_VALID_EMAIL
FROM TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_SENSITIVE
LIMIT 10;

-- Test UDF 4: Days Between
SELECT 
    L_ORDERKEY,
    L_COMMITDATE,
    L_RECEIPTDATE,
    TPCH_ANALYTICS_DB.UDFS.FN_DAYS_BETWEEN(L_COMMITDATE, L_RECEIPTDATE) AS SHIP_DELAY_DAYS
FROM TPCH_ANALYTICS_DB.SILVER.LINEITEM_SILVER
LIMIT 10;

-- Test UDF 5: Format Currency
SELECT 
    SUMMARY_DATE,
    TOTAL_REVENUE,
    TPCH_ANALYTICS_DB.UDFS.FN_FORMAT_CURRENCY(TOTAL_REVENUE) AS FORMATTED_REVENUE
FROM TPCH_ANALYTICS_DB.GOLD.DAILY_SALES_SUMMARY
ORDER BY SUMMARY_DATE DESC
LIMIT 10;

-- Test UDF 6: Year Quarter
SELECT 
    O_ORDERKEY,
    O_ORDERDATE,
    TPCH_ANALYTICS_DB.UDFS.FN_YEAR_QUARTER(O_ORDERDATE) AS YEAR_QUARTER
FROM TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER
LIMIT 10;

-- Test UDF 7: Discount Percentage
SELECT 
    L_ORDERKEY,
    L_EXTENDEDPRICE AS ORIGINAL_PRICE,
    L_DISCOUNT * L_EXTENDEDPRICE AS DISCOUNT_AMOUNT,
    TPCH_ANALYTICS_DB.UDFS.FN_DISCOUNT_PERCENTAGE(L_EXTENDEDPRICE, L_DISCOUNT * L_EXTENDEDPRICE) AS DISCOUNT_PERCENT
FROM TPCH_ANALYTICS_DB.SILVER.LINEITEM_SILVER
LIMIT 10;

-- ============================================================================
-- 5.3 PYTHON UDFs (User-Defined Functions)
-- ============================================================================

-- Python UDF 1: Calculate distance between two coordinates (Haversine formula)
-- Note: This is a template - you would need to provide actual coordinates
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_CALCULATE_DISTANCE(
    LAT1 FLOAT, LON1 FLOAT, LAT2 FLOAT, LON2 FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'calculate_distance'
AS
$$
import math

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    Returns distance in kilometers
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    
    # Convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371  # Radius of earth in kilometers
    
    return c * r
$$;

-- Python UDF 2: Extract text sentiment (simple version)
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_EXTRACT_SENTIMENT(TEXT VARCHAR)
RETURNS VARCHAR(20)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'extract_sentiment'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
def extract_sentiment(text):
    """
    Simple sentiment analysis based on keyword matching
    Returns: POSITIVE, NEGATIVE, or NEUTRAL
    """
    if text is None or text == '':
        return 'NEUTRAL'
    
    text_lower = text.lower()
    
    positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful', 'perfect', 'love', 'best']
    negative_words = ['bad', 'terrible', 'awful', 'worst', 'hate', 'poor', 'disappointed', 'failed']
    
    positive_count = sum(1 for word in positive_words if word in text_lower)
    negative_count = sum(1 for word in negative_words if word in text_lower)
    
    if positive_count > negative_count:
        return 'POSITIVE'
    elif negative_count > positive_count:
        return 'NEGATIVE'
    else:
        return 'NEUTRAL'
$$;

-- Python UDF 3: Calculate compound interest
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_COMPOUND_INTEREST(
    PRINCIPAL FLOAT, RATE FLOAT, TIME_YEARS FLOAT, COMPOUNDS_PER_YEAR INT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'compound_interest'
AS
$$
def compound_interest(principal, rate, time_years, compounds_per_year):
    """
    Calculate compound interest
    Formula: A = P * (1 + r/n)^(n*t)
    """
    if principal is None or rate is None or time_years is None or compounds_per_year is None:
        return None
    
    if principal <= 0 or compounds_per_year <= 0:
        return None
    
    return principal * (1 + rate / compounds_per_year) ** (compounds_per_year * time_years)
$$;

-- Python UDF 4: Format phone number
CREATE OR REPLACE FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_FORMAT_PHONE(PHONE VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'format_phone'
AS
$$
import re

def format_phone(phone):
    """
    Format phone number to standard format: (XX) XXX-XXXX
    """
    if phone is None or phone == '':
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    if len(digits) < 10:
        return phone  # Return original if not enough digits
    
    # Format as (XX) XXX-XXXX
    if len(digits) == 10:
        return f"({digits[0:2]}) {digits[2:5]}-{digits[5:]}"
    elif len(digits) == 11:
        return f"+{digits[0]} ({digits[1:3]}) {digits[3:6]}-{digits[6:]}"
    else:
        return phone  # Return original if format is not standard
$$;

-- ============================================================================
-- 5.4 TEST PYTHON UDFs
-- ============================================================================

-- Test Python UDF 1: Calculate Distance (example with sample coordinates)
-- SELECT TPCH_ANALYTICS_DB.UDFS.FN_CALCULATE_DISTANCE(40.7128, -74.0060, 34.0522, -118.2437) AS DISTANCE_KM;

-- Test Python UDF 2: Extract Sentiment
SELECT 
    O_ORDERKEY,
    O_COMMENT,
    TPCH_ANALYTICS_DB.UDFS.FN_EXTRACT_SENTIMENT(O_COMMENT) AS SENTIMENT
FROM TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER
WHERE O_COMMENT IS NOT NULL
LIMIT 10;

-- Test Python UDF 3: Compound Interest
-- SELECT TPCH_ANALYTICS_DB.UDFS.FN_COMPOUND_INTEREST(1000, 0.05, 10, 12) AS FUTURE_VALUE;

-- Test Python UDF 4: Format Phone
SELECT 
    C_CUSTKEY,
    C_PHONE AS ORIGINAL_PHONE,
    TPCH_ANALYTICS_DB.UDFS.FN_FORMAT_PHONE(C_PHONE) AS FORMATTED_PHONE
FROM TPCH_ANALYTICS_DB.STAGING.CUSTOMER
LIMIT 10;

-- ============================================================================
-- 5.5 GRANT PERMISSIONS
-- ============================================================================

-- Grant usage on UDFs to roles
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_CUSTOMER_TIER(NUMBER) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_CUSTOMER_TIER(NUMBER) TO ROLE TPCH_ANALYST;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_CUSTOMER_TIER(NUMBER) TO ROLE TPCH_DEVELOPER;

GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_VALIDATE_PHONE(VARCHAR) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_VALIDATE_EMAIL(VARCHAR) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_DAYS_BETWEEN(DATE, DATE) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_FORMAT_CURRENCY(NUMBER) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_YEAR_QUARTER(DATE) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_DISCOUNT_PERCENTAGE(NUMBER, NUMBER) TO ROLE TPCH_ADMIN;

GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_EXTRACT_SENTIMENT(VARCHAR) TO ROLE TPCH_ADMIN;
GRANT USAGE ON FUNCTION TPCH_ANALYTICS_DB.UDFS.FN_FORMAT_PHONE(VARCHAR) TO ROLE TPCH_ADMIN;

-- Show all UDFs
SHOW USER FUNCTIONS IN SCHEMA TPCH_ANALYTICS_DB.UDFS;

