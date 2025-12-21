"""
============================================================================
PHẦN 5: SNOWPARK PYTHON ANALYTICS & UDFs
============================================================================
TPC-H Analytics Project - Snowpark Python for Advanced Analytics
============================================================================

Yêu cầu:
- Cài đặt snowflake-snowpark-python: pip install snowflake-snowpark-python
- Có file config.json hoặc environment variables để kết nối Snowflake
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, count, sum as sum_, avg, max as max_, min as min_
from snowflake.snowpark.functions import date_trunc, datediff, current_date
from snowflake.snowpark.types import *
import json
import pandas as pd
from datetime import datetime


# ============================================================================
# CONNECTION CONFIGURATION
# ============================================================================

def create_session():
    """
    Tạo Snowpark session từ connection parameters
    Bạn có thể load từ file config.json hoặc environment variables
    """
    connection_parameters = {
        "account": "<your_account>",  # Thay bằng account của bạn
        "user": "<your_user>",         # Thay bằng username
        "password": "<your_password>", # Thay bằng password
        "warehouse": "COMPUTE_WH",     # Thay bằng warehouse name
        "database": "TPCH_ANALYTICS_DB",
        "schema": "ANALYTICS",
        "role": "TPCH_ADMIN"           # Hoặc role có quyền phù hợp
    }
    
    # Hoặc load từ file config.json
    # with open('config.json', 'r') as f:
    #     connection_parameters = json.load(f)
    
    session = Session.builder.configs(connection_parameters).create()
    return session


# ============================================================================
# 5.1 CUSTOMER SEGMENTATION VỚI RFM
# ============================================================================

def customer_rfm_segmentation(session):
    """
    Customer RFM Segmentation using Snowpark
    RFM = Recency, Frequency, Monetary
    """
    print("\n" + "="*80)
    print("5.1 CUSTOMER RFM SEGMENTATION")
    print("="*80)
    
    # Load tables
    customers = session.table("TPCH_ANALYTICS_DB.SILVER.CUSTOMER_SILVER")
    orders = session.table("TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER")
    
    # Calculate RFM metrics
    rfm_df = (customers
        .join(orders, customers["C_CUSTKEY"] == orders["O_CUSTKEY"], "left")
        .group_by(customers["C_CUSTKEY"], customers["C_NAME"])
        .agg([
            max_("O_ORDERDATE").alias("LAST_ORDER_DATE"),
            count("O_ORDERKEY").alias("FREQUENCY"),
            sum_("O_TOTALPRICE").alias("MONETARY")
        ])
        .with_column("RECENCY_DAYS", 
            datediff("day", col("LAST_ORDER_DATE"), current_date()))
        .select(
            col("C_CUSTKEY"),
            col("C_NAME"),
            col("LAST_ORDER_DATE"),
            col("RECENCY_DAYS"),
            col("FREQUENCY"),
            col("MONETARY")
        )
    )
    
    # Calculate RFM scores (1-5 scale)
    # Recency: Lower is better (recent = high score)
    # Frequency: Higher is better
    # Monetary: Higher is better
    
    # Get percentiles for scoring
    rfm_stats = rfm_df.agg([
        avg("RECENCY_DAYS").alias("AVG_RECENCY"),
        avg("FREQUENCY").alias("AVG_FREQUENCY"),
        avg("MONETARY").alias("AVG_MONETARY")
    ]).collect()[0]
    
    avg_recency = rfm_stats["AVG_RECENCY"]
    avg_frequency = rfm_stats["AVG_FREQUENCY"]
    avg_monetary = rfm_stats["AVG_MONETARY"]
    
    # Add RFM scores
    from snowflake.snowpark.functions import when
    
    rfm_scored = rfm_df.with_column(
        "R_SCORE",
        when(col("RECENCY_DAYS") <= avg_recency * 0.2, 5)
        .when(col("RECENCY_DAYS") <= avg_recency * 0.4, 4)
        .when(col("RECENCY_DAYS") <= avg_recency * 0.6, 3)
        .when(col("RECENCY_DAYS") <= avg_recency * 0.8, 2)
        .otherwise(1)
    ).with_column(
        "F_SCORE",
        when(col("FREQUENCY") >= avg_frequency * 1.8, 5)
        .when(col("FREQUENCY") >= avg_frequency * 1.4, 4)
        .when(col("FREQUENCY") >= avg_frequency * 1.0, 3)
        .when(col("FREQUENCY") >= avg_frequency * 0.6, 2)
        .otherwise(1)
    ).with_column(
        "M_SCORE",
        when(col("MONETARY") >= avg_monetary * 1.8, 5)
        .when(col("MONETARY") >= avg_monetary * 1.4, 4)
        .when(col("MONETARY") >= avg_monetary * 1.0, 3)
        .when(col("MONETARY") >= avg_monetary * 0.6, 2)
        .otherwise(1)
    ).with_column(
        "RFM_SCORE",
        (col("R_SCORE").cast("INTEGER") * 100 + 
         col("F_SCORE").cast("INTEGER") * 10 + 
         col("M_SCORE").cast("INTEGER"))
    )
    
    # Add segment based on RFM score
    rfm_final = rfm_scored.with_column(
        "RFM_SEGMENT",
        when((col("R_SCORE") >= 4) & (col("F_SCORE") >= 4) & (col("M_SCORE") >= 4), "Champions")
        .when((col("R_SCORE") >= 4) & (col("F_SCORE") >= 3) & (col("M_SCORE") >= 3), "Loyal Customers")
        .when((col("R_SCORE") >= 3) & (col("F_SCORE") >= 4) & (col("M_SCORE") >= 4), "Potential Loyalists")
        .when((col("R_SCORE") >= 4) & (col("F_SCORE") <= 2) & (col("M_SCORE") <= 2), "New Customers")
        .when((col("R_SCORE") >= 3) & (col("F_SCORE") <= 2) & (col("M_SCORE") <= 2), "Promising")
        .when((col("R_SCORE") <= 2) & (col("F_SCORE") >= 4) & (col("M_SCORE") >= 4), "Need Attention")
        .when((col("R_SCORE") <= 2) & (col("F_SCORE") >= 3) & (col("M_SCORE") >= 3), "About to Sleep")
        .otherwise("At Risk")
    )
    
    # Save to table
    rfm_final.write.mode("overwrite").save_as_table("TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_RFM_SCORES")
    
    print(f"✅ RFM Segmentation completed!")
    print(f"   Total customers processed: {rfm_final.count()}")
    
    # Show sample
    print("\nSample RFM Results:")
    rfm_final.show(10)
    
    # Show segment distribution
    print("\nRFM Segment Distribution:")
    segment_dist = rfm_final.group_by("RFM_SEGMENT").agg(count("*").alias("CUSTOMER_COUNT")).sort(col("CUSTOMER_COUNT").desc())
    segment_dist.show()
    
    return rfm_final


# ============================================================================
# 5.2 SALES TREND ANALYSIS
# ============================================================================

def analyze_sales_trend(session):
    """
    Analyze monthly sales trends using Snowpark
    """
    print("\n" + "="*80)
    print("5.2 SALES TREND ANALYSIS")
    print("="*80)
    
    # Load orders table
    orders = session.table("TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER")
    
    # Monthly aggregation
    monthly_sales = (orders
        .with_column("MONTH", date_trunc("month", col("O_ORDERDATE")))
        .group_by("MONTH")
        .agg([
            count("O_ORDERKEY").alias("ORDER_COUNT"),
            sum_("O_TOTALPRICE").alias("TOTAL_REVENUE"),
            avg("O_TOTALPRICE").alias("AVG_ORDER_VALUE"),
            min_("O_TOTALPRICE").alias("MIN_ORDER_VALUE"),
            max_("O_TOTALPRICE").alias("MAX_ORDER_VALUE")
        ])
        .sort("MONTH")
    )
    
    # Save to table
    monthly_sales.write.mode("overwrite").save_as_table("TPCH_ANALYTICS_DB.ANALYTICS.MONTHLY_SALES_TREND")
    
    print(f"✅ Sales Trend Analysis completed!")
    print(f"   Total months analyzed: {monthly_sales.count()}")
    
    # Show results
    print("\nMonthly Sales Trend:")
    monthly_sales.show()
    
    # Convert to pandas for visualization (optional)
    df_pandas = monthly_sales.to_pandas()
    print(f"\nPandas DataFrame shape: {df_pandas.shape}")
    
    return monthly_sales


# ============================================================================
# 5.3 PRODUCT PERFORMANCE ANALYSIS
# ============================================================================

def analyze_product_performance(session):
    """
    Analyze product performance with supplier information
    """
    print("\n" + "="*80)
    print("5.3 PRODUCT PERFORMANCE ANALYSIS")
    print("="*80)
    
    # Load tables
    lineitem = session.table("TPCH_ANALYTICS_DB.SILVER.LINEITEM_SILVER")
    orders = session.table("TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER")
    
    # Product performance analysis
    product_perf = (lineitem
        .join(orders, lineitem["L_ORDERKEY"] == orders["O_ORDERKEY"], "inner")
        .group_by(
            col("L_PARTKEY"),
            col("L_PART_NAME"),
            col("L_PART_TYPE"),
            col("L_SUPPKEY"),
            col("L_SUPPLIER_NAME")
        )
        .agg([
            sum_("L_QUANTITY").alias("TOTAL_QUANTITY_SOLD"),
            count("L_ORDERKEY").alias("ORDER_COUNT"),
            sum_("L_FINAL_PRICE").alias("TOTAL_REVENUE"),
            avg("L_FINAL_PRICE").alias("AVG_ITEM_PRICE"),
            avg("L_DISCOUNT").alias("AVG_DISCOUNT")
        ])
        .sort(col("TOTAL_REVENUE").desc())
        .limit(100)  # Top 100 products
    )
    
    # Save to table
    product_perf.write.mode("overwrite").save_as_table("TPCH_ANALYTICS_DB.ANALYTICS.TOP_PRODUCTS_PERFORMANCE")
    
    print(f"✅ Product Performance Analysis completed!")
    print(f"   Top 100 products analyzed")
    
    # Show top 10
    print("\nTop 10 Products by Revenue:")
    product_perf.limit(10).show()
    
    return product_perf


# ============================================================================
# 5.4 CUSTOMER RETENTION ANALYSIS
# ============================================================================

def analyze_customer_retention(session):
    """
    Analyze customer retention by calculating repeat purchase rate
    """
    print("\n" + "="*80)
    print("5.4 CUSTOMER RETENTION ANALYSIS")
    print("="*80)
    
    # Load tables
    orders = session.table("TPCH_ANALYTICS_DB.SILVER.ORDERS_SILVER")
    
    # Calculate customer order counts
    customer_orders = (orders
        .group_by("O_CUSTKEY")
        .agg([
            count("O_ORDERKEY").alias("ORDER_COUNT"),
            min_("O_ORDERDATE").alias("FIRST_ORDER_DATE"),
            max_("O_ORDERDATE").alias("LAST_ORDER_DATE"),
            sum_("O_TOTALPRICE").alias("TOTAL_SPENT")
        ])
        .with_column(
            "IS_REPEAT_CUSTOMER",
            when(col("ORDER_COUNT") > 1, 1).otherwise(0)
        )
    )
    
    # Calculate retention metrics
    retention_metrics = customer_orders.agg([
        count("*").alias("TOTAL_CUSTOMERS"),
        sum_("IS_REPEAT_CUSTOMER").alias("REPEAT_CUSTOMERS"),
        avg("ORDER_COUNT").alias("AVG_ORDERS_PER_CUSTOMER"),
        avg("TOTAL_SPENT").alias("AVG_CUSTOMER_VALUE")
    ]).with_column(
        "RETENTION_RATE",
        (col("REPEAT_CUSTOMERS") / col("TOTAL_CUSTOMERS") * 100)
    )
    
    # Save to table
    customer_orders.write.mode("overwrite").save_as_table("TPCH_ANALYTICS_DB.ANALYTICS.CUSTOMER_RETENTION")
    
    print(f"✅ Customer Retention Analysis completed!")
    
    # Show retention metrics
    print("\nCustomer Retention Metrics:")
    retention_metrics.show()
    
    # Show distribution
    print("\nOrder Count Distribution:")
    order_dist = customer_orders.group_by("ORDER_COUNT").agg(count("*").alias("CUSTOMER_COUNT")).sort("ORDER_COUNT")
    order_dist.show(10)
    
    return customer_orders, retention_metrics


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main function to run all Snowpark analyses
    """
    print("="*80)
    print("TPC-H ANALYTICS PROJECT - SNOWPARK PYTHON ANALYTICS")
    print("="*80)
    
    try:
        # Create session
        session = create_session()
        print("\n✅ Connected to Snowflake successfully!")
        
        # Run analyses
        rfm_result = customer_rfm_segmentation(session)
        sales_trend = analyze_sales_trend(session)
        product_perf = analyze_product_performance(session)
        retention_result, retention_metrics = analyze_customer_retention(session)
        
        print("\n" + "="*80)
        print("ALL ANALYSES COMPLETED SUCCESSFULLY!")
        print("="*80)
        
        # Close session
        session.close()
        print("\n✅ Session closed.")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

