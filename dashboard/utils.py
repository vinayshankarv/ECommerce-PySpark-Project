import pandas as pd
from pyspark.sql import SparkSession


# -------------------------------------------------
# Create Spark Session for Hive
# -------------------------------------------------

def get_spark():

    spark = (
        SparkSession.builder
        .appName("Streamlit Hive Reader")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark


# -------------------------------------------------
# Load Tables from Hive
# -------------------------------------------------

def load_tables():

    spark = get_spark()

    # Use your database
    spark.sql("USE ecommerce_db")

    # -------------------------
    # Existing tables
    # -------------------------

    sales_summary = spark.table("sales_summary").toPandas()
    category_revenue = spark.table("category_revenue").toPandas()
    top_products = spark.table("top_products").toPandas()
    seller_performance = spark.table("seller_performance").toPandas()
    delivery_performance = spark.table("delivery_performance").toPandas()
    monthly_sales = spark.table("monthly_sales").toPandas()
    state_sales = spark.table("state_sales").toPandas()
    payment_analysis = spark.table("payment_analysis").toPandas()

    # -------------------------
    # 🔥 ML Tables (Review 3)
    # -------------------------

    delay_summary = spark.table("delay_summary").toPandas()
    high_risk_sellers = spark.table("high_risk_sellers").toPandas()
    category_delay = spark.table("category_delay").toPandas()

    return (
        sales_summary,
        category_revenue,
        top_products,
        seller_performance,
        delivery_performance,
        monthly_sales,
        state_sales,
        payment_analysis,
        delay_summary,
        high_risk_sellers,
        category_delay
    )