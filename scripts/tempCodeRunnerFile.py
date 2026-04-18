import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.config import *
from scripts.ingestion import read_datasets
from scripts.cleaning import clean_data
from scripts.feature_engineering import add_features
from scripts.build_tables import *
from scripts.quality_checks import run_quality_checks


# ---------------------------------------------------
# Spark Session
# ---------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Olist Ecommerce Analytics Pipeline")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ===================================================
# STEP 1 : DATA INGESTION
# ===================================================

print("\n=================================================")
print("STEP 1 : DATA INGESTION")
print("Loading Olist datasets into Spark DataFrames")
print("=================================================\n")

(
    customers,
    orders,
    order_items,
    products,
    payments,
    reviews,
    sellers,
    category_translation
) = read_datasets(spark, RAW_DATA_PATH)

print("Customers Sample")
customers.show(5)

print("Orders Sample")
orders.show(5)


# ===================================================
# STEP 2 : DATA INTEGRATION
# ===================================================

print("\n=================================================")
print("STEP 2 : DATA INTEGRATION")
print("Joining multiple datasets to create unified dataset")
print("=================================================\n")

df = (
    orders
    .join(order_items, "order_id")
    .join(products, "product_id", "left")
    .join(customers, "customer_id", "left")
    .join(payments, "order_id", "left")
    .join(reviews, "order_id", "left")
    .join(sellers, "seller_id", "left")
    .join(category_translation, "product_category_name", "left")
)

print("Rows after join:", df.count())
df.show(5)


# ===================================================
# STEP 3 : DATA CLEANING
# ===================================================

print("\n=================================================")
print("STEP 3 : DATA CLEANING")
print("Removing invalid values and handling missing data")
print("=================================================\n")

df = clean_data(df)

run_quality_checks(df)


# ===================================================
# STEP 4 : FEATURE ENGINEERING
# ===================================================

print("\n=================================================")
print("STEP 4 : FEATURE ENGINEERING")
print("Creating analytical features such as revenue and delivery time")
print("=================================================\n")

df = add_features(df)

df.select(
    "price",
    "freight_value",
    "revenue",
    "review_score"
).show(10)


# ===================================================
# STEP 5 : SALES SUMMARY
# ===================================================

print("\n=================================================")
print("STEP 5 : SALES SUMMARY")
print("Generating overall business metrics")
print("=================================================\n")

sales_summary = build_sales_summary(df)

sales_summary.show()


# ===================================================
# STEP 6 : CATEGORY REVENUE ANALYSIS
# ===================================================

print("\n=================================================")
print("STEP 6 : CATEGORY REVENUE ANALYSIS")
print("Identifying top performing product categories")
print("=================================================\n")

category_revenue = build_category_revenue(df)

category_revenue.show(10)


# ===================================================
# STEP 7 : TOP PRODUCTS
# ===================================================

print("\n=================================================")
print("STEP 7 : TOP PRODUCTS ANALYSIS")
print("Finding best selling products based on number of sales")
print("=================================================\n")

top_products = build_top_products(df)

top_products.show(10)


# ===================================================
# STEP 8 : SELLER PERFORMANCE
# ===================================================

print("\n=================================================")
print("STEP 8 : SELLER PERFORMANCE ANALYSIS")
print("Evaluating sellers based on revenue generated")
print("=================================================\n")

seller_performance = build_seller_performance(df)

seller_performance.show(10)


# ===================================================
# STEP 9 : DELIVERY PERFORMANCE
# ===================================================

print("\n=================================================")
print("STEP 9 : DELIVERY PERFORMANCE")
print("Analyzing average delivery time by seller location")
print("=================================================\n")

delivery_performance = build_delivery_performance(df)

delivery_performance.show(10)


spark.stop()