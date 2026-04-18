from pyspark.sql.functions import (
    sum,
    avg,
    count,
    col,
    year,
    month,
    datediff
)


# ---------------------------------------------------------
# Sales Summary
# ---------------------------------------------------------

def build_sales_summary(df):

    df = df.withColumn("revenue", col("price") + col("freight_value"))

    return df.agg(
        count("order_id").alias("total_orders"),
        sum("revenue").alias("total_revenue"),
        avg("revenue").alias("avg_order_value"),
        avg("review_score").alias("avg_rating")
    )


# ---------------------------------------------------------
# Category Revenue
# ---------------------------------------------------------

def build_category_revenue(df):

    df = df.withColumn("revenue", col("price") + col("freight_value"))

    return df.groupBy(
        "product_category_name_english"
    ).agg(
        sum("revenue").alias("revenue")
    ).orderBy(col("revenue").desc())


# ---------------------------------------------------------
# Top Products
# ---------------------------------------------------------

def build_top_products(df):

    return df.groupBy("product_id").agg(
        count("*").alias("total_sales")
    ).orderBy(col("total_sales").desc()).limit(10)


# ---------------------------------------------------------
# Seller Performance
# ---------------------------------------------------------

def build_seller_performance(df):

    df = df.withColumn("revenue", col("price") + col("freight_value"))

    return df.groupBy("seller_id").agg(
        sum("revenue").alias("revenue")
    ).orderBy(col("revenue").desc())


# ---------------------------------------------------------
# Delivery Performance
# ---------------------------------------------------------

def build_delivery_performance(df):

    df = df.withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )

    return df.groupBy("seller_city").agg(
        avg("delivery_days").alias("avg_delivery_days")
    ).orderBy(col("avg_delivery_days").desc())


# ---------------------------------------------------------
# Monthly Revenue
# ---------------------------------------------------------

def build_monthly_sales(df):

    df = df.withColumn("revenue", col("price") + col("freight_value"))

    return df.groupBy(
        year("order_purchase_timestamp").alias("year"),
        month("order_purchase_timestamp").alias("month")
    ).agg(
        sum("revenue").alias("total_revenue")
    ).orderBy("year", "month")


# ---------------------------------------------------------
# Revenue by State
# ---------------------------------------------------------

def build_state_sales(df):

    df = df.withColumn("revenue", col("price") + col("freight_value"))

    return df.groupBy("customer_state").agg(
        sum("revenue").alias("revenue")
    ).orderBy(col("revenue").desc())


# ---------------------------------------------------------
# Payment Analysis
# ---------------------------------------------------------

def build_payment_analysis(df):

    return df.groupBy("payment_type").agg(
        count("*").alias("total_orders")
    ).orderBy(col("total_orders").desc())


# =========================================================
# 🔥 NEW TABLES (REVIEW 3 UPGRADE)
# =========================================================


# ---------------------------------------------------------
# Delay Prediction Summary
# ---------------------------------------------------------

def build_delay_summary(predictions):

    return predictions.groupBy("prediction").agg(
        count("*").alias("total_orders")
    )


# ---------------------------------------------------------
# High-Risk Sellers 
# ---------------------------------------------------------

def build_high_risk_sellers(predictions):

    return predictions.groupBy("seller_id").agg(
        avg("prediction").alias("delay_risk")
    ).orderBy(col("delay_risk").desc()).limit(10)


# ---------------------------------------------------------
# Category Delay Analysis
# ---------------------------------------------------------

def build_category_delay(predictions):

    return predictions.groupBy(
        "product_category_name_english"
    ).agg(
        avg("prediction").alias("delay_rate")
    ).orderBy(col("delay_rate").desc())