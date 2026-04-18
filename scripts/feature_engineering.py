from pyspark.sql.functions import (
    when, col, datediff,
    month, dayofweek
)


def add_features(df):

    # =================================================
    # BASIC FEATURES
    # =================================================

    df = df.withColumn(
        "rating_category",
        when(col("review_score") >= 4.5, "Excellent")
        .when(col("review_score") >= 4.0, "Good")
        .when(col("review_score") >= 3.0, "Average")
        .otherwise("Low")
    )

    df = df.withColumn(
        "price_bucket",
        when(col("price") < 50, "Budget")
        .when(col("price") < 200, "Mid")
        .otherwise("Premium")
    )

    # =================================================
    # TIME FEATURES
    # =================================================

    df = df.withColumn(
        "order_month",
        month(col("order_purchase_timestamp"))
    )

    df = df.withColumn(
        "order_dayofweek",
        dayofweek(col("order_purchase_timestamp"))
    )

    # =================================================
    # DELIVERY FEATURES
    # =================================================

    df = df.withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )

    df = df.withColumn(
        "processing_days",
        datediff(
            col("order_approved_at"),
            col("order_purchase_timestamp")
        )
    )

    df = df.withColumn(
        "estimated_days",
        datediff(
            col("order_estimated_delivery_date"),
            col("order_purchase_timestamp")
        )
    )

    # =================================================
    # DERIVED FEATURES (MODEL SIGNALS)
    # =================================================

    # Delivery pressure (buffer time)
    df = df.withColumn(
        "delivery_pressure",
        col("estimated_days") - col("processing_days")
    )

    # Safe freight ratio
    df = df.withColumn(
        "freight_ratio",
        when(col("price") > 0,
             col("freight_value") / col("price")
        ).otherwise(0)
    )

    # Binary flags (tree-friendly features)
    df = df.withColumn(
        "high_freight_flag",
        when(col("freight_ratio") > 0.2, 1).otherwise(0)
    )

    df = df.withColumn(
        "high_price_flag",
        when(col("price") > 200, 1).otherwise(0)
    )

    df = df.withColumn(
        "slow_processing_flag",
        when(col("processing_days") > 3, 1).otherwise(0)
    )

    # =================================================
    # TARGET VARIABLE
    # =================================================

    df = df.withColumn(
        "is_delayed",
        when(
            col("order_delivered_customer_date") >
            col("order_estimated_delivery_date"),
            1
        ).otherwise(0)
    )

    # =================================================
    # NULL HANDLING (STRICT)
    # =================================================

    df = df.fillna({
        "processing_days": 0,
        "estimated_days": 0,
        "delivery_pressure": 0,
        "freight_ratio": 0,
        "delivery_days": 0
    })

    return df