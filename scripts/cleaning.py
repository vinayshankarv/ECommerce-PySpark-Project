from pyspark.sql.functions import col


def clean_data(df):

    # -----------------------------
    # 🔥 FIX: CAST TO CORRECT TYPES
    # -----------------------------

    df = df.withColumn("price", col("price").cast("double"))
    df = df.withColumn("freight_value", col("freight_value").cast("double"))
    df = df.withColumn("review_score", col("review_score").cast("double"))

    # -----------------------------
    # Existing Cleaning
    # -----------------------------

    df = df.filter((col("price") > 0) & (col("price") < 10000))

    df = df.filter((col("review_score") >= 1) & (col("review_score") <= 5))

    df = df.dropDuplicates(["order_id", "product_id"])

    df = df.fillna({"product_category_name_english": "Unknown"})

    return df