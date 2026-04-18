from pyspark.sql.functions import avg, count, sum, col


def run_analytics(df):

    print("Revenue by Category")

    df.groupBy("product_category_name_english") \
        .agg(sum("price").alias("revenue")) \
        .orderBy(col("revenue").desc()) \
        .show(10)

    print("Top Selling Products")

    df.groupBy("product_id") \
        .agg(count("*").alias("sales")) \
        .orderBy(col("sales").desc()) \
        .show(10)

    print("Average Review Score")

    df.agg(avg("review_score").alias("avg_rating")).show()