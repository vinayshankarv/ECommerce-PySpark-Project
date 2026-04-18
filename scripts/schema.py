from pyspark.sql.types import *

amazon_schema = StructType([

    StructField("asin", StringType(), True),
    StructField("title", StringType(), True),
    StructField("imgUrl", StringType(), True),
    StructField("productURL", StringType(), True),
    StructField("stars", DoubleType(), True),
    StructField("reviews", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("listPrice", DoubleType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("isBestSeller", StringType(), True),
    StructField("boughtInLastMonth", StringType(), True)

])

flipkart_schema = StructType([

    StructField("uniq_id", StringType(), True),
    StructField("crawl_timestamp", StringType(), True),
    StructField("product_url", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category_tree", StringType(), True),
    StructField("pid", StringType(), True),
    StructField("retail_price", DoubleType(), True),
    StructField("discounted_price", DoubleType(), True),
    StructField("image", StringType(), True),
    StructField("is_FK_Advantage_product", StringType(), True),
    StructField("description", StringType(), True),
    StructField("product_rating", DoubleType(), True),
    StructField("overall_rating", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("product_specifications", StringType(), True)

])