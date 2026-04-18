def read_datasets(spark, data_path):

    customers = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_customers_dataset.csv")

    orders = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_orders_dataset.csv")

    order_items = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_order_items_dataset.csv")

    products = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_products_dataset.csv")

    payments = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_order_payments_dataset.csv")

    reviews = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_order_reviews_dataset.csv")

    sellers = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/olist_sellers_dataset.csv")

    category_translation = spark.read \
        .option("header", True) \
        .csv(f"{data_path}/product_category_name_translation.csv")

    return (
        customers,
        orders,
        order_items,
        products,
        payments,
        reviews,
        sellers,
        category_translation
    )