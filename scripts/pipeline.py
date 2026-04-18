import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

from config.config import *
from scripts.ingestion import read_datasets
from scripts.cleaning import clean_data
from scripts.feature_engineering import add_features
from scripts.build_tables import *
from scripts.quality_checks import run_quality_checks
from scripts.ml_model import train_delay_model
from scripts.visualization import generate_all_plots
from utils.logger import get_logger

logger = get_logger("pipeline")


# =========================================================
# SPARK SESSION
# =========================================================
def create_spark_session():

    warehouse_path = os.path.abspath("spark-warehouse")

    spark = (
        SparkSession.builder
        .appName("Olist Ecommerce Pipeline - Research Version")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Spark session initialized")

    return spark


# =========================================================
# SAVE HELPER
# =========================================================
def save_hive_table(df, table_name):

    df.write.mode("overwrite").saveAsTable(table_name)
    logger.info(f"Saved table: {table_name}")


# =========================================================
# MODEL RUNNER
# =========================================================
def run_models(train_df, test_df):

    results = {}

    for model_type in ["gbt", "rf"]:

        logger.info(f"Running model: {model_type.upper()}")

        preds = train_delay_model(
            train_df,
            test_df,
            model_type=model_type
        )

        results[model_type] = preds

    return results


# =========================================================
# MAIN PIPELINE
# =========================================================
def main():

    print("🚀 PIPELINE STARTED")

    # 🔥 Ensure output folder exists
    os.makedirs("outputs", exist_ok=True)

    spark = create_spark_session()

    # -----------------------------------------------------
    # DATABASE
    # -----------------------------------------------------
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_db")
    spark.sql("USE ecommerce_db")

    logger.info("Database ready")

    # -----------------------------------------------------
    # INGESTION
    # -----------------------------------------------------
    datasets = read_datasets(spark, RAW_DATA_PATH)

    (customers, orders, order_items,
     products, payments, reviews,
     sellers, category_translation) = datasets

    logger.info("Data loaded")

    # -----------------------------------------------------
    # JOIN
    # -----------------------------------------------------
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

    logger.info("Join complete")

    # -----------------------------------------------------
    # CLEAN + FEATURE ENGINEERING
    # -----------------------------------------------------
    df = clean_data(df)
    df = add_features(df)

    df = df.cache()
    df.count()

    logger.info("Feature engineering complete")

    # -----------------------------------------------------
    # SPLIT (NO LEAKAGE)
    # -----------------------------------------------------
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    logger.info(f"Train: {train_df.count()} | Test: {test_df.count()}")

    # -----------------------------------------------------
    # AGG FEATURES (FIXED JOIN BUG 🔥)
    # -----------------------------------------------------
    seller_stats = train_df.groupBy("seller_id").agg(
        avg("is_delayed").alias("seller_delay_rate"),
        avg("processing_days").alias("seller_avg_processing")
    ).cache()

    category_stats = train_df.groupBy("product_category_name_english").agg(
        avg("is_delayed").alias("category_delay_rate")
    ).cache()

    # ✅ CORRECT JOIN (your previous loop was ineffective)
    train_df = train_df.join(seller_stats, "seller_id", "left") \
                       .join(category_stats, "product_category_name_english", "left")

    test_df = test_df.join(seller_stats, "seller_id", "left") \
                     .join(category_stats, "product_category_name_english", "left")

    train_df = train_df.fillna(0)
    test_df = test_df.fillna(0)

    logger.info("Aggregated features added")

    # -----------------------------------------------------
    # MODEL TRAINING
    # -----------------------------------------------------
    model_outputs = run_models(train_df, test_df)

    pred_gbt = model_outputs["gbt"]
    pred_rf = model_outputs["rf"]

    # 🔥 FINAL MODEL SELECTION
    predictions = pred_gbt

    # -----------------------------------------------------
    # 🔥 VISUALIZATION (ADDED CORRECTLY)
    # -----------------------------------------------------
    generate_all_plots(predictions)

    logger.info("Visualization completed")

    # -----------------------------------------------------
    # QUALITY CHECKS
    # -----------------------------------------------------
    run_quality_checks(df)

    # -----------------------------------------------------
    # ANALYTICS
    # -----------------------------------------------------
    sales_summary = build_sales_summary(df)
    category_revenue = build_category_revenue(df)
    top_products = build_top_products(df)
    seller_performance = build_seller_performance(df)
    delivery_performance = build_delivery_performance(df)
    monthly_sales = build_monthly_sales(df)
    state_sales = build_state_sales(df)
    payment_analysis = build_payment_analysis(df)

    delay_summary = build_delay_summary(predictions)
    high_risk_sellers = build_high_risk_sellers(predictions)
    category_delay = build_category_delay(predictions)

    # -----------------------------------------------------
    # SAVE
    # -----------------------------------------------------
    logger.info("Saving outputs")

    save_hive_table(df, "olist_processed")

    save_hive_table(pred_gbt, "delay_predictions_gbt")
    save_hive_table(pred_rf, "delay_predictions_rf")
    save_hive_table(predictions, "delay_predictions")

    save_hive_table(sales_summary, "sales_summary")
    save_hive_table(category_revenue, "category_revenue")
    save_hive_table(top_products, "top_products")
    save_hive_table(seller_performance, "seller_performance")
    save_hive_table(delivery_performance, "delivery_performance")
    save_hive_table(monthly_sales, "monthly_sales")
    save_hive_table(state_sales, "state_sales")
    save_hive_table(payment_analysis, "payment_analysis")

    save_hive_table(delay_summary, "delay_summary")
    save_hive_table(high_risk_sellers, "high_risk_sellers")
    save_hive_table(category_delay, "category_delay")

    # -----------------------------------------------------
    # SAMPLE OUTPUT
    # -----------------------------------------------------
    spark.sql("""
        SELECT seller_id,
               AVG(adjusted_prediction) AS avg_predicted_delay_rate
        FROM delay_predictions
        GROUP BY seller_id
        ORDER BY avg_predicted_delay_rate DESC
        LIMIT 10
    """).show()

    logger.info("Pipeline finished successfully")

    # -----------------------------------------------------
    # CLEANUP
    # -----------------------------------------------------
    spark.catalog.clearCache()
    spark.stop()


# =========================================================
# ENTRY
# =========================================================
if __name__ == "__main__":
    main()