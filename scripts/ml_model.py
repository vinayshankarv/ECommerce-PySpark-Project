from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import GBTClassifier, RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql.functions import when, col
from pyspark.ml.functions import vector_to_array


def train_delay_model(train_df, test_df, model_type="gbt"):

    print(f"\n===== TRAINING MODEL: {model_type.upper()} =====")

    # =================================================
    # FEATURES
    # =================================================

    feature_cols = [
        "price",
        "freight_value",
        "processing_days",
        "estimated_days",
        "freight_ratio",
        "delivery_pressure",
        "high_freight_flag",
        "high_price_flag",
        "slow_processing_flag",
        "seller_delay_rate",
        "seller_avg_processing",
        "category_delay_rate",
        "order_month",
        "order_dayofweek"
    ]

    # Drop nulls
    train_df = train_df.dropna(subset=feature_cols + ["is_delayed"])
    test_df = test_df.dropna(subset=feature_cols + ["is_delayed"])

    # =================================================
    # HANDLE CLASS IMBALANCE
    # =================================================

    train_df = train_df.withColumn(
        "class_weight",
        when(col("is_delayed") == 1, 5.0).otherwise(1.0)
    )

    # =================================================
    # FEATURE ASSEMBLY
    # =================================================

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features"
    )

    # =================================================
    # MODEL SELECTION
    # =================================================

    if model_type == "gbt":

        model_algo = GBTClassifier(
            labelCol="is_delayed",
            featuresCol="features",
            weightCol="class_weight",
            maxIter=50,
            maxDepth=7
        )

    elif model_type == "rf":

        model_algo = RandomForestClassifier(
            labelCol="is_delayed",
            featuresCol="features",
            weightCol="class_weight",
            numTrees=100,
            maxDepth=10
        )

    else:
        raise ValueError("Invalid model_type. Use 'gbt' or 'rf'")

    pipeline = Pipeline(stages=[assembler, model_algo])

    # =================================================
    # TRAIN
    # =================================================

    model = pipeline.fit(train_df)

    # =================================================
    # PREDICT
    # =================================================

    predictions = model.transform(test_df)

    # Extract probability
    predictions = predictions.withColumn(
        "delay_probability",
        vector_to_array(col("probability"))[1]
    )

    # =================================================
    # THRESHOLD TUNING
    # =================================================

    threshold = 0.2

    predictions = predictions.withColumn(
        "adjusted_prediction",
        when(col("delay_probability") > threshold, 1).otherwise(0)
    )

    # =================================================
    # EVALUATION
    # =================================================

    evaluator = BinaryClassificationEvaluator(
        labelCol="is_delayed",
        metricName="areaUnderROC"
    )

    auc = evaluator.evaluate(predictions)
    print(f"\nAUC: {auc:.4f}")

    # =================================================
    # CONFUSION MATRIX
    # =================================================

    print("\n===== CONFUSION MATRIX (ADJUSTED) =====")

    cm_df = predictions.groupBy("is_delayed", "adjusted_prediction").count()
    cm_df.show()

    cm = {(row["is_delayed"], row["adjusted_prediction"]): row["count"] for row in cm_df.collect()}

    TP = cm.get((1, 1), 0)
    TN = cm.get((0, 0), 0)
    FP = cm.get((0, 1), 0)
    FN = cm.get((1, 0), 0)

    # =================================================
    # METRICS
    # =================================================

    precision = TP / (TP + FP) if (TP + FP) else 0
    recall = TP / (TP + FN) if (TP + FN) else 0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) else 0

    print(f"\nPrecision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1 Score:  {f1:.4f}")

    # =================================================
    # FEATURE IMPORTANCE
    # =================================================

    print("\n===== FEATURE IMPORTANCE =====")

    model_stage = model.stages[-1]

    try:
        importances = model_stage.featureImportances

        for i, col_name in enumerate(feature_cols):
            print(f"{col_name}: {importances[i]:.4f}")

    except:
        print("Feature importance not available for this model")

    # =================================================
    # FINAL OUTPUT
    # =================================================

    return predictions.select(
        "order_id",
        "seller_id",
        "product_category_name_english",
        "prediction",
        "adjusted_prediction",
        "delay_probability",
        "is_delayed"
    )