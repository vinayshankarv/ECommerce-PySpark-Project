from pyspark.sql.functions import col


def run_quality_checks(df):

    print("\n===== DATA QUALITY CHECKS =====")

    print("Total rows:", df.count())

    print("Null price:", df.filter(col("price").isNull()).count())

    print("Null review score:",
          df.filter(col("review_score").isNull()).count())

    print("Invalid price:",
          df.filter(col("price") <= 0).count())

    print("Invalid review score:",
          df.filter(col("review_score") > 5).count())

    print("===== QUALITY CHECK COMPLETE =====\n")