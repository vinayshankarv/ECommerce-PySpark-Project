import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import (
    roc_curve, auc,
    precision_recall_curve,
    confusion_matrix
)


# =========================================================
# PREPARE DATA (Spark → Pandas)
# =========================================================
def prepare_data(predictions_df):

    pdf = predictions_df.select(
        "is_delayed",
        "delay_probability",
        "adjusted_prediction"
    ).toPandas()

    return pdf


# =========================================================
# ROC CURVE
# =========================================================
def plot_roc_curve(pdf):

    fpr, tpr, _ = roc_curve(
        pdf["is_delayed"],
        pdf["delay_probability"]
    )

    roc_auc = auc(fpr, tpr)

    plt.figure()
    plt.plot(fpr, tpr, label=f"AUC = {roc_auc:.3f}")
    plt.plot([0, 1], [0, 1], linestyle="--")

    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC Curve")
    plt.legend()

    plt.savefig("outputs/roc_curve.png")
    plt.close()


# =========================================================
# PRECISION-RECALL CURVE
# =========================================================
def plot_pr_curve(pdf):

    precision, recall, _ = precision_recall_curve(
        pdf["is_delayed"],
        pdf["delay_probability"]
    )

    plt.figure()
    plt.plot(recall, precision)

    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.title("Precision-Recall Curve")

    plt.savefig("outputs/pr_curve.png")
    plt.close()


# =========================================================
# CONFUSION MATRIX
# =========================================================
def plot_confusion_matrix(pdf):

    cm = confusion_matrix(
        pdf["is_delayed"],
        pdf["adjusted_prediction"]
    )

    plt.figure()
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")

    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title("Confusion Matrix")

    plt.savefig("outputs/confusion_matrix.png")
    plt.close()


# =========================================================
# THRESHOLD ANALYSIS (🔥 VERY IMPORTANT)
# =========================================================
def plot_threshold_analysis(pdf):

    thresholds = [i / 100 for i in range(5, 95, 5)]

    precision_list = []
    recall_list = []

    for t in thresholds:

        preds = (pdf["delay_probability"] > t).astype(int)

        cm = confusion_matrix(pdf["is_delayed"], preds)

        TP = cm[1][1]
        FP = cm[0][1]
        FN = cm[1][0]

        precision = TP / (TP + FP) if (TP + FP) else 0
        recall = TP / (TP + FN) if (TP + FN) else 0

        precision_list.append(precision)
        recall_list.append(recall)

    plt.figure()
    plt.plot(thresholds, precision_list, label="Precision")
    plt.plot(thresholds, recall_list, label="Recall")

    plt.xlabel("Threshold")
    plt.ylabel("Score")
    plt.title("Threshold vs Precision/Recall")
    plt.legend()

    plt.savefig("outputs/threshold_analysis.png")
    plt.close()


# =========================================================
# MAIN VISUALIZATION FUNCTION
# =========================================================
def generate_all_plots(predictions_df):

    print("\n===== GENERATING VISUALIZATIONS =====")

    pdf = prepare_data(predictions_df)

    plot_roc_curve(pdf)
    plot_pr_curve(pdf)
    plot_confusion_matrix(pdf)
    plot_threshold_analysis(pdf)

    print("Plots saved in /outputs/")