"""Stage 5 - comparable Spark MLlib severity classifiers.

The default binary target is Non-Severe (1-2) versus Severe (3-4). Every model
uses the same split, features, class weights and evaluation data.
"""
import pandas as pd
import plotly.express as px
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    DecisionTreeClassifier,
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.preprocessing import load_clean
from src.utils import EmptyDatasetError, get_logger, save_json, save_table

log = get_logger("model")


def _labels():
    if config.MODEL_MODE == "binary":
        return ["Non-Severe (1-2)", "Severe (3-4)"]
    return ["Severity 1", "Severity 2", "Severity 3", "Severity 4"]


def _classifiers():
    common = {
        "featuresCol": "features",
        "labelCol": "label",
        "weightCol": "weight",
        "maxBins": config.RF_MAX_BINS,
        "seed": config.RANDOM_SEED,
    }
    models = {
        "Logistic Regression": LogisticRegression(
            **{k: v for k, v in common.items() if k not in {"maxBins", "seed"}},
            maxIter=30,
        ),
        "Decision Tree": DecisionTreeClassifier(**common, maxDepth=config.RF_MAX_DEPTH),
        "Random Forest": RandomForestClassifier(
            **common, numTrees=config.RF_NUM_TREES, maxDepth=config.RF_MAX_DEPTH
        ),
    }
    if config.MODEL_MODE == "binary":
        models["Gradient-Boosted Tree"] = GBTClassifier(
            **common, maxIter=30, maxDepth=min(config.RF_MAX_DEPTH, 8)
        )
    return models


def _pipeline(classifier):
    stages = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in config.ML_CATEGORICAL_FEATURES
    ]
    input_cols = config.ML_NUMERIC_FEATURES + [
        f"{c}_idx" for c in config.ML_CATEGORICAL_FEATURES
    ]
    stages.append(
        VectorAssembler(inputCols=input_cols, outputCol="features", handleInvalid="keep")
    )
    stages.append(classifier)
    return Pipeline(stages=stages), input_cols


def _evaluate(preds, model_name):
    evaluator = lambda metric: MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName=metric
    ).evaluate(preds)
    metrics = {
        "model": model_name,
        "accuracy": evaluator("accuracy"),
        "weighted_f1": evaluator("f1"),
        "weighted_precision": evaluator("weightedPrecision"),
        "weighted_recall": evaluator("weightedRecall"),
        "macro_f1": evaluator("f1"),  # Spark's f1 is weighted; macro is computed below.
    }
    per_class = (
        preds.groupBy("label", "prediction")
        .count()
        .toPandas()
    )
    classes = sorted(set(per_class["label"]) | set(per_class["prediction"]))
    names = _labels()
    matrix = pd.DataFrame(0, index=classes, columns=classes, dtype=int)
    for _, row in per_class.iterrows():
        matrix.loc[row["label"], row["prediction"]] = int(row["count"])
    rows = []
    for cls in classes:
        tp = matrix.loc[cls, cls]
        fp = matrix[cls].sum() - tp
        fn = matrix.loc[cls].sum() - tp
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        rows.append(
            {
                "model": model_name,
                "class": names[int(cls)],
                "precision": round(precision, 4),
                "recall": round(recall, 4),
                "f1": round(f1, 4),
                "support": int(matrix.loc[cls].sum()),
            }
        )
    per_class_df = pd.DataFrame(rows)
    metrics["macro_f1"] = float(per_class_df["f1"].mean())
    if config.MODEL_MODE == "binary":
        binary = BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="rawPrediction"
        )
        metrics["auc_roc"] = binary.setMetricName("areaUnderROC").evaluate(preds)
        metrics["auc_pr"] = binary.setMetricName("areaUnderPR").evaluate(preds)
    matrix.index = [names[int(cls)] for cls in classes]
    matrix.columns = [names[int(cls)] for cls in classes]
    return metrics, matrix.reset_index().rename(columns={"index": "actual \\ predicted"}), per_class_df


def run_model(spark: SparkSession) -> dict:
    config.ensure_dirs()
    if config.MODEL_MODE not in ("binary", "multiclass"):
        raise ValueError("MODEL_MODE must be 'binary' or 'multiclass'")
    df = load_clean(spark)
    label = (
        F.col("Is_Severe").cast("double")
        if config.MODEL_MODE == "binary"
        else (F.col("Severity") - 1).cast("double")
    )
    features = config.ML_NUMERIC_FEATURES + config.ML_CATEGORICAL_FEATURES
    data = df.select(*features, label.alias("label"))
    if config.TRAIN_SAMPLE_FRACTION < 1.0:
        data = data.sample(False, config.TRAIN_SAMPLE_FRACTION, config.RANDOM_SEED)
    train, test = data.randomSplit(
        [config.TRAIN_FRACTION, 1 - config.TRAIN_FRACTION], seed=config.RANDOM_SEED
    )
    train = train.cache()
    n_train, n_test = train.count(), test.count()
    if n_train == 0 or n_test == 0:
        raise EmptyDatasetError("Train/test split is empty - dataset too small.")

    counts = {r["label"]: r["count"] for r in train.groupBy("label").count().collect()}
    if len(counts) < 2:
        raise EmptyDatasetError(f"Only one class present in training data: {counts}")
    weights = {cls: n_train / (len(counts) * count) for cls, count in counts.items()}
    mapping = F.create_map(
        *[item for cls, weight in weights.items() for item in (F.lit(float(cls)), F.lit(float(weight)))]
    )
    train = train.withColumn("weight", mapping[F.col("label")])

    comparison = []
    best = None
    best_f1 = -1.0
    for name, classifier in _classifiers().items():
        log.info("Training %s", name)
        pipeline, input_cols = _pipeline(classifier)
        fitted = pipeline.fit(train)
        preds = fitted.transform(test).cache()
        metrics, matrix, per_class = _evaluate(preds, name)
        metrics.update(
            {
                "mode": config.MODEL_MODE,
                "train_rows": n_train,
                "test_rows": n_test,
                "seed": config.RANDOM_SEED,
                "sample_mode": config.SAMPLE_MODE,
            }
        )
        comparison.append(metrics)
        save_table(matrix, f"confusion_matrix_{name.lower().replace(' ', '_').replace('-', '_')}")
        save_table(per_class, f"per_class_metrics_{name.lower().replace(' ', '_').replace('-', '_')}")
        if metrics["macro_f1"] > best_f1:
            best, best_f1 = (name, metrics), metrics["macro_f1"]
        if name in {"Decision Tree", "Random Forest", "Gradient-Boosted Tree"}:
            tree = fitted.stages[-1]
            importance = pd.DataFrame(
                {
                    "feature": input_cols,
                    "importance": [float(x) for x in tree.featureImportances.toArray()],
                    "model": name,
                }
            )
            importance["feature"] = importance["feature"].str.replace("_idx", "", regex=False)
            save_table(
                importance.sort_values("importance", ascending=False),
                f"feature_importance_{name.lower().replace(' ', '_').replace('-', '_')}",
            )
        preds.unpersist()

    comparison_df = pd.DataFrame(comparison).sort_values("macro_f1", ascending=False)
    save_table(comparison_df, "model_comparison")
    chart_metrics = ["accuracy", "weighted_precision", "weighted_recall", "weighted_f1", "macro_f1"]
    chart = px.bar(
        comparison_df.melt(
            id_vars=["model"], value_vars=chart_metrics,
            var_name="metric", value_name="score",
        ),
        x="model", y="score", color="metric", barmode="group",
        range_y=[0, 1], title="Classification model comparison",
    )
    chart.write_html(str(config.FIGURES_DIR / "model_comparison.html"), include_plotlyjs="cdn")
    for metric in chart_metrics:
        px.bar(comparison_df, x="model", y=metric, range_y=[0, 1],
               title=f"{metric.replace('_', ' ').title()} comparison").write_html(
                   str(config.FIGURES_DIR / f"{metric}_comparison.html"), include_plotlyjs="cdn"
               )
    save_json(
        {
            "best_model": best[0],
            "selection_metric": "macro_f1",
            "best_metrics": best[1],
            "models": [m["model"] for m in comparison],
            "class_counts_train": counts,
        },
        "model_metrics",
    )
    save_json(
        {"used_features": features, "excluded_leakage_prone": config.LEAKAGE_EXCLUDED},
        "model_features",
    )
    # Keep backwards-compatible dashboard files for the selected model.
    best_name = best[0].lower().replace(" ", "_").replace("-", "_")
    best_cm = pd.read_csv(config.TABLES_DIR / f"confusion_matrix_{best_name}.csv")
    best_pc = pd.read_csv(config.TABLES_DIR / f"per_class_metrics_{best_name}.csv")
    save_table(best_cm, "confusion_matrix")
    save_table(best_pc, "per_class_metrics")
    best_importance = config.TABLES_DIR / f"feature_importance_{best_name}.csv"
    if best_importance.exists():
        save_table(pd.read_csv(best_importance), "feature_importance")
    save_json({"comparison": comparison}, "model_comparison_metrics")
    return best[1]
