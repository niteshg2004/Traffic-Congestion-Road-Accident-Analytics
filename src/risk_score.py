"""Stage 4 - project-defined, explainable accident RISK SCORE per grid cell.
(This is an analytical score for this project - NOT an official risk model.)

  risk_score = 100 * ( w_f * freq_norm + w_s * sev_norm
                     + w_a * adverse_weather_share + w_n * night_share )

  freq_norm = min-max scaled log(1 + accidents)   (log stops mega-cells dominating)
  sev_norm  = (avg_severity - 1) / 3               (severity runs 1..4)
Only cells with >= MIN_CELL_ACCIDENTS accidents are scored (small counts are noise).
Categories are relative: score percentiles P50 / P85 / P97 -> Low/Moderate/High/Critical.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.hotspot_analysis import run_hotspot_analysis
from src.utils import EmptyDatasetError, get_logger, save_json, save_table, update_json

log = get_logger("risk")


def compute_risk(grid: DataFrame) -> DataFrame:
    w = config.RISK_WEIGHTS
    assert abs(sum(w.values()) - 1.0) < 1e-9, "RISK_WEIGHTS must sum to 1"
    cells = grid.filter(F.col("accidents") >= config.MIN_CELL_ACCIDENTS)
    cells = cells.withColumn("log_acc", F.log1p("accidents"))
    mm = cells.agg(F.min("log_acc").alias("lo"), F.max("log_acc").alias("hi")).first()
    if mm["lo"] is None:
        raise EmptyDatasetError(
            f"No grid cell has >= {config.MIN_CELL_ACCIDENTS} accidents; "
            "lower MIN_CELL_ACCIDENTS in src/config.py")
    lo, hi = float(mm["lo"]), float(mm["hi"])
    span = (hi - lo) or 1.0
    cells = (cells
             .withColumn("freq_norm", (F.col("log_acc") - lo) / span)
             .withColumn("sev_norm", (F.col("avg_severity") - 1) / 3.0)
             .withColumn("risk_score", 100.0 * (
                 w["frequency"] * F.col("freq_norm")
                 + w["severity"] * F.col("sev_norm")
                 + w["adverse_weather"] * F.col("adverse_weather_pct")
                 + w["night"] * F.col("night_pct"))))
    return cells


def run_risk_score(spark: SparkSession, grid: DataFrame = None) -> None:
    config.ensure_dirs()
    if grid is None:
        grid = run_hotspot_analysis(spark)
    cells = compute_risk(grid).cache()

    cuts = cells.approxQuantile("risk_score", config.RISK_PERCENTILES, 0.001)
    c1, c2, c3 = cuts
    cells = cells.withColumn(
        "risk_category",
        F.when(F.col("risk_score") >= c3, "Critical")
         .when(F.col("risk_score") >= c2, "High")
         .when(F.col("risk_score") >= c1, "Moderate")
         .otherwise("Low"))

    counts = (cells.groupBy("risk_category").agg(
        F.count("*").alias("grid_cells"), F.sum("accidents").alias("accidents"))
        .toPandas())
    order = {k: i for i, k in enumerate(config.RISK_CATEGORIES)}
    counts = counts.sort_values("risk_category", key=lambda s: s.map(order))
    save_table(counts, "risk_category_counts")

    top = (cells.orderBy(F.desc("risk_score")).limit(config.TOP_RISK_SAVED).toPandas())
    top.insert(0, "rank", range(1, len(top) + 1))
    for c in ("center_lat", "center_lng", "avg_severity", "adverse_weather_pct",
              "night_pct", "freq_norm", "sev_norm", "risk_score"):
        top[c] = top[c].round(4)
    top = top.drop(columns=["log_acc"])
    save_table(top, "risk_cells_top")

    best = top.iloc[0]
    update_json({"scored_cells": int(counts["grid_cells"].sum()),
                 "highest_risk_location": f"{best['city']}, {best['state']}",
                 "highest_risk_cell": best["Grid_ID"],
                 "highest_risk_score": float(best["risk_score"])}, "kpis")
    save_json({"formula": "100*(wf*freq_norm + ws*sev_norm + wa*adverse_pct + wn*night_pct)",
               "weights": config.RISK_WEIGHTS,
               "min_cell_accidents": config.MIN_CELL_ACCIDENTS,
               "category_percentiles": config.RISK_PERCENTILES,
               "category_cutoffs_score": {"Moderate>=": c1, "High>=": c2, "Critical>=": c3},
               "note": "Project-defined analytical score, not an official risk model."},
              "risk_method")
    log.info("Risk score done. Cut-offs: %s", cuts)
