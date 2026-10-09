"""Stage 3 - grid-based accident hotspot detection (simple & explainable).

Method: latitude/longitude are rounded to 2 decimals (0.01 deg ~ 1.1 km), so each
(lat, lng) pair identifies one ~1 km x ~1 km grid cell. Accidents are counted
per cell; cells in the top 1 % by count (and >= MIN_HOTSPOT_ACCIDENTS) are hotspots.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.preprocessing import load_clean
from src.utils import get_logger, save_json, save_table, update_json

log = get_logger("hotspot")


def build_grid(df: DataFrame) -> DataFrame:
    """One row per grid cell with accident statistics."""
    return (df.groupBy("Grid_ID")
              .agg(F.count("*").alias("accidents"),
                   F.avg("Start_Lat").alias("center_lat"),
                   F.avg("Start_Lng").alias("center_lng"),
                   F.avg("Severity").alias("avg_severity"),
                   F.sum("Is_Severe").alias("severe_accidents"),
                   F.avg("Is_Adverse_Weather").alias("adverse_weather_pct"),
                   F.avg("Is_Night").alias("night_pct"),
                   F.first("City").alias("city"),
                   F.first("State").alias("state")))


def run_hotspot_analysis(spark: SparkSession) -> DataFrame:
    config.ensure_dirs()
    df = load_clean(spark)
    grid = build_grid(df).cache()
    n_cells = grid.count()

    candidates = grid.filter(F.col("accidents") >= config.MIN_HOTSPOT_ACCIDENTS)
    if candidates.count() == 0:
        log.warning("No grid cell reaches MIN_HOTSPOT_ACCIDENTS=%d; lower it in config.py",
                    config.MIN_HOTSPOT_ACCIDENTS)
        threshold = float("inf")
    else:
        q = grid.approxQuantile("accidents", [config.HOTSPOT_PERCENTILE], 0.001)
        threshold = max(float(q[0]), float(config.MIN_HOTSPOT_ACCIDENTS))

    grid = grid.withColumn("is_hotspot", (F.col("accidents") >= threshold).cast("int"))
    n_hot = grid.filter("is_hotspot = 1").count()

    top = (grid.filter("is_hotspot = 1").orderBy(F.desc("accidents"))
               .limit(config.TOP_HOTSPOTS_SAVED).toPandas())
    top.insert(0, "rank", range(1, len(top) + 1))
    for c in ("center_lat", "center_lng", "avg_severity", "adverse_weather_pct", "night_pct"):
        top[c] = top[c].round(4)
    save_table(top, "hotspots_top")

    update_json({"grid_cells_total": n_cells, "hotspot_count": n_hot,
                 "hotspot_threshold_accidents": threshold}, "kpis")
    save_json({"grid_decimals": config.GRID_DECIMALS,
               "hotspot_percentile": config.HOTSPOT_PERCENTILE,
               "min_hotspot_accidents": config.MIN_HOTSPOT_ACCIDENTS,
               "grid_cells_total": n_cells, "hotspot_count": n_hot,
               "threshold_accidents": threshold}, "hotspot_method")
    log.info("Grid cells: %d | hotspot threshold: %s | hotspots: %d", n_cells, threshold, n_hot)
    return grid
