"""Stage 2 - Spark SQL analytics. Heavy work is done by Spark; only small
aggregated tables are converted to Pandas and saved for the dashboard."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.preprocessing import load_clean
from src.utils import (get_logger, save_json, save_parquet_table, save_table)

log = get_logger("analysis")

# name -> Spark SQL query (run against the temp view "accidents")
QUERIES = {
    "by_severity": """
        SELECT Severity, COUNT(*) AS accidents
        FROM accidents GROUP BY Severity ORDER BY Severity""",
    "by_hour": """
        SELECT Hour, COUNT(*) AS accidents, SUM(Is_Severe) AS severe
        FROM accidents GROUP BY Hour ORDER BY Hour""",
    "by_day_of_week": """
        SELECT Day_Of_Week, COUNT(*) AS accidents, SUM(Is_Severe) AS severe
        FROM accidents GROUP BY Day_Of_Week ORDER BY Day_Of_Week""",
    "by_month": """
        SELECT Month, COUNT(*) AS accidents, SUM(Is_Severe) AS severe
        FROM accidents GROUP BY Month ORDER BY Month""",
    "by_year": """
        SELECT Year, COUNT(*) AS accidents, SUM(Is_Severe) AS severe
        FROM accidents GROUP BY Year ORDER BY Year""",
    "by_weather_group": """
        SELECT Weather_Group, COUNT(*) AS accidents, SUM(Is_Severe) AS severe,
               ROUND(AVG(Severity), 3) AS avg_severity
        FROM accidents GROUP BY Weather_Group ORDER BY accidents DESC""",
    "by_weather_condition_top20": """
        SELECT Weather_Condition, COUNT(*) AS accidents,
               ROUND(AVG(Severity), 3) AS avg_severity
        FROM accidents GROUP BY Weather_Condition ORDER BY accidents DESC LIMIT 20""",
    "by_state": """
        SELECT State, COUNT(*) AS accidents, SUM(Is_Severe) AS severe,
               ROUND(AVG(Severity), 3) AS avg_severity
        FROM accidents GROUP BY State ORDER BY accidents DESC""",
    "top_cities": """
        SELECT City, State, COUNT(*) AS accidents, SUM(Is_Severe) AS severe
        FROM accidents GROUP BY City, State ORDER BY accidents DESC LIMIT 20""",
    "hour_by_day_heatmap": """
        SELECT Day_Of_Week, Hour, COUNT(*) AS accidents
        FROM accidents GROUP BY Day_Of_Week, Hour ORDER BY Day_Of_Week, Hour""",
    "day_vs_night": """
        SELECT CASE WHEN Is_Night = 1 THEN 'Night' ELSE 'Day' END AS period,
               COUNT(*) AS accidents, SUM(Is_Severe) AS severe,
               ROUND(100.0 * SUM(Is_Severe) / COUNT(*), 2) AS severe_pct
        FROM accidents GROUP BY Is_Night""",
    "rush_hour_comparison": """
        SELECT CASE WHEN Is_Rush_Hour = 1 THEN 'Rush hour' ELSE 'Other time' END AS period,
               COUNT(*) AS accidents,
               ROUND(100.0 * SUM(Is_Severe) / COUNT(*), 2) AS severe_pct
        FROM accidents GROUP BY Is_Rush_Hour""",
    "environment_by_severity": """
        SELECT Severity, ROUND(AVG(Temperature_F), 2) AS avg_temp_f,
               ROUND(AVG(Visibility_mi), 2) AS avg_visibility_mi,
               ROUND(AVG(Wind_Speed_mph), 2) AS avg_wind_mph,
               ROUND(AVG(Humidity_pct), 2) AS avg_humidity_pct
        FROM accidents GROUP BY Severity ORDER BY Severity""",
}


def _infrastructure_table(spark: SparkSession, df):
    """For each road feature: accidents / severe share when present vs absent."""
    rows = []
    for flag in config.INFRA_FLAGS:
        agg = (df.groupBy(flag)
                 .agg(F.count("*").alias("accidents"), F.sum("Is_Severe").alias("severe"))
                 .collect())
        for r in agg:
            n, s = int(r["accidents"]), int(r["severe"] or 0)
            rows.append({"feature": flag, "present": int(r[flag]), "accidents": n,
                         "severe": s, "severe_pct": round(100.0 * s / n, 2) if n else 0.0})
    import pandas as pd
    return pd.DataFrame(rows)


def run_analysis(spark: SparkSession) -> None:
    config.ensure_dirs()
    df = load_clean(spark)
    df.createOrReplaceTempView("accidents")

    total = df.count()
    severe = df.filter(F.col("Is_Severe") == 1).count()
    ext = df.agg(F.min("Start_Time").alias("t0"), F.max("Start_Time").alias("t1"),
                 F.avg("Severity").alias("avg_sev"),
                 F.countDistinct("State").alias("states")).first()
    save_json({
        "total_accidents": total, "severe_accidents": severe,
        "severe_pct": round(100.0 * severe / total, 2),
        "avg_severity": round(float(ext["avg_sev"]), 3),
        "states_covered": int(ext["states"]),
        "first_accident": str(ext["t0"]), "last_accident": str(ext["t1"]),
        "sample_mode": config.SAMPLE_MODE}, "kpis")

    for name, sql in QUERIES.items():
        log.info("Spark SQL -> %s", name)
        save_table(spark.sql(sql).toPandas(), name)

    log.info("Infrastructure analysis")
    save_table(_infrastructure_table(spark, df), "infrastructure_analysis")

    # Pre-aggregated "cubes" so the dashboard filters (state / severity /
    # weather / year) work WITHOUT Spark and without raw data.
    log.info("Dashboard cubes")
    cube_time = spark.sql("""
        SELECT State, City, Severity, Weather_Group, Year, Month, Day_Of_Week,
               Hour, Is_Night, COUNT(*) AS accidents
        FROM accidents
        GROUP BY State, City, Severity, Weather_Group, Year, Month,
                 Day_Of_Week, Hour, Is_Night""").toPandas()
    cube_hour = spark.sql("""
        SELECT State, Severity, Weather_Group, Year, Day_Of_Week, Hour, COUNT(*) AS accidents
        FROM accidents GROUP BY State, Severity, Weather_Group, Year, Day_Of_Week, Hour""").toPandas()
    save_parquet_table(cube_time, "cube_time")
    save_parquet_table(cube_hour, "cube_hour")
    log.info("Analysis finished: %d accidents analysed", total)
