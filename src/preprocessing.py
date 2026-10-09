"""Stage 1 - ingestion, cleaning and feature engineering with PySpark.

Input : raw US Accidents CSV (data/raw/) or synthetic sample (data/sample/)
Output: cleaned Parquet dataset (data/processed/accidents_clean.parquet)
"""
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src import config
from src.utils import (DatasetNotFoundError, EmptyDatasetError, SchemaError,
                       get_logger, save_json)

log = get_logger("preprocessing")


# ------------------------------------------------------------------ loading
def find_dataset() -> Path:
    """Locate the input CSV, or raise an error that explains what to do."""
    if config.SAMPLE_MODE:
        if not config.SAMPLE_FILE.exists():
            raise DatasetNotFoundError(
                f"Sample file missing: {config.SAMPLE_FILE}\n"
                "Re-create it with:  python -m src.make_sample")
        return config.SAMPLE_FILE

    config.RAW_DIR.mkdir(parents=True, exist_ok=True)
    csvs = sorted(config.RAW_DIR.glob("*.csv"),
                  key=lambda p: (not p.name.startswith("US_Accidents"), -p.stat().st_size))
    if not csvs:
        raise DatasetNotFoundError(
            f"No CSV file found in: {config.RAW_DIR}\n"
            "Download the US Accidents dataset (see data/README.md), unzip it, and\n"
            "place the CSV (e.g. US_Accidents_March23.csv) inside data/raw/.\n"
            "To test the pipeline without it, run with:  --sample")
    return csvs[0]


def load_raw(spark: SparkSession, path: Path) -> DataFrame:
    """Read every column as string (cheap, no schema-inference pass) and rename."""
    log.info("Reading dataset: %s", path)
    df = spark.read.option("header", True).option("multiLine", False).csv(str(path))
    for old, new in config.RENAME_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def validate_schema(df: DataFrame) -> None:
    missing = [c for c in config.REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SchemaError(
            f"Required column(s) missing from dataset: {missing}\n"
            f"Columns found: {df.columns}\n"
            "Make sure you downloaded the 'US Accidents' dataset by Moosavi et al.")


# ------------------------------------------------------------------ cleaning
def _to_flag(col):
    return F.when(F.lower(F.trim(F.col(col))) == "true", 1).otherwise(0).cast("int")


def weather_group_expr():
    c = F.lower(F.coalesce(F.col("Weather_Condition"), F.lit("unknown")))
    return (F.when(c.rlike("thunder|t-storm|tstorm"), "Thunderstorm")
             .when(c.rlike("snow|sleet|ice|wintry|hail|freezing"), "Snow/Ice")
             .when(c.rlike("rain|drizzle|shower"), "Rain")
             .when(c.rlike("fog|mist|haze|smoke|dust|sand"), "Fog/Haze")
             .when(c.rlike("cloud|overcast"), "Cloudy")
             .when(c.rlike("clear|fair"), "Clear")
             .when(c.rlike("unknown"), "Unknown")
             .otherwise("Other"))


def _sample_target(df: DataFrame, stats: dict) -> DataFrame:
    """Keep a reproducible, proportional sample when the clean data is large."""
    eligible = df.count()
    stats["eligible_rows_before_sampling"] = eligible
    if config.SAMPLE_MODE or eligible <= config.TARGET_ROWS:
        stats["sampling_applied"] = False
        stats["sampling_method"] = "all valid, deduplicated rows"
        return df

    counts = {int(r["Severity"]): int(r["count"])
              for r in df.groupBy("Severity").count().collect()}
    total_target = min(config.TARGET_ROWS, eligible)
    raw_targets = {severity: total_target * count / eligible
                   for severity, count in counts.items()}
    targets = {severity: min(counts[severity], int(raw_targets[severity]))
               for severity in counts}
    remainder = total_target - sum(targets.values())
    for severity in sorted(counts, key=lambda s: raw_targets[s] - targets[s], reverse=True):
        if remainder == 0:
            break
        if targets[severity] < counts[severity]:
            targets[severity] += 1
            remainder -= 1

    ranked = df.withColumn("_sample_order", F.rand(config.RANDOM_SEED))
    window = Window.partitionBy("Severity").orderBy(F.col("_sample_order"))
    ranked = ranked.withColumn("_sample_rank", F.row_number().over(window))
    # Use a literal per-class limit because Spark cannot compare to a Python map
    # inside the window expression without introducing a join.
    sampled = None
    parts = []
    for severity, target in targets.items():
        parts.append(ranked.where(F.col("Severity") == severity)
                     .where(F.col("_sample_rank") <= target))
    sampled = parts[0]
    for part in parts[1:]:
        sampled = sampled.unionByName(part)
    sampled = sampled.drop("_sample_order", "_sample_rank")
    stats["sampling_applied"] = True
    stats["sampling_method"] = "proportional stratified sample by Severity"
    stats["sampling_seed"] = config.RANDOM_SEED
    stats["target_rows"] = total_target
    stats["sampled_rows"] = sampled.count()
    stats["sampling_class_targets"] = targets
    log.info("Using %d reproducible records from %d eligible rows: %s",
             stats["sampled_rows"], eligible, targets)
    return sampled


def clean_and_engineer(df: DataFrame):
    """Return (clean_df, stats_dict)."""
    stats = {}
    stats["raw_rows"] = df.count()
    if stats["raw_rows"] == 0:
        raise EmptyDatasetError("The input file contains no rows.")
    stats["raw_columns"] = len(df.columns)

    # 1. data-type conversion (strings -> proper types) --------------------
    present_flags = [c for c in config.INFRA_FLAGS if c in df.columns]
    absent_flags = [c for c in config.INFRA_FLAGS if c not in df.columns]
    if absent_flags:
        log.warning("Infrastructure flag columns not in file (set to 0): %s", absent_flags)
    for c in absent_flags:
        df = df.withColumn(c, F.lit("False"))
    for c in config.INFRA_FLAGS:
        df = df.withColumn(c, _to_flag(c))

    num_cols = [c for c in config.NUMERIC_WEATHER_COLS if c in df.columns]
    for c in config.NUMERIC_WEATHER_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
        else:
            df = df.withColumn(c, F.lit(None).cast("double"))
    df = (df.withColumn("Severity", F.col("Severity").cast("int"))
            .withColumn("Start_Lat", F.col("Start_Lat").cast("double"))
            .withColumn("Start_Lng", F.col("Start_Lng").cast("double"))
            # first 19 chars = 'yyyy-MM-dd HH:mm:ss' (some releases add fractions)
            .withColumn("Start_Time", F.to_timestamp(F.substring("Start_Time", 1, 19))))

    # 2. invalid / missing critical values --------------------------------
    df = df.filter(
        F.col("Severity").between(1, 4)
        & F.col("Start_Time").isNotNull()
        & F.col("Start_Lat").between(*config.LAT_RANGE)
        & F.col("Start_Lng").between(*config.LNG_RANGE)
        & F.col("ID").isNotNull())
    stats["rows_after_invalid_removal"] = df.count()
    if stats["rows_after_invalid_removal"] == 0:
        raise EmptyDatasetError(
            "No valid rows left after removing invalid Severity/time/coordinates. "
            "Check that the file is the US Accidents dataset.")

    # 3. duplicate removal --------------------------------------------------
    df = df.dropDuplicates(["ID"])
    stats["rows_after_dedup"] = df.count()
    stats["duplicates_removed"] = (stats["rows_after_invalid_removal"]
                                   - stats["rows_after_dedup"])
    df = _sample_target(df, stats)

    # 4. missing-value handling --------------------------------------------
    # numeric weather: fill with the median (robust to outliers)
    medians = {}
    q = df.approxQuantile(config.NUMERIC_WEATHER_COLS, [0.5], 0.01)
    for col, val in zip(config.NUMERIC_WEATHER_COLS, q):
        medians[col] = float(val[0]) if val else 0.0
    null_counts = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
                             for c in config.NUMERIC_WEATHER_COLS]).first().asDict()
    stats["missing_before_imputation"] = {k: int(v or 0) for k, v in null_counts.items()}
    stats["median_fill_values"] = medians
    df = df.fillna(medians)
    for c in ("City", "County", "State", "Weather_Condition", "Sunrise_Sunset"):
        if c not in df.columns:
            df = df.withColumn(c, F.lit("Unknown"))
    df = df.fillna("Unknown", subset=["City", "County", "State",
                                       "Weather_Condition", "Sunrise_Sunset"])

    # 5. feature engineering ------------------------------------------------
    ts = F.col("Start_Time")
    hour = F.hour(ts)
    dow = F.dayofweek(ts)  # 1 = Sunday ... 7 = Saturday
    df = (df
          .withColumn("Year", F.year(ts))
          .withColumn("Month", F.month(ts))
          .withColumn("Day_Of_Week", dow)
          .withColumn("Hour", hour)
          .withColumn("Is_Weekend", F.when(dow.isin(1, 7), 1).otherwise(0))
          .withColumn("Is_Rush_Hour",
                      F.when((~dow.isin(1, 7)) &
                             (hour.between(7, 9) | hour.between(16, 18)), 1).otherwise(0))
          .withColumn("Is_Night",
                      F.when(F.col("Sunrise_Sunset") == "Night", 1)
                       .when(F.col("Sunrise_Sunset") == "Day", 0)
                       .otherwise(F.when((hour < 6) | (hour >= 19), 1).otherwise(0)))
          .withColumn("Weather_Group", weather_group_expr())
          .withColumn("Is_Adverse_Weather",
                      F.when(F.col("Weather_Group").isin(
                          "Thunderstorm", "Snow/Ice", "Rain", "Fog/Haze"), 1).otherwise(0))
          .withColumn("Is_Severe", F.when(F.col("Severity") >= 3, 1).otherwise(0))
          # ~1 km grid cell (see hotspot_analysis.py)
          .withColumn("Grid_Lat", F.round("Start_Lat", config.GRID_DECIMALS))
          .withColumn("Grid_Lng", F.round("Start_Lng", config.GRID_DECIMALS))
          .withColumn("Grid_ID", F.concat_ws(
              "_", F.format_string(f"%.{config.GRID_DECIMALS}f", "Grid_Lat"),
              F.format_string(f"%.{config.GRID_DECIMALS}f", "Grid_Lng"))))

    # 6. keep only safe, useful columns (leakage-prone columns are dropped here)
    keep = (["ID", "Severity", "Is_Severe", "Start_Time", "Start_Lat", "Start_Lng",
             "City", "County", "State", "Weather_Condition", "Weather_Group",
             "Is_Adverse_Weather", "Sunrise_Sunset", "Is_Night",
             "Year", "Month", "Day_Of_Week", "Hour", "Is_Weekend", "Is_Rush_Hour",
             "Grid_Lat", "Grid_Lng", "Grid_ID"]
            + config.NUMERIC_WEATHER_COLS + config.INFRA_FLAGS)
    df = df.select(*keep)
    stats["clean_columns"] = len(keep)
    return df, stats


# ------------------------------------------------------------------ driver
def run_preprocessing(spark: SparkSession) -> DataFrame:
    config.ensure_dirs()
    path = find_dataset()
    raw = load_raw(spark, path)

    log.info("Raw schema:")
    raw.printSchema()
    validate_schema(raw)

    clean, stats = clean_and_engineer(raw)
    stats["source_file"] = path.name
    stats["sample_mode"] = config.SAMPLE_MODE

    log.info("Writing Parquet (partitioned by Year): %s", config.CLEAN_PARQUET)
    (clean.write.mode("overwrite").partitionBy("Year")
          .parquet(str(config.CLEAN_PARQUET)))

    out = spark.read.parquet(str(config.CLEAN_PARQUET))
    stats["final_rows"] = out.count()
    stats["final_partitions"] = out.rdd.getNumPartitions()
    save_json(stats, "cleaning_summary")
    (config.TABLES_DIR / "schema_clean.txt").write_text(
        out._jdf.schema().treeString(), encoding="utf-8")
    log.info("Cleaning summary: %s", stats)
    out.show(5, truncate=False)
    return out


def load_clean(spark: SparkSession) -> DataFrame:
    if not config.CLEAN_PARQUET.exists():
        raise DatasetNotFoundError(
            f"Cleaned data not found: {config.CLEAN_PARQUET}\n"
            "Run the preprocessing stage first:  python -m src.run_pipeline --stage preprocess")
    return spark.read.parquet(str(config.CLEAN_PARQUET))
