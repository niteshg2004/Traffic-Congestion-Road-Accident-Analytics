"""Central configuration for the whole project.

Every path is relative to the project root (no personal paths).
Set environment variable CA3_SAMPLE=1 (or run the pipeline with --sample)
to use the tiny SYNTHETIC smoke-test dataset; outputs then go to separate
folders (data/processed_sample, results_sample) so real results are never mixed.
"""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_MODE = os.environ.get("CA3_SAMPLE", "0") == "1"

# ---------------------------------------------------------------- paths
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "sample"
SAMPLE_FILE = SAMPLE_DIR / "synthetic_accidents_sample.csv"
PROCESSED_DIR = DATA_DIR / ("processed_sample" if SAMPLE_MODE else "processed")
CLEAN_PARQUET = PROCESSED_DIR / "accidents_clean.parquet"

RESULTS_DIR = PROJECT_ROOT / ("results_sample" if SAMPLE_MODE else "results")
TABLES_DIR = RESULTS_DIR / "tables"
FIGURES_DIR = RESULTS_DIR / "figures"
MODELS_DIR = RESULTS_DIR / "models"

# ---------------------------------------------------------------- spark
APP_NAME = "TrafficAccidentAnalytics"
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
SHUFFLE_PARTITIONS = int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "64"))
RANDOM_SEED = 42

# ---------------------------------------------------------------- dataset
# Columns the pipeline cannot work without (original Kaggle names).
REQUIRED_COLUMNS = [
    "ID", "Severity", "Start_Time", "Start_Lat", "Start_Lng", "City", "State",
    "Weather_Condition", "Sunrise_Sunset",
    "Crossing", "Junction", "Railway", "Traffic_Signal",
]
# Original header -> clean name (only applied if the column exists)
RENAME_MAP = {
    "Distance(mi)": "Distance_mi",
    "Temperature(F)": "Temperature_F",
    "Wind_Chill(F)": "Wind_Chill_F",
    "Humidity(%)": "Humidity_pct",
    "Pressure(in)": "Pressure_in",
    "Visibility(mi)": "Visibility_mi",
    "Wind_Speed(mph)": "Wind_Speed_mph",
    "Precipitation(in)": "Precipitation_in",
}
NUMERIC_WEATHER_COLS = ["Temperature_F", "Humidity_pct", "Pressure_in",
                        "Visibility_mi", "Wind_Speed_mph", "Precipitation_in"]
# Road-infrastructure flags (True/False in the raw file -> 1/0)
INFRA_FLAGS = ["Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
               "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
               "Traffic_Signal"]
# Valid US latitude/longitude window (includes Alaska & Hawaii)
LAT_RANGE = (17.0, 72.0)
LNG_RANGE = (-180.0, -60.0)

# ---------------------------------------------------------------- hotspot / grid
GRID_DECIMALS = 2            # 0.01 deg latitude ~ 1.1 km  -> "~1 km cells"
HOTSPOT_PERCENTILE = 0.99    # hotspot = top 1% of cells by accident count ...
MIN_HOTSPOT_ACCIDENTS = 20   # ... and at least this many accidents
TOP_HOTSPOTS_SAVED = 1000

# ---------------------------------------------------------------- risk score
MIN_CELL_ACCIDENTS = 20      # cells with fewer accidents are too noisy to score
RISK_WEIGHTS = {             # must sum to 1.0
    "frequency": 0.40,
    "severity": 0.30,
    "adverse_weather": 0.15,
    "night": 0.15,
}
# Category cut-offs are percentiles of the score among scored cells:
#   < P50 Low | P50-P85 Moderate | P85-P97 High | >= P97 Critical
RISK_PERCENTILES = [0.50, 0.85, 0.97]
RISK_CATEGORIES = ["Low", "Moderate", "High", "Critical"]
TOP_RISK_SAVED = 2000

if SAMPLE_MODE:  # tiny synthetic file: use small thresholds so the smoke test runs
    MIN_HOTSPOT_ACCIDENTS = 3
    MIN_CELL_ACCIDENTS = 3

# ---------------------------------------------------------------- ML
MODEL_MODE = os.environ.get("MODEL_MODE", "binary")  # "binary" | "multiclass"
TARGET_ROWS = int(os.environ.get("TARGET_ROWS", "500000"))
TRAIN_FRACTION = 0.8
TRAIN_SAMPLE_FRACTION = float(os.environ.get("TRAIN_SAMPLE_FRACTION", "1.0"))
RF_NUM_TREES = int(os.environ.get("RF_NUM_TREES", "50"))
RF_MAX_DEPTH = int(os.environ.get("RF_MAX_DEPTH", "10"))
RF_MAX_BINS = 64             # must be >= number of categories (50 states + 'keep')
# Features known at the time the accident is reported (no leakage)
ML_NUMERIC_FEATURES = ["Start_Lat", "Start_Lng", "Hour", "Day_Of_Week", "Month",
                       "Is_Night", "Is_Weekend", "Is_Rush_Hour",
                       "Temperature_F", "Humidity_pct", "Pressure_in",
                       "Visibility_mi", "Wind_Speed_mph", "Precipitation_in",
                       "Is_Adverse_Weather"] + INFRA_FLAGS
ML_CATEGORICAL_FEATURES = ["State", "Weather_Group"]
# Columns deliberately EXCLUDED (documented in the report)
LEAKAGE_EXCLUDED = {
    "End_Time": "known only after the accident is cleared",
    "End_Lat / End_Lng": "describe the end of the accident's impact",
    "Distance_mi": "length of road affected - measured after the event",
    "Description": "free text written after the event, often mentions delays",
    "Weather_Timestamp": "observation time, not a causal feature",
    "Severity / Is_Severe": "the target itself",
    "ID, Source, Street, Zipcode, Airport_Code": "identifiers / high-cardinality text",
}

# ---------------------------------------------------------------- helpers
def ensure_dirs():
    for p in (RAW_DIR, SAMPLE_DIR, PROCESSED_DIR, TABLES_DIR, FIGURES_DIR, MODELS_DIR):
        p.mkdir(parents=True, exist_ok=True)
