# Methodology

## 1. Cleaning (preprocessing.py)
1. Read CSV with Spark (all columns as strings, then explicit casts - avoids a slow schema-inference pass).
2. Rename columns such as `Temperature(F)` → `Temperature_F`.
3. Cast types: Severity → int, coordinates → double, `Start_Time` → timestamp, True/False flags → 1/0.
4. Remove invalid rows: Severity outside 1-4, unparseable time, coordinates outside the US window, missing ID.
5. Remove duplicates by `ID`.
6. Missing values: numeric weather columns → **median** (robust to outliers); text columns → `"Unknown"`.
   Counts of missing values before imputation are saved in `cleaning_summary.json`.
7. Drop leakage-prone / identifier columns (see below) and write Parquet partitioned by `Year`.

## 2. Feature engineering
`Hour, Day_Of_Week (1=Sun..7=Sat), Month, Year, Is_Weekend, Is_Rush_Hour (weekday 7-9h, 16-18h),
Is_Night (from Sunrise_Sunset), Weather_Group, Is_Adverse_Weather, Is_Severe (Severity ≥ 3),
Grid_Lat, Grid_Lng, Grid_ID`.
Weather groups (keyword matching on `Weather_Condition`): Thunderstorm, Snow/Ice, Rain, Fog/Haze,
Cloudy, Clear, Unknown, Other. *Adverse* = Thunderstorm, Snow/Ice, Rain, Fog/Haze.

## 3. Spark SQL analytics
The clean Parquet is registered as the view `accidents`; queries in `analysis.py` count accidents by
severity, hour, weekday, month, year, weather, state, city, day/night, rush-hour, infrastructure feature,
and average environment values per severity.

## 4. Hotspot analysis (grid binning)
`Grid_ID = round(lat,2)_round(lng,2)`. 0.01° latitude ≈ 1.1 km; 0.01° longitude ≈ 0.8-1.0 km over most of
the US, so cells are *approximately* 1 km. Count accidents per cell; a **hotspot** is a cell with
≥ `MIN_HOTSPOT_ACCIDENTS` and in the top 1 % by count (99th percentile, `approxQuantile`).
Why not DBSCAN? It is harder to scale and explain; grid counting is a single Spark `groupBy`.

## 5. Risk score (project-defined, not official)
For every cell with ≥ `MIN_CELL_ACCIDENTS` accidents:

```
freq_norm = (log(1+n) - min) / (max - min)
sev_norm  = (avg_severity - 1) / 3
risk      = 100 × (0.40·freq_norm + 0.30·sev_norm + 0.15·adverse_weather_share + 0.15·night_share)
```
Weights are in `config.py` and are judgement-based (frequency matters most, then severity). Categories use
score percentiles: below P50 Low, P50-P85 Moderate, P85-P97 High, ≥ P97 Critical. Because they are
percentile-based the categories are *relative* (a ranking), not an absolute safety measure.
Limitation: counts are not divided by traffic volume (not in the dataset).

## 6. Machine learning
* **Sampling**: after validation and ID deduplication, inputs above `TARGET_ROWS` (default 500,000)
  receive a fixed-seed proportional stratified sample by Severity. Exact class targets are saved
  in `cleaning_summary.json`; no synthetic records are used.
* **Target**: default **binary** - Severe (3-4) vs Non-severe (1-2). Rationale: the four classes are heavily
  imbalanced and severities 1/4 are rare, so multiclass results are unstable and harder to explain.
  `MODEL_MODE=multiclass` is also supported. State your own class counts from the pipeline output in the report.
* **Features** (known when an accident is reported): location, hour/day/month, night/weekend/rush-hour flags,
  weather measurements, weather group, adverse-weather flag, state, 12 road-infrastructure flags.
* **Excluded to prevent leakage**: `End_Time`, `End_Lat/Lng`, `Distance_mi`, `Description`,
  `Weather_Timestamp`, `Severity`/`Is_Severe` (target), identifiers/free text (`ID, Source, Street, Zipcode, Airport_Code`).
  Listed in `results/tables/model_features.json` after running.
* **Imbalance**: class weights `w = N / (K · n_class)` computed on the training set only and passed to
  `RandomForestClassifier(weightCol=...)`. Test data keep the true distribution.
* **Split**: random 80/20 with fixed seed 42. (A time-based split would be stricter - see Future Scope.)
* **Models**: Logistic Regression, Decision Tree, Random Forest and (in binary mode) Gradient-Boosted Tree.
  All models use the same split and feature pipeline. The best model is selected by macro F1.
* **Metrics**: accuracy, macro/weighted F1/precision/recall, per-class precision/recall/F1/support, confusion matrix,
  majority-class baseline accuracy, AUC-ROC and AUC-PR (binary). Accuracy is never reported alone.
* `TRAIN_SAMPLE_FRACTION` env var lets you train on a fraction of rows if your laptop is short of memory.
