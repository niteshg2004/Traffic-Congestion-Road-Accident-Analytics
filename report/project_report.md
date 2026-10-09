# Traffic Congestion and Road Accident Analytics Using Apache Spark

**Course:** Big Data Analytics (CA3) | **Branch:** CSE | **Batch:** 2023–27 | **Semester:** VII
**Student:** [NAME] | **Roll No.:** [ROLL NO.] | **Submission:** 11 October 2026

> This report reflects the real Kaggle run in `results/tables/`. The production experiment uses a
> reproducible 500,000-row sample from the source CSV, not the entire source file.

---

## 1. Title
Traffic Congestion and Road Accident Analytics Using Apache Spark

## 2. Abstract
Road accidents cause loss of life, injuries and heavy congestion. This project builds a Big Data analytics pipeline on the
US Accidents dataset (Moosavi et al.) using Apache Spark. PySpark performs ingestion, cleaning and feature engineering and stores
the data as Parquet; Spark SQL answers questions about when, where and under which weather conditions accidents occur; a ~1 km
grid method identifies accident hotspots; a project-defined weighted risk score ranks locations into Low, Moderate, High and Critical
categories; and Spark MLlib classifiers compare severity prediction using only information available when the
accident is reported. Results are presented in a Streamlit/Plotly dashboard. The dataset contains
**500,000** sampled records; the best model (Gradient-Boosted Tree) achieved
**0.729 weighted F1, 0.722 severe-class recall and 0.636 macro F1**.

## 3. Introduction
Traffic accidents are a major public-safety and economic problem. Modern sensors, traffic APIs and weather feeds generate large volumes of
accident data, but its size and variety make it hard to process with single-machine tools. Distributed engines such as Apache Spark
enable scalable analysis, helping authorities understand patterns and plan interventions.

## 4. Problem Statement
Given a large, multi-attribute dataset of reported road accidents, (a) identify temporal, spatial, weather and infrastructure patterns,
(b) locate accident hotspots, (c) compute an explainable risk score for locations, and (d) predict accident severity — all with a
scalable Big Data pipeline. Questions addressed:
1. Where do accidents occur most frequently? 2. Which locations are hotspots? 3. At what hours do accidents peak?
4. Which days/months have most accidents? 5. Which weather conditions are associated with accidents?
6. How do road-infrastructure features relate to accidents? 7. Which accidents are severe?
8. Can severity be predicted with machine learning? 9. Can a meaningful risk score be calculated?

## 5. Motivation
* Identify high-risk locations and times so that enforcement, signage and road engineering can be prioritised.
* The data volume (millions of rows) justifies a Big Data approach and demonstrates practical Spark skills.
* Severity prediction and risk scoring show how analytics can support decision-making.

## 6. Objectives
1. Build a reproducible PySpark ETL pipeline. 2. Perform Spark SQL analytics. 3. Detect hotspots with grid binning.
4. Define an explainable risk score. 5. Train and evaluate a leakage-free MLlib classifier on imbalanced data.
6. Deliver an interactive dashboard that does not require Spark at view time.

## 7. Existing System
Traditional accident analysis relies on manual police reports, spreadsheets and desktop GIS tools, usually on aggregated yearly statistics.
These approaches (i) do not scale to millions of records, (ii) are slow to update, (iii) rarely combine weather, time and road features,
and (iv) provide little predictive capability. Many student analyses use Pandas on a sample, losing most of the data.

## 8. Proposed System
A Spark-based pipeline processes the full dataset in parallel, saves compact analytical results, and serves them through an interactive
dashboard (see Section 12). The system adds hotspot detection, a risk score and severity prediction on top of descriptive analytics.

## 9. Dataset Description
* **Source:** US Accidents dataset — Moosavi, Samavatian, Parthasarathy, Ramnath (2019), distributed via Kaggle / the authors' website.
* **Records:** 7,728,394 source rows; 500,000 records selected for the experiment.
* **Columns:** 46 raw columns; time span in the selected sample: 2016-02-08 11:16:00 to 2023-04-01 04:55:30.
* **Key fields:** Severity (1–4), Start_Time, Start_Lat/Lng, City, State, weather measurements (temperature, humidity, pressure,
  visibility, wind speed, precipitation), Weather_Condition, 13 road-feature flags (Junction, Crossing, Railway, Traffic_Signal, …), Sunrise_Sunset.
* **Note:** Severity reflects the impact on traffic (delay), not medical injury severity.
* **Data quality:** 0 duplicates and 0 invalid rows were removed. Missing weather values before
  median imputation were Temperature 10,446; Humidity 11,117; Pressure 8,967; Visibility 11,313;
  Wind Speed 36,908; Precipitation 142,179.

## 10. Big Data Characteristics (5 V's)
| V | In this project |
|---|---|
| Volume | 7,728,394 rows; source CSV is approximately 2.8 GB |
| Velocity | Accident records are collected continuously from traffic APIs; this project processes them in batch (streaming = future scope) |
| Variety | Timestamps, geo-coordinates, categorical text, numeric weather readings, boolean road features |
| Veracity | Missing weather values, duplicates, source differences → cleaning stage |
| Value | Hotspots, risk ranking and severity prediction support safety decisions |

## 11. Technology Stack
| Tool | Purpose |
|---|---|
| Python 3.10/3.11 | Language |
| Apache Spark 3.5 / PySpark | Distributed ETL and analytics |
| Spark SQL | Aggregations and group-by queries |
| Spark MLlib | Shared feature pipeline, four classifiers, evaluators |
| Parquet | Columnar storage between stages |
| Pandas / NumPy | Small aggregated outputs only |
| Plotly + Streamlit | Interactive dashboard |
Kafka was intentionally not used because the data is historical (batch) and streaming would add complexity without benefit.

## 12. System Architecture
Raw CSV → PySpark ingestion → cleaning → feature engineering → Parquet → Spark SQL analytics → {trends, hotspots, risk} → MLlib model →
aggregated results (`results/tables`) → Streamlit + Plotly dashboard. *(Insert the diagram from `docs/architecture.md` as a figure.)*
Key principle: **Spark computes once; the dashboard only reads small outputs**.

## 13. Methodology
Six stages — ingestion, cleaning, feature engineering, analytics, hotspot/risk modelling, ML — each implemented as a module in `src/` and run by
`run_pipeline.py` with fixed random seed 42 and central configuration (`config.py`). Details: `docs/methodology.md`.

## 14. Data Preprocessing
1. Read CSV as strings; rename columns with units (e.g. `Temperature(F)` → `Temperature_F`).
2. Explicit type casting (int, double, timestamp, boolean → 0/1).
3. Remove rows with invalid Severity (not 1–4), unparseable Start_Time, coordinates outside the US window, or missing ID.
4. Remove duplicate records by ID.
5. Impute numeric weather columns with the **median**; set missing City/County/State/Weather text to "Unknown".
6. Drop leakage-prone and identifier columns; write Parquet partitioned by Year.
Results: 7,728,394 raw → 7,728,394 valid/deduplicated → 500,000 sampled rows.

## 15. Spark Processing
Spark local mode (`local[*]`) with 4g driver memory and 64 shuffle partitions.
DataFrame transformations are lazy; actions trigger execution. Engineered features: Hour, Day_Of_Week, Month, Year, Is_Weekend, Is_Rush_Hour,
Is_Night, Weather_Group, Is_Adverse_Weather, Is_Severe, Grid_ID. The full staged run completed
in approximately 9 minutes on the development laptop.
*(Insert Spark UI screenshot.)*

## 16. Spark SQL Analysis
The cleaned Parquet is registered as the view `accidents`. Thirteen queries compute accident counts by severity, hour, weekday, month, year,
weather group/condition, state, city, day vs night, rush hour, weekday×hour heat-map and average environment values by severity; a further
loop analyses 12 road-infrastructure features. *(Insert sample query and output screenshot.)*

## 17. Hotspot Analysis
Method: round Start_Lat/Start_Lng to 2 decimals (≈1.1 km latitude; ≈0.8–1.0 km longitude in the US) → `Grid_ID`; count accidents per cell;
cells in the top 1 % (and ≥ 20 accidents) are hotspots.
Number of grid cells: 124,676; hotspot threshold: 39 accidents; number of hotspots: 1,429.
Top cells include Orlando, FL (241), Baton Rouge, LA (208), New York, NY (198), North Miami, FL (198)
and New York, NY (189).
Limitation: cell edges are arbitrary and cells are only approximately equal in area.

## 18. Risk Score
*Project-defined analytical risk score — not an official government model.*

`risk = 100 × (0.40·freq_norm + 0.30·sev_norm + 0.15·adverse_weather_share + 0.15·night_share)`
* `freq_norm` = min–max of log(1+accidents) over scored cells; `sev_norm` = (avg severity − 1)/3.
* Only cells with ≥ 20 accidents are scored; 4,595 cells were scored.
* Categories by score percentile: Low (< P50), Moderate (P50–P85), High (P85–P97), Critical (≥ P97).
* Rationale for weights: frequency and severity describe the harm and exposure; weather and night describe contributing conditions. Weights are adjustable.
Results: Low 2,293 cells, Moderate 1,608, High 551 and Critical 143. The highest-risk
location was Downey, CA with score 59.3278.
Limitation: no traffic-volume normalisation; categories are relative.

## 19. Machine Learning Methodology
* **Task:** binary classification — Severe (3–4) vs Non-severe (1–2). The selected sample contains
  4,358 severity-1, 398,335 severity-2, 84,063 severity-3 and 13,244 severity-4 records.
* **Algorithms:** Logistic Regression, Decision Tree, Random Forest and Gradient-Boosted Tree,
  all with seed 42 and the same StringIndexer → VectorAssembler feature pipeline.
* **Imbalance:** class weights w = N/(K·n_c) from the training set.
* **Split:** 80 % train / 20 % test, random, seed 42.
* **Features used:** location, hour, day, month, night/weekend/rush-hour flags, weather measurements, weather group, adverse flag, state, 12 road-feature flags.
* **Excluded (leakage-prone) features:**

| Excluded | Reason |
|---|---|---|---|---|---|
| End_Time | known only after the accident ends |
| End_Lat, End_Lng | describe the end of the impact |
| Distance(mi) | length of road affected — post-event measurement |
| Description | free text written after the event, may mention delays |
| Weather_Timestamp | observation metadata, not causal |
| Severity / Is_Severe | the target |
| ID, Source, Street, Zipcode, Airport_Code | identifiers / high-cardinality text |

## 20. Model Evaluation
Accuracy alone is misleading on imbalanced data, so all models were compared (all values are from
`model_comparison.csv`):

| Model | Accuracy | Weighted F1 | Macro F1 | Weighted precision | Weighted recall |
|---|---|
| Logistic Regression | 0.577 | 0.621 | 0.528 | 0.752 | 0.577 |
| Decision Tree | 0.649 | 0.685 | 0.594 | 0.791 | 0.649 |
| Random Forest | 0.655 | 0.690 | 0.598 | 0.791 | 0.655 |
| Gradient-Boosted Tree | 0.700 | 0.729 | 0.636 | 0.805 | 0.700 |

The best model is Gradient-Boosted Tree by macro F1 (0.636). Its AUC-ROC is 0.781 and AUC-PR
is 0.468. On the test set, the severe class had precision 0.364, recall 0.721 and F1 0.484.
The confusion matrix and per-class results are saved in `results/tables/`. The most important
features for the selected model were State, Wind_Speed_mph, Pressure_in, Start_Lng and Start_Lat.

## 21. Dashboard
Streamlit app (`dashboard/app.py`) with KPI cards, trend, hotspot, risk, infrastructure and ML
comparison tabs. Sidebar filters include State, City, Severity, Weather, Year, Month, Hour and
Day/Night. It reads pre-aggregated files and never starts Spark.
*(Insert dashboard screenshots.)*

## 22. Results
Present the actual outputs here, one subsection per research question (Section 4), each with a chart and a table copied from `results/tables/`.
The sample contains 500,000 real records covering 2016–2023. Accidents are concentrated in
Severity 2 (398,335 records); 97,307 records (19.46%) are severe. The grid analysis identifies
1,429 hotspots, while the risk analysis identifies 143 Critical cells. Gradient-Boosted Tree
performed best by macro F1 among the four tested classifiers.

## 23. Key Insights
Write 5–8 insights **only** after viewing the real results, e.g. peak hours, busiest weekday/month, weather association, infrastructure association,
hotspot cities, risk categories, model behaviour. Each insight must cite the table/figure it comes from.
The results are reproducible with seed 42. They should be interpreted as associations: the data
has no traffic-exposure denominator, and the project risk score is not an official safety standard.

## 24. Innovation
* Explainable, tunable risk score combining frequency, severity, weather and night exposure.
* Simple grid hotspot method that is fully parallel and easy to explain.
* Leakage-aware, class-weighted severity model with documented exclusions.
* Decoupled architecture: batch Spark computation → lightweight dashboard.
* Real-world relevance: prioritising patrols, signage and road-engineering work.

## 25. Limitations
* Accident counts are not normalised by traffic volume or road length, so "risk" is relative.
* Dataset contains reported accidents only and Severity measures traffic impact, not injury.
* Weather values come from nearby stations and may not match the exact location/time.
* Random train/test split (not time-based); no hyperparameter tuning; local-mode Spark only.
* Grid cells are approximate; risk weights are subjective.
* Association ≠ causation for infrastructure and weather findings.

## 26. Future Scope
Real-time ingestion with Kafka + Spark Structured Streaming; time-based validation and hyperparameter tuning (CrossValidator); gradient-boosted
trees; traffic-volume and OpenStreetMap road-network features; cluster deployment (YARN/Kubernetes/Databricks); alerting for Critical zones.

## 27. Conclusion
The project demonstrates an end-to-end Spark pipeline from raw accident records to hotspots, a risk score, a severity classifier and an interactive
dashboard. The real-data run shows a 2016–2023 time span, 19.46% severe accidents, 1,429
hotspots and 143 Critical risk cells. Gradient-Boosted Tree was the strongest tested classifier
by macro F1, while severe-class recall was prioritised over accuracy through class weighting.

## 28. References
1. S. Moosavi, M. H. Samavatian, S. Parthasarathy, R. Ramnath, "A Countrywide Traffic Accident Dataset," arXiv:1906.05409, 2019.
2. S. Moosavi, M. H. Samavatian, S. Parthasarathy, R. Teodorescu, R. Ramnath, "Accident Risk Prediction based on Heterogeneous Sparse Data: New Dataset and Insights," ACM SIGSPATIAL, 2019 (arXiv:1909.09638).
3. M. Zaharia et al., "Apache Spark: A Unified Engine for Big Data Processing," Communications of the ACM, 59(11), 2016.
4. X. Meng et al., "MLlib: Machine Learning in Apache Spark," Journal of Machine Learning Research, 17, 2016.
5. L. Breiman, "Random Forests," Machine Learning, 45(1), 2001.
6. Apache Spark documentation: https://spark.apache.org/docs/latest/
7. Streamlit documentation: https://docs.streamlit.io ; Plotly documentation: https://plotly.com/python/
*(Verify each reference details before submission.)*
