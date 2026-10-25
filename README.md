# Traffic Congestion and Road Accident Analytics Using Apache Spark
** | B.Tech CSE, Batch 2023-27, Semester VII**

## 1. Project overview
A Big Data pipeline that analyses millions of US road-accident records with **PySpark**, finds where/when accidents occur,
detects ~1 km **hotspots**, computes an explainable **risk score**, trains a **Spark MLlib Random Forest** to predict
accident severity, and presents everything in a **Streamlit + Plotly** dashboard.

> ⚠️ **Status:** the production pipeline uses a reproducible, proportional 500,000-row sample of
> the real Kaggle CSV. Generated numbers in the report/dashboard come from the run.

## 2. Features
* PySpark ETL: schema inspection, type conversion, missing values, duplicates, invalid-row removal, feature engineering, Parquet output
* 13 Spark SQL analyses + infrastructure analysis
* Grid-based hotspot detection and a project-defined risk score (Low/Moderate/High/Critical)
* Leakage-free comparison of Logistic Regression, Decision Tree, Random Forest and Gradient-Boosted Tree with class weights
* Accuracy, macro/weighted F1, precision/recall, per-model confusion matrices, AUC and saved Plotly comparison charts
* Dashboard with KPI cards, 10 visualisations and filters (state, severity, weather, year)
* One-command pipeline, central config, clear error messages

## 3. Technology stack
Python 3.10/3.11 · Apache Spark 3.5 (PySpark, Spark SQL, MLlib) · Pandas · NumPy · PyArrow/Parquet · Plotly · Streamlit
(Kafka deliberately **not** used – data is historical/batch.)

## 4. Requirements
* **Python 3.10 or 3.11** (python.org)
* **Java 11 or 17** (Spark needs it) – check with `java -version`. e.g. Temurin/Adoptium JDK 17
* ≥ 8 GB RAM recommended for the full dataset (4 GB is the default Spark driver memory; see Troubleshooting)
* ~10 GB free disk

## 5. Installation
```bash
# Linux / macOS
bash scripts/setup_environment.sh
# Windows (Command Prompt)
scripts\setup_environment.bat
```
(or manually: `python -m venv .venv`, activate it, `pip install -r requirements.txt`)

## 6. Dataset download
See **`data/README.md`**. In short: download *US Accidents* from Kaggle
(https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents), unzip, and put the CSV in **`data/raw/`**.
The full dataset is NOT in this ZIP.

## 7. Folder structure
```
Traffic_Accident_BigData/
├── README.md  requirements.txt  .gitignore
├── data/        README.md  raw/ (you add CSV)  processed/ (Parquet output)  sample/ (synthetic test CSV)
├── src/         config.py spark_session.py preprocessing.py analysis.py hotspot_analysis.py
│                risk_score.py model.py run_pipeline.py utils.py make_sample.py
├── dashboard/   app.py  assets/
├── results/     tables/ (outputs for dashboard)  figures/  models/
├── notebooks/   exploratory_analysis.ipynb
├── report/      project_report.md  figures/
├── scripts/     setup_environment.* run_project.* run_dashboard.*
└── docs/        architecture.md methodology.md viva_questions.md rubric_mapping.md screenshots_checklist.md
```

## 8. How to run
**Step A – smoke test (2 minutes, fake data, optional but recommended):**
```bash
bash scripts/run_project.sh --sample        # Windows: scripts\run_project.bat --sample
bash scripts/run_dashboard.sh --sample      # Windows: scripts\run_dashboard.bat --sample
```
Sample outputs go to `results_sample/` and show a red "SAMPLE MODE" banner – never use them in your report.

**Step B – real run (after placing `US_Accidents_March23.csv` in `data/raw/`):**
```bash
bash scripts/run_project.sh                 # Windows: scripts\run_project.bat
```
This runs all stages in order: `preprocess → analysis → hotspot → risk → model`. If the valid,
deduplicated input exceeds 500,000 rows, preprocessing takes a fixed-seed proportional stratified
sample by `Severity`; exact counts and the date range are saved in `cleaning_summary.json`.
Run a single stage with `--stage preprocess|analysis|hotspot|risk|model`, e.g. `python -m src.run_pipeline --stage model`.
Keep the Spark UI open for a screenshot: set `CA3_PAUSE=1` before running.

| Stage | Command | Main output |
|---|---|---|
| Preprocess | `--stage preprocess` | `data/processed/accidents_clean.parquet`, `results/tables/cleaning_summary.json` |
| Spark SQL | `--stage analysis` | `results/tables/by_*.csv`, `kpis.json`, `cube_*.parquet` |
| Hotspots | `--stage hotspot` | `hotspots_top.csv`, `hotspot_method.json` |
| Risk | `--stage risk` | `risk_cells_top.csv`, `risk_category_counts.csv`, `risk_method.json` |
| Model comparison | `--stage model` | `model_comparison.csv`, per-model confusion/per-class tables, `model_metrics.json`, feature importance and comparison charts |

Useful options (environment variables): `SPARK_DRIVER_MEMORY=8g`, `TARGET_ROWS=500000`,
`TRAIN_SAMPLE_FRACTION=1.0`, `RF_NUM_TREES=50`, `RF_MAX_DEPTH=10`,
`MODEL_MODE=binary|multiclass`. Gradient-Boosted Tree is included for binary mode because Spark GBT
does not support multiclass classification.

## 9. Launch the dashboard
```bash
bash scripts/run_dashboard.sh               # or:  streamlit run dashboard/app.py
```
Open http://localhost:8501. The dashboard reads `results/tables/` and **never starts Spark**.
It provides state, city, severity, weather, year, month, hour and day/night filters, plus
trend, hotspot, risk, infrastructure and model-comparison tabs.

### Run with Docker
The Docker image runs the dashboard against the precomputed results included in this
repository. It does not run the Spark pipeline.

```bash
docker build -t traffic-accident-dashboard .
docker run --rm -p 8501:8501 traffic-accident-dashboard
```

Open http://localhost:8501 after the container starts. To use newly generated results,
rebuild the image after running the pipeline.

## 10. Expected outputs
After a successful run `results/tables/` contains the files in the table above, plus `schema_clean.txt`,
`model_features.json` (used vs excluded features) and ~15 other CSVs. The terminal ends with `DONE in X min`.
