# CA3 Rubric Mapping (12 marks)

| # | Criterion (2 marks each) | How this project satisfies it | Evidence (file / screenshot) |
|---|---|---|---|
| 1 | **Problem Identification** | Road accidents cause deaths, injuries and congestion. The project analyses *where, when and under what conditions* accidents happen, finds hotspots, scores risk and predicts severity - a real, clearly stated problem with answerable questions (README §1, report §4-§6). | `report/project_report.md` §3-6 |
| 2 | **Tool / Framework Usage** | PySpark (ETL), Spark SQL (analytics), Spark MLlib (Random Forest pipeline), Parquet (storage), Pandas/NumPy (small outputs only), Plotly + Streamlit (dashboard). Each tool has a justified role; Kafka deliberately not used. | `src/*.py`, Spark UI screenshot |
| 3 | **Data Processing & Analysis** | Distributed cleaning (types, nulls, duplicates, invalid rows), feature engineering, 13 Spark SQL queries, group-by aggregations, ~1 km grid hotspot analysis, risk score, leakage-free ML with imbalance handling and full metrics. | `src/preprocessing.py`, `analysis.py`, `hotspot_analysis.py`, `model.py`, `results/tables/` |
| 4 | **Visualization & Insights** | Streamlit dashboard: KPI cards, severity/hour/day/month/weather charts, hotspot map, risk categories, infrastructure analysis, model metrics, confusion matrix, filters (state, severity, weather, year). Insights written from *actual* outputs after the run. | `dashboard/app.py`, dashboard screenshots, report §22-23 |
| 5 | **Innovation & Real-world Relevance** | Explainable project-defined risk score; grid-based hotspots; class-weighted severity model with documented leakage exclusions; decoupled "Spark computes → dashboard reads" architecture. Usable by traffic authorities for patrol/engineering prioritisation (as decision support). | `src/risk_score.py`, report §24 |
| 6 | **Report** | Full 28-section report with architecture, methodology, evaluation, limitations, future scope, references; plus README, methodology, architecture, viva and screenshot docs. Results placeholders are filled from real pipeline output. | `report/project_report.md`, `docs/` |

## Viva tips per criterion
* Problem → be ready to state the 9 questions the project answers.
* Tools → explain why Spark over Pandas, and what each module does.
* Processing → walk through `preprocessing.py` top-to-bottom; explain median imputation and dedup by ID.
* Visualization → show filters, then explain one chart and what *your* data says.
* Innovation → derive the risk-score formula on the board; admit its limits.
* Report → know where every number in the report comes from (`results/tables/`).
