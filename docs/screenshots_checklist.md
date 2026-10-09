# Screenshots to capture AFTER you run the real pipeline
Never use screenshots produced from the synthetic sample in your submission.

| # | What | How / where |
|---|---|---|
| 1 | Spark startup | terminal right after `run_project` starts (shows "Spark 3.5.3 started | UI: ...") |
| 2 | Spark UI | open http://localhost:4040 while the pipeline runs (Jobs & Stages tabs). Set `CA3_PAUSE=1` to keep it open at the end |
| 3 | Dataset schema | terminal output of the raw `printSchema()` at preprocessing start |
| 4 | Preprocessing output | terminal "Cleaning summary" + `results/tables/cleaning_summary.json` |
| 5 | Parquet output | file explorer showing `data/processed/accidents_clean.parquet/` (Year partitions) |
| 6 | Spark SQL result | terminal log or e.g. `results/tables/by_hour.csv` opened in Excel |
| 7 | Hotspot table/map | dashboard → Hotspots tab |
| 8 | Risk score | dashboard → Risk tab (formula + categories) |
| 9 | ML metrics | dashboard → ML Model tab (metric cards) |
| 10 | Confusion matrix | dashboard → ML Model tab (both Counts and Row-normalised) |
| 11 | Feature importance | dashboard → ML Model tab |
| 12 | Dashboard home | KPI cards + Trends tab |
| 13 | Dashboard filters | sidebar with a State and year filter applied |
| 14 | Infrastructure analysis | dashboard → Infrastructure tab |
| 15 | Final project output | terminal "DONE in ... min" line + `results/tables/` folder listing |

Save images into `report/figures/` and reference them in the report.
