# Viva Questions & Short Answers

**1. Why is this a Big Data problem?**
The US Accidents data has millions of rows and 40+ columns (several GB), mixing time, geography, weather and road data. It strains single-machine tools like Excel or plain Pandas (volume), has varied types (variety), and keeps growing (velocity of collection).

**2. Why Spark?**
Spark processes data in parallel across CPU cores/machines and keeps intermediate data in memory, so it is much faster and more scalable than disk-based MapReduce or single-threaded Pandas. It also bundles SQL, ML and streaming in one engine.

**3. Why PySpark?**
It lets us use Spark from Python, the language of the data-science ecosystem (Pandas, Plotly, Streamlit), while Spark's JVM engine does the heavy lifting.

**4. Why Spark SQL?**
SQL is familiar and readable; Spark SQL queries compile to the same optimised plan (Catalyst) as DataFrame code, so we get speed plus clarity for aggregations.

**5. Why MLlib?**
MLlib trains models in a distributed way directly on Spark DataFrames, so the model scales with the data and fits in the same pipeline without exporting data to scikit-learn.

**6. Why Random Forest?**
It handles mixed numeric/categorical features, non-linear relations and outliers, needs little scaling, gives feature importances (explainability), supports class weights, and is robust against overfitting compared with one tree.

**7. Why not just Pandas?**
Pandas loads everything into one machine's RAM and uses one core for most operations; several GB of CSV may not fit. Pandas is used here only for small aggregated tables.

**8. What is distributed processing?**
Splitting data into partitions processed in parallel by multiple workers/cores, then combining the results. We run Spark in local mode (cores as workers) but the code would run unchanged on a cluster.

**9. What is a Spark DataFrame?**
A distributed table with named, typed columns. Operations are lazy (a plan is built) and run only when an action like `count()`, `write` or `toPandas()` is called.

**10. What is lazy evaluation?**
Transformations (`filter`, `groupBy`, `withColumn`) only build a plan; Spark optimises the whole plan and executes it at an action. This avoids unnecessary work.

**11. What is partitioning?**
Data is divided into chunks (partitions), each processed by one task. `spark.sql.shuffle.partitions` controls partitions after shuffles; we also write Parquet partitioned by `Year` so queries on a year read only that folder.

**12. What is Parquet and why use it?**
A columnar, compressed file format that stores the schema. Reading only needed columns is much faster and files are far smaller than CSV; it avoids re-parsing the raw CSV in every stage.

**13. What is data leakage?**
Using information in training that would not be available at prediction time (or that is derived from the target), giving unrealistically good results. We excluded `End_Time`, distance, description, end coordinates and the target itself.

**14. Why is accuracy insufficient here?**
If most accidents are non-severe, predicting "non-severe" always gives high accuracy but catches no severe accident. So we report weighted F1, precision, recall, per-class metrics, confusion matrix and a majority-class baseline.

**15. What is class imbalance and how did you handle it?**
Some classes are much rarer than others. We used class weights (`N / (K·n_class)`, from training data only) so the forest pays more attention to the rare severe class, and evaluated on the untouched test distribution.

**16. Why binary severity instead of 4 classes?**
Severity 1 and 4 are rare, making multiclass metrics unstable; severe vs non-severe is more reliable, easier to interpret and still operationally useful. Multiclass mode exists in `config.py`.

**17. How does hotspot detection work?**
Round latitude/longitude to 2 decimals (~1 km cell), count accidents per cell with a Spark `groupBy`, and flag cells in the top 1 % by count (with a minimum count) as hotspots.

**18. Why a grid instead of DBSCAN/K-means?**
It is simple, fully parallel, deterministic and easy to explain; DBSCAN needs neighbour searches that scale poorly and parameter tuning. A limitation is that cell borders are arbitrary.

**19. How is the risk score calculated?**
`100 × (0.40·frequency + 0.30·severity + 0.15·adverse-weather share + 0.15·night share)`, each normalised to 0-1 (frequency via log + min-max). Cells are then ranked into Low/Moderate/High/Critical by percentile.

**20. Why those weights? Is it an official model?**
They are judgement-based project choices (frequency and severity matter most). It is *not* an official government model; it is an analytical ranking and weights can be changed in `config.py`.

**21. Why log-scale the frequency?**
A few huge cells would otherwise squash every other cell's score near zero; the log keeps differences visible.

**22. How did you handle missing values and duplicates?**
Rows missing time/coordinates/severity are dropped; duplicates removed by `ID`; numeric weather fields filled with the median (robust to outliers); text fields set to "Unknown". Counts are logged in `cleaning_summary.json`.

**23. Why was Kafka not used?**
The dataset is historical (batch), not a live stream. Kafka would add complexity without benefit. It is listed under future scope for real-time accident feeds.

**24. How does the dashboard work?**
Spark saves small aggregated tables (CSV/Parquet/JSON). Streamlit loads them with Pandas, applies sidebar filters on pre-aggregated "cubes" and draws Plotly charts. It never starts Spark, so it opens instantly.

**25. Why not query the 7M rows directly from Streamlit?**
It would need Spark or huge memory on every page load. Pre-aggregation moves the heavy work to a one-time batch job.

**26. How do you evaluate the model and what do the metrics mean?**
Precision: of predicted severe, how many were truly severe. Recall: of truly severe, how many were found. F1: their harmonic mean. Confusion matrix shows exactly which classes get confused. AUC-ROC/PR measure ranking quality.

**27. How do you ensure reproducibility?**
Fixed seed 42 for the split and forest, central configuration, relative paths, pinned requirements and one pipeline command.

**28. What are the limitations?**
No traffic-volume data (counts ≠ risk rate); only reported accidents; severity is based on traffic impact, not injury; weather values come from nearby stations; random (not time-based) split; grid cells are approximate; the risk score is subjective.

**29. How can the project be scaled?**
Run Spark on a cluster (YARN/Kubernetes/Databricks), store data in HDFS/S3, increase partitions and executors, tune the forest, add Kafka + Structured Streaming for live data. The code itself is already Spark-native.

**30. What would you do next?**
Time-based validation, hyperparameter tuning (CrossValidator), gradient-boosted trees, traffic-volume normalisation, road-network (OpenStreetMap) features, and live prediction.

**31. Which Spark components did you use?**
Spark Core/SQL (DataFrames, SQL, Catalyst), MLlib (Pipeline, StringIndexer, VectorAssembler, RandomForestClassifier, evaluators) and the Spark UI for monitoring jobs.

**33. Why compare multiple classification models?**
Logistic Regression is a linear baseline, a Decision Tree gives simple non-linear rules, Random Forest reduces the variance of many trees,
and GBT builds trees sequentially to correct errors. Comparing them on the same split and features makes the choice evidence-based.

**34. Why use approximately 500,000 records?**
The source CSV has millions of rows, while a 500,000-row real-data sample is practical on a student laptop and still large enough for
Spark experiments. It is selected reproducibly and proportionally by Severity, so class distribution is preserved as reasonably as possible.

**35. How is the best model selected?**
The pipeline ranks models by macro F1, which gives each class equal importance and is more informative than accuracy for an imbalanced target.
The selected model and its actual metrics are written to `model_metrics.json` and displayed in the dashboard.

**36. How does dashboard filtering work?**
The dashboard reads Spark-generated cubes rather than starting Spark. State, city, severity, weather, year, month, hour and day/night filters
are applied to pre-aggregated data before charts are drawn.

**32. What is the difference between a transformation and an action?**
Transformations (e.g. `filter`) lazily define a new DataFrame; actions (e.g. `count`, `write`, `toPandas`) trigger execution.
