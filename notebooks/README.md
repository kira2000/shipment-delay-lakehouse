# Notebooks

Databricks notebooks for the pipeline, in run order:

- `01_bronze_ingest`: load the raw Olist files into bronze Delta tables
- `02_silver_clean`: type, deduplicate and standardize; add delay columns and the silver data-quality log
- `03_gold_model`: build the gold facts, dimensions and KPI tables
- `04_delay_analysis`: statistical analysis of what drives late deliveries
- `07_agent_core`: text-to-SQL report generator over the gold tables
- `08_agent_eval`: evaluation of the agent against reviewed questions
- `09_weekly_brief`: weekly executive brief from fixed, reviewed SQL
- `10_data_quality_tests`: 15 SQL tests on the gold tables (13 blocking, 2 warning); each run is appended to `dq_results`
