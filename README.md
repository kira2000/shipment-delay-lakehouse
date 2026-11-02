# Shipment Delay Lakehouse

A Databricks lakehouse (Delta Lake, bronze / silver / gold) that finds which sellers, regions and product categories drive late deliveries on a marketplace, and what late deliveries cost in customer reviews.

**Status:** in progress

## Business problem
Marketplace operations cannot see which sellers, regions or categories cause late deliveries, and late deliveries hurt review scores and repeat purchases. See [docs/problem_brief.md](docs/problem_brief.md).

## Data
[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (9 CSV files). This is a proxy for a mid-size marketplace, so findings are illustrative.

## Architecture
- **Bronze:** raw CSVs ingested as Delta tables with `ingested_at` and `source_file`
- **Silver:** cleaned, deduplicated, validated tables
- **Gold:** star schema and KPI tables (on-time rate, delay by seller / region / category)
- **Serving:** Power BI and Tableau Public dashboards, delay-driver analysis, recommendation memo

## Results
_To be added: key numbers, dashboard screenshots, recommendation._

## Repo layout
- `notebooks/` Databricks notebooks (`01_bronze_ingest`, `02_silver_clean`, `03_gold_model`, ...)
- `docs/` problem brief and data-issues log

## Tech
Databricks, Delta Lake, PySpark, Spark SQL, Power BI, Tableau Public
