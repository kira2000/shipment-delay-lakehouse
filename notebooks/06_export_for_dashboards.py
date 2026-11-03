# Databricks notebook source
# MAGIC %md
# MAGIC # 06 Export for dashboards
# MAGIC Saves the KPI and analysis tables as CSV files in the volume folder `/Volumes/workspace/olist/raw/exports/`.
# MAGIC Then download them from Catalog -> workspace -> olist -> raw -> exports.

# COMMAND ----------

import os
from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

EXPORT_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/raw/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# COMMAND ----------

# Review score by how late the order was (saved as a table first, so it can be exported and reused)
fo = (spark.table("fact_orders")
      .filter(F.col("is_delivered") & F.col("is_late").isNotNull() & F.col("review_score").isNotNull()))

bucket = (F.when(~F.col("is_late"), "0 on time")
           .when(F.col("delay_days") <= 3, "1 late 1-3 days")
           .when(F.col("delay_days") <= 7, "2 late 4-7 days")
           .when(F.col("delay_days") <= 14, "3 late 8-14 days")
           .otherwise("4 late 15+ days"))

(fo.withColumn("lateness", bucket)
   .groupBy("lateness").agg(
       F.count("*").alias("orders"),
       F.avg("review_score").alias("avg_review_score"),
       F.avg(F.when(F.col("review_score") == 1, 1).otherwise(0)).alias("share_1_star"))
   .orderBy("lateness")
   .write.format("delta").mode("overwrite").option("overwriteSchema", True)
   .saveAsTable(f"{CATALOG}.{SCHEMA}.kpi_review_by_lateness"))

# COMMAND ----------

TABLES = [
    "kpi_state_delivery",
    "kpi_seller_delivery",
    "kpi_monthly",
    "kpi_late_vs_review",
    "kpi_eta_gap_state",
    "kpi_category_delivery",
    "kpi_review_by_lateness",
    "analysis_delay_drivers",
]

for name in TABLES:
    pdf = spark.table(f"{CATALOG}.{SCHEMA}.{name}").toPandas()
    pdf.to_csv(f"{EXPORT_DIR}/{name}.csv", index=False)
    print(f"{name}.csv: {len(pdf):,} rows")

print("Files:", [f.name for f in dbutils.fs.ls(EXPORT_DIR)])
