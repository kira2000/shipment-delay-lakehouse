# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Bronze ingest
# MAGIC Reads the 9 raw Olist CSVs from a Unity Catalog volume and writes them as Delta tables.
# MAGIC Bronze keeps data exactly as received (all columns as strings) and adds two lineage columns: `ingested_at` and `source_file`.

# COMMAND ----------

# Change these three names if yours differ
CATALOG = "workspace"
SCHEMA = "olist"
VOLUME = "raw"

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# file name -> bronze table name
FILES = {
    "olist_customers_dataset.csv": "bronze_customers",
    "olist_geolocation_dataset.csv": "bronze_geolocation",
    "olist_order_items_dataset.csv": "bronze_order_items",
    "olist_order_payments_dataset.csv": "bronze_order_payments",
    "olist_order_reviews_dataset.csv": "bronze_order_reviews",
    "olist_orders_dataset.csv": "bronze_orders",
    "olist_products_dataset.csv": "bronze_products",
    "olist_sellers_dataset.csv": "bronze_sellers",
    "product_category_name_translation.csv": "bronze_category_translation",
}

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("Files found in volume:")
for f in dbutils.fs.ls(RAW_PATH):
    print(" ", f.name)

# COMMAND ----------

results = []

for file_name, table in FILES.items():
    path = f"{RAW_PATH}/{file_name}"
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)   # bronze = raw strings; types are cast in silver
        .option("multiLine", True)      # review comments contain line breaks
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", True)
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{table}")
    )
    n = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    results.append((table, n))
    print(f"{table}: {n:,} rows")

# COMMAND ----------

# Row counts: copy these into the README
display(spark.createDataFrame(results, ["table", "row_count"]))

# COMMAND ----------

# Check the schema of one table (repeat for others)
display(spark.sql(f"DESCRIBE TABLE {CATALOG}.{SCHEMA}.bronze_orders"))
