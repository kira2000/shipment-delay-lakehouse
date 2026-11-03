# Databricks notebook source
# MAGIC %md
# MAGIC # 05 ETA check
# MAGIC Is lateness a *promise* problem? For each customer state (and for peak months) compare the promised delivery window with what actually happened, and compute how many extra days the estimate would need to hit a 95% on-time rate.
# MAGIC
# MAGIC `delay_days` = actual delivery date minus estimated date (negative = early).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

fo = (spark.table(f"{CATALOG}.{SCHEMA}.fact_orders")
      .filter(F.col("is_delivered") & F.col("is_late").isNotNull())
      .withColumn("promised_days",
                  F.datediff(F.to_date("order_estimated_delivery_date"), F.to_date("order_purchase_timestamp")))
      .withColumn("actual_days",
                  F.datediff(F.to_date("order_delivered_customer_date"), F.to_date("order_purchase_timestamp"))))

def eta_table(df, keys):
    return (df.groupBy(*keys).agg(
        F.count("*").alias("delivered_orders"),
        F.avg("promised_days").alias("avg_promised_days"),
        F.avg("actual_days").alias("avg_actual_days"),
        F.avg(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_rate"),
        F.percentile_approx("delay_days", 0.95).alias("p95_delay_days"))
        .withColumn("extra_eta_days_for_95pct_on_time", F.greatest(F.lit(0), F.col("p95_delay_days"))))

# COMMAND ----------

by_state = eta_table(fo, ["customer_state"])
by_state.write.format("delta").mode("overwrite").option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.kpi_eta_gap_state")
display(by_state.filter("delivered_orders >= 100").orderBy(F.desc("late_rate")))

# COMMAND ----------

# Peak (Nov-Dec) versus other months
peak = (fo.withColumn("period",
                      F.when(F.month("order_purchase_timestamp").isin(11, 12), "peak (Nov-Dec)").otherwise("other months")))
display(eta_table(peak, ["period"]))

# COMMAND ----------

# Nov 2017 specifically (the one real peak in the data) versus the months around it
m = (fo.withColumn("month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
       .filter(F.col("month").between("2017-09", "2018-02")))
display(eta_table(m, ["month"]).orderBy("month"))
