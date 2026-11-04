# Databricks notebook source
# MAGIC %md
# MAGIC # 10 Data quality tests (gate for the scheduled pipeline)
# MAGIC Runs a fixed suite of tests against the gold tables. Each test is a SQL query that returns the number of **offending rows** (0 = pass). Results are appended to `dq_results` with a run timestamp so failures can be tracked over time. If any `error`-severity test fails, the notebook raises, which fails the Databricks Job task and stops downstream tasks (the weekly brief) from publishing numbers built on bad data.
# MAGIC
# MAGIC This is a hand-built test runner in the style of dbt tests (unique, not-null, accepted range, reconciliation). It is not dbt.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# (name, severity, SQL returning offending-row count)
TESTS = [
 # --- structure ---
 ("fact_orders_not_empty", "error", "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM fact_orders"),
 ("kpi_monthly_not_empty", "error", "SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END FROM kpi_monthly"),
 # --- uniqueness / nulls ---
 ("fact_orders_order_id_unique", "error",
  "SELECT COUNT(*) FROM (SELECT order_id FROM fact_orders GROUP BY order_id HAVING COUNT(*) > 1)"),
 ("fact_orders_order_id_not_null", "error", "SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL"),
 ("dim_seller_seller_id_unique", "error",
  "SELECT COUNT(*) FROM (SELECT seller_id FROM dim_seller GROUP BY seller_id HAVING COUNT(*) > 1)"),
 ("kpi_state_unique_state", "error",
  "SELECT COUNT(*) FROM (SELECT customer_state FROM kpi_state_delivery GROUP BY customer_state HAVING COUNT(*) > 1)"),
 # --- accepted ranges ---
 ("kpi_monthly_late_rate_in_0_1", "error",
  "SELECT COUNT(*) FROM kpi_monthly WHERE late_rate < 0 OR late_rate > 1"),
 ("kpi_state_late_rate_in_0_1", "error",
  "SELECT COUNT(*) FROM kpi_state_delivery WHERE late_rate < 0 OR late_rate > 1"),
 ("kpi_seller_late_rate_in_0_1", "error",
  "SELECT COUNT(*) FROM kpi_seller_delivery WHERE late_rate < 0 OR late_rate > 1"),
 ("kpi_late_never_exceeds_delivered", "error",
  "SELECT COUNT(*) FROM kpi_monthly WHERE late_orders > delivered_orders"),
 ("avg_review_score_in_1_5", "error",
  "SELECT COUNT(*) FROM kpi_late_vs_review WHERE avg_review_score < 1 OR avg_review_score > 5"),
 ("avg_delivery_days_non_negative", "warn",
  "SELECT COUNT(*) FROM kpi_monthly WHERE avg_delivery_days < 0"),
 # --- reconciliation between layers ---
 ("monthly_delivered_matches_fact_orders", "error",
  """SELECT CASE WHEN (SELECT SUM(delivered_orders) FROM kpi_monthly) =
                    (SELECT COUNT(DISTINCT order_id) FROM fact_orders WHERE is_delivered = true AND is_late IS NOT NULL)
             THEN 0 ELSE 1 END"""),
 ("state_delivered_matches_monthly", "error",
  """SELECT CASE WHEN (SELECT SUM(delivered_orders) FROM kpi_state_delivery) =
                    (SELECT SUM(delivered_orders) FROM kpi_monthly)
             THEN 0 ELSE 1 END"""),
 ("late_vs_review_orders_match_fact", "warn",
  """SELECT CASE WHEN (SELECT SUM(orders) FROM kpi_late_vs_review) <=
                    (SELECT COUNT(DISTINCT order_id) FROM fact_orders)
             THEN 0 ELSE 1 END"""),
]

# COMMAND ----------

import datetime as dt
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

run_ts = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
results = []
for name, severity, sql in TESTS:
    try:
        bad = spark.sql(sql).collect()[0][0]
        bad = int(bad) if bad is not None else 1   # NULL result means the check could not be computed
        status = "pass" if bad == 0 else "fail"
        err = None
    except Exception as e:
        bad, status, err = -1, "error", str(e)[:300]
    results.append(Row(run_ts=run_ts, test=name, severity=severity, offending_rows=bad, status=status, detail=err))

# explicit schema: 'detail' is None for every passing run, which Spark cannot infer a type from
dq_schema = StructType([
    StructField("run_ts", TimestampType(), False),
    StructField("test", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("offending_rows", LongType(), True),
    StructField("status", StringType(), False),
    StructField("detail", StringType(), True),
])
res_df = spark.createDataFrame(results, schema=dq_schema)
res_df.write.mode("append").saveAsTable("dq_results")
display(res_df.select("test", "severity", "status", "offending_rows", "detail"))

# COMMAND ----------

failed_err  = [r for r in results if r.status != "pass" and r.severity == "error"]
failed_warn = [r for r in results if r.status != "pass" and r.severity == "warn"]
print(f"{len(results) - len(failed_err) - len(failed_warn)}/{len(results)} passed; "
      f"{len(failed_warn)} warnings; {len(failed_err)} blocking failures")
if failed_err:
    raise Exception("Data quality gate failed: " + ", ".join(r.test for r in failed_err))
