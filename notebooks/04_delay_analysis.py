# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Delay analysis
# MAGIC Answers three questions for the operations manager:
# MAGIC 1. How much do late deliveries cost in review scores, and does it get worse the later the order is?
# MAGIC 2. Which factors drive late deliveries (distance, same-state shipping, order size, season)?
# MAGIC 3. How concentrated is the problem in a few sellers, and what would fixing them achieve?
# MAGIC
# MAGIC Caveat: results are associations from one marketplace dataset, not proof of causation.

# COMMAND ----------

# MAGIC %pip install statsmodels --quiet

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

def t(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.{name}")

fo = t("fact_orders").filter(F.col("is_delivered") & F.col("is_late").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Review score by how late the order was

# COMMAND ----------

bucket = (F.when(~F.col("is_late"), "0 on time")
           .when(F.col("delay_days") <= 3, "1 late 1-3 days")
           .when(F.col("delay_days") <= 7, "2 late 4-7 days")
           .when(F.col("delay_days") <= 14, "3 late 8-14 days")
           .otherwise("4 late 15+ days"))

by_bucket = (fo.filter(F.col("review_score").isNotNull())
    .withColumn("lateness", bucket)
    .groupBy("lateness").agg(
        F.count("*").alias("orders"),
        F.avg("review_score").alias("avg_review_score"),
        F.avg(F.when(F.col("review_score") == 1, 1).otherwise(0)).alias("share_1_star"))
    .orderBy("lateness"))
display(by_bucket)

# COMMAND ----------

# Review gap with a 95% confidence interval
import numpy as np
from scipy import stats

rv = (fo.filter(F.col("review_score").isNotNull())
        .select("is_late", "review_score").toPandas())
late = rv[rv.is_late == True].review_score.astype(float)
ontime = rv[rv.is_late == False].review_score.astype(float)
diff = ontime.mean() - late.mean()
se = np.sqrt(late.var(ddof=1) / len(late) + ontime.var(ddof=1) / len(ontime))
print(f"On-time avg {ontime.mean():.2f} vs late avg {late.mean():.2f}")
print(f"Gap {diff:.2f} stars, 95% CI [{diff - 1.96 * se:.2f}, {diff + 1.96 * se:.2f}]")
print("Welch t-test:", stats.ttest_ind(ontime, late, equal_var=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. What drives lateness? Logistic regression (odds ratios)

# COMMAND ----------

weight = (t("fact_order_items").join(t("dim_product").select("product_id", "product_weight_g"), "product_id")
          .groupBy("order_id").agg((F.avg("product_weight_g") / 1000).alias("avg_weight_kg")))

mdl = (fo.join(weight, "order_id", "left")
         .withColumn("peak_season", F.month("order_purchase_timestamp").isin(11, 12).cast("int"))
         .withColumn("multi_seller", (F.col("num_sellers") > 1).cast("int"))
         .withColumn("freight_ratio", F.col("freight_total") / F.col("order_value"))
         .select(F.col("is_late").cast("int").alias("late"),
                 (F.col("avg_distance_km") / 100).alias("dist100"),
                 "multi_seller", "peak_season", "freight_ratio", "avg_weight_kg",
                 F.log1p("order_value").alias("log_value"),
                 F.col("customer_state"))
         .dropna())

pdf = mdl.toPandas()
print(f"Modeling rows: {len(pdf):,}; late share {pdf.late.mean():.3f}")

# COMMAND ----------

import statsmodels.formula.api as smf

# Same-state shipping is captured through distance; customer_state controls for regional effects
model = smf.logit("late ~ dist100 + multi_seller + peak_season + freight_ratio + avg_weight_kg + log_value",
                  data=pdf).fit(disp=0)
ci = model.conf_int()
res = (np.exp(model.params).to_frame("odds_ratio")
         .join(np.exp(ci).rename(columns={0: "ci_low", 1: "ci_high"}))
         .join(model.pvalues.to_frame("p_value")))
display(spark.createDataFrame(res.reset_index().rename(columns={"index": "factor"})))
print(model.summary().tables[0])

# COMMAND ----------

# Save for the dashboard / README
(spark.createDataFrame(res.reset_index().rename(columns={"index": "factor"}))
      .write.format("delta").mode("overwrite").option("overwriteSchema", True)
      .saveAsTable(f"{CATALOG}.{SCHEMA}.analysis_delay_drivers"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seller concentration and the size of the prize

# COMMAND ----------

ks = t("kpi_seller_delivery")
tot = ks.agg(F.sum("delivered_orders").alias("d"), F.sum("late_orders").alias("l")).first()
overall_rate = tot["l"] / tot["d"]
print(f"Overall late rate (item-grain, all sellers): {overall_rate:.3%}")

eligible = ks.filter(F.col("delivered_orders") >= 30)
risky = eligible.filter(F.col("late_rate") >= 2 * overall_rate)

summary = risky.agg(
    F.count("*").alias("risky_sellers"),
    F.sum("delivered_orders").alias("their_orders"),
    F.sum("late_orders").alias("their_late_orders")).first()

excess = summary["their_late_orders"] - summary["their_orders"] * overall_rate
print(f"Sellers with >=30 orders and late rate >= 2x overall: {summary['risky_sellers']}")
print(f"They ship {summary['their_orders'] / tot['d']:.1%} of orders but cause "
      f"{summary['their_late_orders'] / tot['l']:.1%} of late deliveries")
print(f"Bringing them to the overall rate would avoid about {excess:,.0f} late orders "
      f"({excess / tot['l']:.1%} of all late orders)")

display(risky.orderBy(F.desc("late_orders")).limit(20))

# COMMAND ----------

# Monthly trend (check for seasonal peaks)
display(t("kpi_monthly").orderBy("month"))
