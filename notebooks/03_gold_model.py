# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Gold model
# MAGIC Builds the star schema (dimensions + facts) and the KPI tables the dashboards and the report agent will query.
# MAGIC
# MAGIC **Grain:** `fact_order_items` = one row per order item; `fact_orders` = one row per order.
# MAGIC **Late** = delivered to the customer after the estimated delivery date (compared by calendar date).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "workspace"
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

def t(name):
    return spark.table(f"{CATALOG}.{SCHEMA}.{name}")

def save(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"saved {name}: {spark.table(f'{CATALOG}.{SCHEMA}.{name}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

geo = t("silver_geolocation").select(
    F.col("geolocation_zip_code_prefix").alias("zip"), F.col("lat"), F.col("lng"))

dim_customer = (t("silver_customers")
    .join(geo, F.col("customer_zip_code_prefix") == F.col("zip"), "left")
    .drop("zip")
    .withColumnRenamed("lat", "customer_lat")
    .withColumnRenamed("lng", "customer_lng"))

dim_seller = (t("silver_sellers")
    .join(geo, F.col("seller_zip_code_prefix") == F.col("zip"), "left")
    .drop("zip")
    .withColumnRenamed("lat", "seller_lat")
    .withColumnRenamed("lng", "seller_lng"))

dim_product = t("silver_products").select(
    "product_id", "category", "product_weight_g", "product_length_cm",
    "product_height_cm", "product_width_cm", "product_photos_qty")

save(dim_customer, "dim_customer")
save(dim_seller, "dim_seller")
save(dim_product, "dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

orders = t("silver_orders").select(
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_delivered_customer_date", "order_estimated_delivery_date",
    "is_delivered", "delivery_days", "delay_days", "is_late",
    "dq_missing_delivery_date")
reviews = t("silver_order_reviews").select("order_id", "review_score")

# Haversine distance (km) between seller and customer zip-prefix centroids
lat1, lon1 = F.radians("seller_lat"), F.radians("seller_lng")
lat2, lon2 = F.radians("customer_lat"), F.radians("customer_lng")
a = (F.sin((lat2 - lat1) / 2) ** 2
     + F.cos(lat1) * F.cos(lat2) * F.sin((lon2 - lon1) / 2) ** 2)
distance_km = 2 * 6371 * F.asin(F.sqrt(a))

fact_order_items = (t("silver_order_items")
    .join(orders, "order_id")
    .join(reviews, "order_id", "left")
    .join(dim_customer.select("customer_id", "customer_state", "customer_lat", "customer_lng"),
          "customer_id", "left")
    .join(dim_seller.select("seller_id", "seller_state", "seller_lat", "seller_lng"),
          "seller_id", "left")
    .join(dim_product.select("product_id", "category"), "product_id", "left")
    .withColumn("distance_km", distance_km)
    .withColumn("same_state", F.col("customer_state") == F.col("seller_state"))
    .drop("seller_lat", "seller_lng", "customer_lat", "customer_lng"))

save(fact_order_items, "fact_order_items")

# COMMAND ----------

item_agg = (t("silver_order_items").groupBy("order_id").agg(
    F.count("*").alias("num_items"),
    F.countDistinct("seller_id").alias("num_sellers"),
    F.sum("price").alias("order_value"),
    F.sum("freight_value").alias("freight_total")))

dist_agg = fact_order_items.groupBy("order_id").agg(F.avg("distance_km").alias("avg_distance_km"))

pay_agg = t("silver_order_payments").groupBy("order_id").agg(F.sum("payment_value").alias("payment_total"))

fact_orders = (orders
    .join(reviews, "order_id", "left")
    .join(item_agg, "order_id", "left")
    .join(dist_agg, "order_id", "left")
    .join(pay_agg, "order_id", "left")
    .join(dim_customer.select("customer_id", "customer_state"), "customer_id", "left"))

save(fact_orders, "fact_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KPI tables (delivered orders with a delivery date only)

# COMMAND ----------

fo = fact_orders.filter(F.col("is_delivered") & F.col("is_late").isNotNull())
fi = fact_order_items.filter(F.col("is_delivered") & F.col("is_late").isNotNull())

def delivery_kpis(df, keys):
    return (df.groupBy(*keys).agg(
        F.countDistinct("order_id").alias("delivered_orders"),
        F.countDistinct(F.when(F.col("is_late"), F.col("order_id"))).alias("late_orders"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg(F.when(F.col("is_late"), F.col("delay_days"))).alias("avg_days_late_when_late"),
        F.avg("review_score").alias("avg_review_score"),
        F.sum("price").alias("revenue"))
      .withColumn("late_rate", F.col("late_orders") / F.col("delivered_orders")))

# by seller (item grain so each seller gets credit for its own items)
save(delivery_kpis(fi, ["seller_id", "seller_state"]), "kpi_seller_delivery")
save(delivery_kpis(fi, ["customer_state"]), "kpi_state_delivery")
save(delivery_kpis(fi, ["category"]), "kpi_category_delivery")

monthly = (fo.withColumn("month", F.date_trunc("month", "order_purchase_timestamp"))
    .groupBy("month").agg(
        F.count("*").alias("delivered_orders"),
        F.sum(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_orders"),
        F.avg("delivery_days").alias("avg_delivery_days"),
        F.avg("review_score").alias("avg_review_score"))
    .withColumn("late_rate", F.col("late_orders") / F.col("delivered_orders")))
save(monthly, "kpi_monthly")

late_vs_review = (fo.filter(F.col("review_score").isNotNull())
    .groupBy("is_late").agg(
        F.count("*").alias("orders"),
        F.avg("review_score").alias("avg_review_score"),
        F.avg(F.when(F.col("review_score") == 1, 1).otherwise(0)).alias("share_1_star"),
        F.avg(F.when(F.col("review_score") == 5, 1).otherwise(0)).alias("share_5_star")))
save(late_vs_review, "kpi_late_vs_review")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Headline numbers (copy into the README)

# COMMAND ----------

overall = fo.agg(
    F.count("*").alias("delivered_orders"),
    F.avg(F.when(F.col("is_late"), 1).otherwise(0)).alias("late_rate"),
    F.avg("delivery_days").alias("avg_delivery_days"))
display(overall)
display(spark.table(f"{CATALOG}.{SCHEMA}.kpi_late_vs_review"))
display(spark.table(f"{CATALOG}.{SCHEMA}.kpi_state_delivery").orderBy(F.desc("late_rate")))
