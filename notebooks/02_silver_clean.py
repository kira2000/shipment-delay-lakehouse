# Databricks notebook source
# MAGIC %md
# MAGIC # 02 Silver clean
# MAGIC Casts types, deduplicates, standardizes and validates the bronze tables.
# MAGIC Every check is logged to `silver_dq_log` (table, check, count, how it was handled). Use that table to fill `docs/data_issues.md`.

# COMMAND ----------

from pyspark.sql import functions as F, Window

CATALOG = "workspace"
SCHEMA = "olist"
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

dq_rows = []

def bronze(name):
    """Read a bronze table and strip any BOM / whitespace from column names."""
    df = spark.table(f"{CATALOG}.{SCHEMA}.{name}")
    return df.toDF(*[c.replace("\ufeff", "").strip() for c in df.columns])

def log(table, check, n, handling):
    dq_rows.append((table, check, int(n), handling))
    print(f"[{table}] {check}: {int(n):,} -> {handling}")

def save(df, name):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
       .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"saved {name}: {spark.table(f'{CATALOG}.{SCHEMA}.{name}').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

o = bronze("bronze_orders")
for c in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
          "order_delivered_customer_date", "order_estimated_delivery_date"]:
    o = o.withColumn(c, F.to_timestamp(c))

log("orders", "duplicate order_id rows", o.count() - o.dropDuplicates(["order_id"]).count(), "dropped duplicates")
o = o.dropDuplicates(["order_id"])

delivered = F.col("order_status") == "delivered"
deliv_cust = F.col("order_delivered_customer_date")
purchase = F.col("order_purchase_timestamp")

log("orders", "status is not delivered", o.filter(~delivered).count(), "kept; is_delivered = false")
log("orders", "delivered but missing delivered_customer_date", o.filter(delivered & deliv_cust.isNull()).count(),
    "kept; dq_missing_delivery_date = true")
log("orders", "delivered_customer_date before purchase", o.filter(deliv_cust < purchase).count(),
    "kept; dq_delivered_before_purchase = true")
log("orders", "delivered_carrier_date before purchase",
    o.filter(F.col("order_delivered_carrier_date") < purchase).count(), "kept; review in analysis")

o = (o
     .withColumn("is_delivered", delivered)
     .withColumn("delivery_days", F.datediff(deliv_cust, purchase))
     .withColumn("delay_days", F.datediff(F.to_date(deliv_cust), F.to_date("order_estimated_delivery_date")))
     .withColumn("is_late", F.when(delivered & deliv_cust.isNotNull(), F.col("delay_days") > 0))
     .withColumn("dq_missing_delivery_date", delivered & deliv_cust.isNull())
     .withColumn("dq_delivered_before_purchase", deliv_cust < purchase))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers and sellers

# COMMAND ----------

cu = bronze("bronze_customers")
log("customers", "duplicate customer_id rows", cu.count() - cu.dropDuplicates(["customer_id"]).count(), "dropped duplicates")
cu = (cu.dropDuplicates(["customer_id"])
        .withColumn("customer_city", F.lower(F.trim("customer_city")))
        .withColumn("customer_state", F.upper(F.trim("customer_state"))))

se = bronze("bronze_sellers")
log("sellers", "duplicate seller_id rows", se.count() - se.dropDuplicates(["seller_id"]).count(), "dropped duplicates")
se = (se.dropDuplicates(["seller_id"])
        .withColumn("seller_city", F.lower(F.trim("seller_city")))
        .withColumn("seller_state", F.upper(F.trim("seller_state"))))

log("orders", "customer_id not found in customers", o.join(cu, "customer_id", "left_anti").count(), "flag only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order items

# COMMAND ----------

it = (bronze("bronze_order_items")
      .withColumn("order_item_id", F.col("order_item_id").cast("int"))
      .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
      .withColumn("price", F.col("price").cast("double"))
      .withColumn("freight_value", F.col("freight_value").cast("double")))

log("order_items", "duplicate (order_id, order_item_id) rows",
    it.count() - it.dropDuplicates(["order_id", "order_item_id"]).count(), "dropped duplicates")
it = it.dropDuplicates(["order_id", "order_item_id"])

log("order_items", "price <= 0 or null", it.filter(F.col("price").isNull() | (F.col("price") <= 0)).count(), "flag only")
log("order_items", "freight_value < 0 or null", it.filter(F.col("freight_value").isNull() | (F.col("freight_value") < 0)).count(), "flag only")
log("order_items", "order_id not found in orders", it.join(o, "order_id", "left_anti").count(), "flag only")
log("order_items", "seller_id not found in sellers", it.join(se, "seller_id", "left_anti").count(), "flag only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

p = (bronze("bronze_products")
     .withColumnRenamed("product_name_lenght", "product_name_length")
     .withColumnRenamed("product_description_lenght", "product_description_length"))
for c in ["product_name_length", "product_description_length", "product_photos_qty",
          "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
    p = p.withColumn(c, F.col(c).cast("double"))

log("products", "duplicate product_id rows", p.count() - p.dropDuplicates(["product_id"]).count(), "dropped duplicates")
p = p.dropDuplicates(["product_id"])
log("products", "missing product_category_name", p.filter(F.col("product_category_name").isNull()).count(),
    "category set to 'unknown'")

tr = bronze("bronze_category_translation").select("product_category_name", "product_category_name_english")
untranslated = (p.filter(F.col("product_category_name").isNotNull())
                  .join(tr, "product_category_name", "left")
                  .filter(F.col("product_category_name_english").isNull())
                  .select("product_category_name").distinct().count())
log("products", "categories without an English translation", untranslated, "fall back to Portuguese name")

p = (p.join(tr, "product_category_name", "left")
       .withColumn("category", F.coalesce("product_category_name_english", "product_category_name", F.lit("unknown")))
       .drop("product_category_name_english"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviews and payments

# COMMAND ----------

r = (bronze("bronze_order_reviews")
     .withColumn("review_score", F.col("review_score").cast("int"))
     .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
     .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp")))

log("reviews", "review_id appears more than once",
    r.count() - r.dropDuplicates(["review_id"]).count(), "resolved by keeping one review per order")
log("reviews", "review_score outside 1-5 or null",
    r.filter(F.col("review_score").isNull() | ~F.col("review_score").between(1, 5)).count(), "dropped")
r = r.filter(F.col("review_score").between(1, 5))

orders_with_many = r.groupBy("order_id").count().filter("count > 1").count()
log("reviews", "orders with more than one review", orders_with_many, "kept the latest review per order")
w = Window.partitionBy("order_id").orderBy(F.col("review_answer_timestamp").desc_nulls_last(),
                                           F.col("review_creation_date").desc_nulls_last())
r = r.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

pay = (bronze("bronze_order_payments")
       .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
       .withColumn("payment_installments", F.col("payment_installments").cast("int"))
       .withColumn("payment_value", F.col("payment_value").cast("double")))
log("payments", "exact duplicate rows", pay.count() - pay.dropDuplicates().count(), "dropped duplicates")
pay = pay.dropDuplicates()
log("payments", "payment_value <= 0", pay.filter(F.col("payment_value") <= 0).count(), "flag only")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geolocation (one row per zip prefix)

# COMMAND ----------

g = (bronze("bronze_geolocation")
     .withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
     .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double")))
log("geolocation", "exact duplicate rows", g.count() - g.dropDuplicates().count(), "collapsed by zip prefix")
geo = (g.groupBy("geolocation_zip_code_prefix")
         .agg(F.avg("geolocation_lat").alias("lat"),
              F.avg("geolocation_lng").alias("lng"),
              F.first("geolocation_city", ignorenulls=True).alias("city"),
              F.first("geolocation_state", ignorenulls=True).alias("state")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write silver tables and the data-quality log

# COMMAND ----------

save(o, "silver_orders")
save(cu, "silver_customers")
save(se, "silver_sellers")
save(it, "silver_order_items")
save(p, "silver_products")
save(r, "silver_order_reviews")
save(pay, "silver_order_payments")
save(geo, "silver_geolocation")

dq = spark.createDataFrame(dq_rows, ["table", "check", "row_count", "handling"])
save(dq, "silver_dq_log")
display(dq)
