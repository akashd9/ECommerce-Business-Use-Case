# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Gold: dimensions, facts, business aggregates
# MAGIC Reads Bronze/Silver tables published by the `01_dlt_pipeline` DLT pipeline (this
# MAGIC notebook only reads them — DLT owns writes to those schemas) and builds the
# MAGIC star schema + reporting marts, including SCD2 `dim_customer`/`dim_product` via
# MAGIC `MERGE INTO`.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "ecommerce_lakehouse"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.gold")

def read_bronze(name):
    return spark.table(f"{CATALOG}.bronze.{name}")

def read_silver(name):
    return spark.table(f"{CATALOG}.silver.{name}")

def save_gold(df, name):
    df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.gold.{name}")
    print(f"gold.{name}: {df.count()} rows")

def dedup_latest(df, key_cols):
    """store_locations_raw is an Auto Loader streaming table, so it can carry more
    than one row per store_id across repeated ingestion runs — keep the latest."""
    w = Window.partitionBy(*key_cols).orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

# gold.dim_date
date_df = spark.sql("""
    SELECT explode(sequence(to_date(date_sub(current_date(), 730)), current_date(), interval 1 day)) as date
""").withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int")) \
   .withColumn("day_of_week", F.date_format("date", "EEEE")) \
   .withColumn("month", F.month("date")) \
   .withColumn("quarter", F.quarter("date")) \
   .withColumn("year", F.year("date")) \
   .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))
save_gold(date_df, "dim_date")

# COMMAND ----------

# MAGIC %md ### SCD2 merge (see notebooks/medallion_ecommerce_notebook.py for full commentary)

# COMMAND ----------

def scd2_merge(target_table_full, updates_df, business_key, tracked_cols, sk_col):
    if not spark.catalog.tableExists(target_table_full):
        initial = (
            updates_df
            .withColumn(sk_col, F.expr("uuid()"))
            .withColumn("effective_from", F.current_date())
            .withColumn("effective_to", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
        )
        initial.write.format("delta").saveAsTable(target_table_full)
        print(f"{target_table_full}: initial load, {initial.count()} rows")
        return

    target = DeltaTable.forName(spark, target_table_full)
    current_cols = [business_key] + tracked_cols
    target_current = target.toDF().filter("is_current = true").select(*current_cols)
    for c in current_cols:
        target_current = target_current.withColumnRenamed(c, f"tgt_{c}")

    joined = updates_df.join(target_current, updates_df[business_key] == target_current[f"tgt_{business_key}"], "inner")
    diff_cond = None
    for c in tracked_cols:
        clause = joined[c] != joined[f"tgt_{c}"]
        diff_cond = clause if diff_cond is None else (diff_cond | clause)
    changed_rows = joined.filter(diff_cond).select(updates_df["*"])

    staged = (
        updates_df.withColumn("merge_key", F.col(business_key))
        .unionByName(changed_rows.withColumn("merge_key", F.lit(None).cast("string")))
    )

    when_matched_cond = "t.is_current = true AND (" + " OR ".join(f"t.{c} <> s.{c}" for c in tracked_cols) + ")"
    insert_values = {sk_col: "uuid()", business_key: f"s.{business_key}"}
    insert_values.update({c: f"s.{c}" for c in tracked_cols})
    insert_values.update({"effective_from": "current_date()", "effective_to": "CAST(NULL AS DATE)", "is_current": "true"})

    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.{business_key} = s.merge_key")
        .whenMatchedUpdate(condition=when_matched_cond, set={"is_current": "false", "effective_to": "current_date()"})
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    print(f"{target_table_full}: merged {updates_df.count()} source rows ({changed_rows.count()} changed)")

# COMMAND ----------

customer_attrs = read_silver("customers").select("customer_id", "email", "source_system")
scd2_merge(f"{CATALOG}.gold.dim_customer", customer_attrs, "customer_id", ["email", "source_system"], "customer_sk")
dim_customer = spark.table(f"{CATALOG}.gold.dim_customer").filter("is_current = true")

product_attrs = read_silver("products").select("product_id", "category", "subcategory", "unit_price")
scd2_merge(f"{CATALOG}.gold.dim_product", product_attrs, "product_id", ["category", "subcategory", "unit_price"], "product_sk")
dim_product = spark.table(f"{CATALOG}.gold.dim_product").filter("is_current = true")

# COMMAND ----------

# gold.dim_store
dim_store = (
    dedup_latest(read_bronze("store_locations_raw"), ["store_id"])
    .withColumn("store_sk", F.monotonically_increasing_id())
    .withColumnRenamed("name_raw", "store_name")
    .withColumnRenamed("region_raw", "region")
)
save_gold(dim_store, "dim_store")

# COMMAND ----------

# gold.fact_sales
fact_sales = (
    read_silver("order_items")
    .join(read_silver("orders"), "order_id")
    .join(dim_customer.select("customer_id", "customer_sk"), "customer_id")
    .join(dim_product.select("product_id", "product_sk"), "product_id")
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .withColumn("revenue", F.col("qty") * F.col("unit_price"))
    .select("order_id", "line_id", "customer_sk", "product_sk", "date_key", "qty", "unit_price", "revenue", "order_status")
)
save_gold(fact_sales, "fact_sales")

# COMMAND ----------

# gold.fact_inventory_daily
fact_inventory = (
    read_silver("inventory_snapshot")
    .withColumn("date_key", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
    .withColumn("days_of_supply", F.round(F.col("qty_on_hand") / F.lit(15), 1))
)
save_gold(fact_inventory, "fact_inventory_daily")

# COMMAND ----------

# gold.fact_marketing_performance
email_agg = (
    read_silver("email_events")
    .withColumn("event_date", F.to_date("event_ts"))
    .groupBy("campaign_id", "event_date")
    .pivot("event_type", ["sent", "open", "click"])
    .count()
    .na.fill(0)
)
ad_spend_daily_df = read_silver("ad_spend_daily")
fact_marketing = (
    ad_spend_daily_df
    .join(email_agg, (ad_spend_daily_df.campaign_id == email_agg.campaign_id) & (ad_spend_daily_df.spend_date == email_agg.event_date), "left")
    .select(
        ad_spend_daily_df.campaign_id, "platform", "spend_date", "spend",
        F.coalesce("sent", F.lit(0)).alias("impressions"),
        F.coalesce("click", F.lit(0)).alias("clicks"),
    )
)
save_gold(fact_marketing, "fact_marketing_performance")

# COMMAND ----------

# gold.customer_360 — now enriched with vendor demographics
orders_agg = read_silver("orders").groupBy("customer_id").agg(F.count("*").alias("total_orders"), F.max("order_date").alias("last_order_date"))
revenue_agg = fact_sales.join(dim_customer.select("customer_sk", "customer_id"), "customer_sk").groupBy("customer_id").agg(F.sum("revenue").alias("total_revenue"))
reviews_agg = read_silver("reviews").groupBy("customer_id").agg(F.avg("rating").alias("avg_rating_given"))
tickets_agg = read_silver("support_tickets").groupBy("customer_id").agg(F.count("*").alias("support_tickets_count"))
demographics = read_silver("customer_demographics")

customer_360 = (
    read_silver("customers").select("customer_id")
    .join(orders_agg, "customer_id", "left")
    .join(revenue_agg, "customer_id", "left")
    .join(reviews_agg, "customer_id", "left")
    .join(tickets_agg, "customer_id", "left")
    .join(demographics, "customer_id", "left")
    .na.fill({"total_orders": 0, "total_revenue": 0.0, "support_tickets_count": 0})
)
save_gold(customer_360, "customer_360")

# COMMAND ----------

# gold.customer_ltv — heuristic LTV, weighted by vendor affluence signal where available
customer_ltv = (
    read_silver("customers").select("customer_id", "signup_date")
    .join(customer_360, "customer_id")
    .withColumn("tenure_days", F.datediff(F.current_date(), F.col("signup_date")))
    .withColumn("affluence_multiplier", F.coalesce(F.col("region_affluence_index"), F.lit(1.0)))
    .withColumn("predicted_ltv", F.round(F.col("total_revenue") * 1.8 * F.col("affluence_multiplier"), 2))
    .withColumn(
        "churn_risk_score",
        F.when(F.col("last_order_date") < F.date_sub(F.current_date(), 120), 0.8)
         .when(F.col("last_order_date") < F.date_sub(F.current_date(), 60), 0.5)
         .otherwise(0.2),
    )
    .select("customer_id", "predicted_ltv", "churn_risk_score", "tenure_days")
)
save_gold(customer_ltv, "customer_ltv")

# COMMAND ----------

# gold.daily_sales_summary
pos_s = (
    read_bronze("pos_transactions_raw")
    .withColumn("amount", F.col("amount_raw").cast("double"))
    .withColumn("txn_date", F.to_date("txn_ts"))
)
daily_sales_summary = (
    pos_s.groupBy("store_id", "txn_date")
    .agg(F.sum("amount").alias("total_revenue"), F.count("*").alias("total_orders"))
    .withColumn("avg_basket_size", F.round(F.col("total_revenue") / F.col("total_orders"), 2))
    .withColumnRenamed("txn_date", "sale_date")
)
save_gold(daily_sales_summary, "daily_sales_summary")

# COMMAND ----------

# gold.product_performance
sales_monthly = (
    fact_sales.join(dim_product.select("product_sk", "product_id"), "product_sk")
    .join(read_silver("orders").select("order_id", "order_date"), "order_id")
    .withColumn("month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("product_id", "month")
    .agg(F.sum("qty").alias("units_sold"), F.sum("revenue").alias("revenue"))
)
refunds_monthly = (
    read_silver("refunds")
    .join(read_silver("order_items").select("order_id", "product_id"), "order_id")
    .withColumn("month", F.date_format("refund_date", "yyyy-MM"))
    .groupBy("product_id", "month")
    .agg(F.count("*").alias("refund_count"))
)
product_performance = (
    sales_monthly.join(refunds_monthly, ["product_id", "month"], "left")
    .na.fill({"refund_count": 0})
    .withColumn("return_rate", F.round(F.col("refund_count") / F.col("units_sold"), 3))
)
save_gold(product_performance, "product_performance")

# COMMAND ----------

# gold.campaign_roi
spend_total = read_silver("ad_spend_daily").groupBy("campaign_id").agg(F.sum("spend").alias("total_spend"))
engaged_customers = read_silver("email_events").filter("event_type = 'click'").select("campaign_id", "customer_id").distinct()
attributed_revenue = (
    engaged_customers
    .join(fact_sales.join(dim_customer.select("customer_sk", "customer_id"), "customer_sk"), "customer_id")
    .groupBy("campaign_id")
    .agg(F.sum("revenue").alias("attributed_revenue"))
)
campaign_roi = (
    spend_total.join(attributed_revenue, "campaign_id", "left")
    .na.fill({"attributed_revenue": 0.0})
    .withColumn("roi_pct", F.round((F.col("attributed_revenue") - F.col("total_spend")) / F.col("total_spend") * 100, 1))
)
save_gold(campaign_roi, "campaign_roi")

# COMMAND ----------

for schema in ("bronze", "silver", "gold"):
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    print(f"{schema}: {len(tables)} tables")
