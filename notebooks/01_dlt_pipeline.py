# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Delta Live Tables: Bronze -> Silver
# MAGIC Real DLT pipeline (not a plain notebook). Bronze tables are Auto Loader streaming
# MAGIC tables reading the files landed by `00_generate_source_data`; the three transactional
# MAGIC tables (orders/payments/refunds) are built with `dlt.apply_changes` against
# MAGIC Debezium-style CDC envelopes, matching how a real Postgres-via-Debezium feed would be
# MAGIC applied. Silver tables are materialized views with `@dlt.expect*` data quality rules.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "ecommerce_lakehouse"
VOLUME_ROOT = f"/Volumes/{CATALOG}/landing/raw_files"

def bronze_name(t):
    return f"{CATALOG}.bronze.{t}"

def silver_name(t):
    return f"{CATALOG}.silver.{t}"

def dedup_latest(df, key_cols):
    """Bronze master-data tables are Auto Loader streaming tables, so re-running the
    generator appends a fresh snapshot rather than replacing it. Keep only the most
    recently ingested row per business key before anything downstream (joins, SCD2
    merges) assumes one row per key."""
    w = Window.partitionBy(*key_cols).orderBy(F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

# MAGIC %md ## Bronze — Auto Loader ingestion (file-based sources)
# MAGIC One factory function generates a streaming table per source folder — this is the
# MAGIC same Auto Loader mechanics whether the origin is a daily REST API pull (product
# MAGIC catalog), an Ads API export (marketing), a vendor S3 drop (demographics), or a
# MAGIC Kafka/Event Hubs topic dump (clickstream); only the schema and cadence differ.

# COMMAND ----------

BRONZE_FILE_SOURCES = {
    "clickstream_events_raw": ("clickstream", "json"),
    "products_raw": ("product_catalog", "json"),
    "marketing_campaigns_raw": ("marketing/campaigns", "json"),
    "ad_spend_raw": ("marketing/ad_spend", "json"),
    "email_events_raw": ("marketing/email_events", "json"),
    "customer_demographics_raw": ("vendor/customer_demographics", "csv"),
    "customers_raw": ("internal/customers", "json"),
    "warehouses_raw": ("internal/warehouses", "json"),
    "suppliers_raw": ("internal/suppliers", "json"),
    "store_locations_raw": ("internal/store_locations", "json"),
    "order_items_raw": ("internal/order_items", "json"),
    "inventory_raw": ("internal/inventory", "json"),
    "shipments_raw": ("internal/shipments", "json"),
    "cart_events_raw": ("internal/cart_events", "json"),
    "reviews_raw": ("internal/reviews", "json"),
    "promotions_raw": ("internal/promotions", "json"),
    "support_tickets_raw": ("internal/support_tickets", "json"),
    "pos_transactions_raw": ("internal/pos_transactions", "json"),
}

def make_autoloader_bronze(table_name, folder, fmt):
    @dlt.table(
        name=bronze_name(table_name),
        comment=f"Bronze: raw {table_name}, ingested via Auto Loader from {folder}",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        reader = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", fmt)
            .option("cloudFiles.schemaLocation", f"{VOLUME_ROOT}/_schemas/{table_name}")
            .option("cloudFiles.inferColumnTypes", "true")
        )
        if fmt == "csv":
            reader = reader.option("header", "true")
        return reader.load(f"{VOLUME_ROOT}/{folder}").withColumn("_ingest_ts", F.current_timestamp())
    return _bronze

for _table_name, (_folder, _fmt) in BRONZE_FILE_SOURCES.items():
    make_autoloader_bronze(_table_name, _folder, _fmt)

# COMMAND ----------

# MAGIC %md ## Bronze — CDC (orders, payments, refunds via `dlt.apply_changes`)
# MAGIC Simulates applying a Debezium change-data-capture stream from Postgres: each file
# MAGIC contains `{op, before, after, ts_ms}` envelopes; `apply_changes` upserts/deletes
# MAGIC against the target using `ts_ms` to sequence out-of-order events, the same call
# MAGIC you'd make against a real Debezium topic.

# COMMAND ----------

from pyspark.sql.types import LongType, StringType, StructField, StructType

# Auto Loader's automatic inference mis-types a nested object as an array when the
# sample it draws is small/uniform (as our CDC envelopes are) — pinning an explicit
# schema for the Debezium-style envelope sidesteps that and matches how a real CDC
# pipeline would pin the upstream Postgres table's schema anyway.
CDC_AFTER_SCHEMAS = {
    "orders_raw": StructType([
        StructField("order_id", StringType()), StructField("customer_id", StringType()),
        StructField("order_ts", StringType()), StructField("status_raw", StringType()),
        StructField("__table", StringType()),
    ]),
    "payments_raw": StructType([
        StructField("payment_id", StringType()), StructField("order_id", StringType()),
        StructField("amount_raw", StringType()), StructField("method_raw", StringType()),
        StructField("payment_ts", StringType()), StructField("__table", StringType()),
    ]),
    "refunds_raw": StructType([
        StructField("refund_id", StringType()), StructField("order_id", StringType()),
        StructField("amount_raw", StringType()), StructField("reason_raw", StringType()),
        StructField("refund_ts", StringType()), StructField("__table", StringType()),
    ]),
}

CDC_SOURCES = {
    "orders_raw": ("cdc/orders", "order_id"),
    "payments_raw": ("cdc/payments", "payment_id"),
    "refunds_raw": ("cdc/refunds", "refund_id"),
}

def make_cdc_bronze(table_name, folder, key_col):
    staging_name = f"{table_name}_cdc_staging"
    envelope_schema = StructType([
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
        StructField("after", CDC_AFTER_SCHEMAS[table_name]),
    ])

    @dlt.table(name=bronze_name(staging_name), temporary=True)
    def _staging():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(envelope_schema)
            .load(f"{VOLUME_ROOT}/{folder}")
            .select("op", "ts_ms", "after.*")
        )

    dlt.create_streaming_table(bronze_name(table_name), comment=f"Bronze: {table_name} applied from CDC envelopes", table_properties={"quality": "bronze"})
    dlt.apply_changes(
        target=bronze_name(table_name),
        source=bronze_name(staging_name),
        keys=[key_col],
        sequence_by="ts_ms",
        apply_as_deletes="op = 'd'",
        except_column_list=["op", "ts_ms", "__table"],
    )

for _table_name, (_folder, _key) in CDC_SOURCES.items():
    make_cdc_bronze(_table_name, _folder, _key)

# COMMAND ----------

# MAGIC %md ## Silver — cleaned, deduped, conformed, with data quality expectations

# COMMAND ----------

@dlt.table(name=silver_name("customers"), comment="Deduped, standardized customers")
@dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+[.][^@]+$'")
@dlt.expect_or_fail("has_customer_id", "customer_id IS NOT NULL")
def silver_customers():
    return (
        dedup_latest(dlt.read(bronze_name("customers_raw")), ["customer_id"])
        .withColumn("email", F.trim(F.lower("email")))
        .withColumn("signup_date", F.to_date("signup_ts"))
        .select("customer_id", "email", "source_system", "signup_date")
    )

# COMMAND ----------

@dlt.table(name=silver_name("products"), comment="Cleaned product catalog with category hierarchy")
@dlt.expect_or_drop("positive_price", "unit_price > 0")
def silver_products():
    df = (
        dedup_latest(dlt.read(bronze_name("products_raw")), ["product_id"])
        .withColumn("category_raw", F.trim("category_raw"))
        .withColumn("category", F.split("category_raw", "/").getItem(0))
        .withColumn("subcategory", F.split("category_raw", "/").getItem(1))
        .withColumn("unit_price", F.col("price_raw").cast("double"))
    )
    return df.select("product_id", "sku", "category", "subcategory", "unit_price")

# COMMAND ----------

@dlt.table(name=silver_name("warehouses"))
def silver_warehouses():
    return (
        dedup_latest(dlt.read(bronze_name("warehouses_raw")), ["warehouse_id"])
        .withColumnRenamed("name_raw", "name").withColumnRenamed("region_raw", "region")
    )

@dlt.table(name=silver_name("suppliers"))
def silver_suppliers():
    return (
        dedup_latest(dlt.read(bronze_name("suppliers_raw")), ["supplier_id"])
        .withColumnRenamed("name_raw", "name").withColumnRenamed("contact_raw", "contact_email")
    )

# COMMAND ----------

@dlt.table(name=silver_name("orders"), comment="Orders with normalized status")
@dlt.expect_or_fail("has_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("has_customer", "customer_id IS NOT NULL")
def silver_orders():
    status_map = {"placed": "PLACED", "shipped": "SHIPPED", "delivered": "DELIVERED", "cancelled": "CANCELLED"}
    return (
        dlt.read(bronze_name("orders_raw"))
        .withColumn("status_clean", F.trim(F.lower("status_raw")))
        .replace(status_map, subset=["status_clean"])
        .withColumnRenamed("status_clean", "order_status")
        .withColumn("order_date", F.to_date("order_ts"))
        .select("order_id", "customer_id", "order_date", "order_status")
    )

# COMMAND ----------

@dlt.table(name=silver_name("order_items"), comment="Line items joined to product")
@dlt.expect_or_drop("positive_qty", "qty > 0")
def silver_order_items():
    return (
        dlt.read(bronze_name("order_items_raw"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("unit_price", F.col("unit_price_raw").cast("double"))
        .join(dlt.read(silver_name("products")).select("product_id"), "product_id", "inner")
        .select("order_id", "line_id", "product_id", "qty", "unit_price")
    )

# COMMAND ----------

@dlt.table(name=silver_name("payments"), comment="Payments reconciled against orders")
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_payments():
    return (
        dlt.read(bronze_name("payments_raw"))
        .withColumn("amount", F.col("amount_raw").cast("double"))
        .withColumn("payment_date", F.to_date("payment_ts"))
        .join(dlt.read(silver_name("orders")).select("order_id"), "order_id", "inner")
        .select("payment_id", "order_id", "amount", "method_raw", "payment_date")
        .withColumnRenamed("method_raw", "method")
    )

# COMMAND ----------

@dlt.table(name=silver_name("refunds"))
def silver_refunds():
    return (
        dlt.read(bronze_name("refunds_raw"))
        .withColumn("amount", F.col("amount_raw").cast("double"))
        .withColumn("refund_date", F.to_date("refund_ts"))
        .select("refund_id", "order_id", "amount", "reason_raw", "refund_date")
        .withColumnRenamed("reason_raw", "reason")
    )

# COMMAND ----------

@dlt.table(name=silver_name("inventory_snapshot"))
@dlt.expect_or_drop("non_negative_stock", "qty_on_hand >= 0")
def silver_inventory_snapshot():
    return (
        dlt.read(bronze_name("inventory_raw"))
        .withColumn("snapshot_date", F.to_date("snapshot_ts"))
        .withColumn("qty_on_hand", F.col("qty_on_hand_raw").cast("int"))
        .groupBy("sku", "warehouse_id", "snapshot_date")
        .agg(F.last("qty_on_hand").alias("qty_on_hand"))
    )

# COMMAND ----------

@dlt.table(name=silver_name("shipments"))
def silver_shipments():
    return (
        dlt.read(bronze_name("shipments_raw"))
        .join(dlt.read(silver_name("orders")).select("order_id"), "order_id", "inner")
        .withColumnRenamed("carrier_raw", "carrier")
        .withColumnRenamed("status_raw", "status")
    )

# COMMAND ----------

@dlt.table(name=silver_name("clickstream_sessions"), comment="Sessionized via 30-min inactivity window")
def silver_clickstream_sessions():
    click_raw = dlt.read(bronze_name("clickstream_events_raw")).withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    w_cust = Window.partitionBy("customer_id").orderBy("event_ts")
    return (
        click_raw
        .withColumn("prev_ts", F.lag("event_ts").over(w_cust))
        .withColumn("gap_min", (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 60)
        .withColumn("new_session", F.when((F.col("gap_min").isNull()) | (F.col("gap_min") > 30), 1).otherwise(0))
        .withColumn("session_seq", F.sum("new_session").over(w_cust.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("session_id", F.concat_ws("-", "customer_id", "session_seq"))
        .groupBy("session_id", "customer_id")
        .agg(F.min("event_ts").alias("start_ts"), F.max("event_ts").alias("end_ts"), F.count("*").alias("page_count"))
    )

# COMMAND ----------

@dlt.table(name=silver_name("cart_events"))
def silver_cart_events():
    return (
        dlt.read(bronze_name("cart_events_raw"))
        .join(dlt.read(silver_name("customers")).select("customer_id"), "customer_id", "inner")
        .join(dlt.read(silver_name("products")).select("product_id"), "product_id", "inner")
    )

# COMMAND ----------

@dlt.table(name=silver_name("reviews"))
@dlt.expect_or_drop("valid_rating", "rating BETWEEN 1 AND 5")
def silver_reviews():
    return (
        dlt.read(bronze_name("reviews_raw"))
        .withColumnRenamed("rating_raw", "rating")
        .withColumn("sentiment", F.when(F.col("rating") >= 4, "positive").when(F.col("rating") == 3, "neutral").otherwise("negative"))
        .select("review_id", "product_id", "customer_id", "rating", "sentiment", "review_ts")
    )

# COMMAND ----------

@dlt.table(name=silver_name("marketing_campaigns"))
def silver_marketing_campaigns():
    return (
        dedup_latest(dlt.read(bronze_name("marketing_campaigns_raw")), ["campaign_id"])
        .withColumnRenamed("name_raw", "name").withColumnRenamed("channel_raw", "channel")
    )

@dlt.table(name=silver_name("ad_spend_daily"))
@dlt.expect_or_drop("non_negative_spend", "spend >= 0")
def silver_ad_spend_daily():
    return (
        dlt.read(bronze_name("ad_spend_raw"))
        .withColumn("spend", F.col("spend_raw").cast("double"))
        .groupBy("campaign_id", "platform", "spend_date")
        .agg(F.sum("spend").alias("spend"))
    )

@dlt.table(name=silver_name("email_events"))
def silver_email_events():
    return dlt.read(bronze_name("email_events_raw")).join(dlt.read(silver_name("customers")).select("customer_id"), "customer_id", "inner")

# COMMAND ----------

@dlt.table(name=silver_name("promotions"))
def silver_promotions():
    return (
        dedup_latest(dlt.read(bronze_name("promotions_raw")), ["promo_id"])
        .withColumn("discount_pct", F.regexp_replace("discount_raw", "%", "").cast("int"))
        .select("promo_id", "code_raw", "discount_pct", "valid_from", "valid_to")
        .withColumnRenamed("code_raw", "code")
    )

@dlt.table(name=silver_name("support_tickets"))
def silver_support_tickets():
    return (
        dlt.read(bronze_name("support_tickets_raw"))
        .join(dlt.read(silver_name("customers")).select("customer_id"), "customer_id", "inner")
        .withColumnRenamed("category_raw", "category")
    )

# COMMAND ----------

# MAGIC %md ## Silver — customer demographics (third-party vendor enrichment)

# COMMAND ----------

@dlt.table(name=silver_name("customer_demographics"), comment="Vendor-enriched customer demographics")
@dlt.expect_or_drop("plausible_age", "age BETWEEN 13 AND 110")
def silver_customer_demographics():
    return (
        dedup_latest(dlt.read(bronze_name("customer_demographics_raw")), ["customer_id"])
        .join(dlt.read(silver_name("customers")).select("customer_id"), "customer_id", "inner")
        .select("customer_id", "age", "income_bracket", "household_size", "region_affluence_index")
    )
