# Databricks notebook source
# MAGIC %md
# MAGIC # E-Commerce Lakehouse — 50-Table Medallion Build
# MAGIC Generates synthetic retail data with Faker and materializes 20 Bronze, 18 Silver,
# MAGIC and 12 Gold Delta tables under `ecommerce_lakehouse.{bronze,silver,gold}`.
# MAGIC
# MAGIC Run top to bottom. Sized for interactive/Community Edition use (low-thousands of rows per table).

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable

fake = Faker()
random.seed(42)
Faker.seed(42)

CATALOG = "ecommerce_lakehouse"
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"USE CATALOG {CATALOG}")

def save_bronze(pdf: pd.DataFrame, name: str):
    df = spark.createDataFrame(pdf)
    df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.bronze.{name}")
    print(f"bronze.{name}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md ## Config — volumes (small/local practice scale)

# COMMAND ----------

N_CUSTOMERS = 500
N_PRODUCTS = 200
N_WAREHOUSES = 5
N_SUPPLIERS = 30
N_STORES = 15
N_ORDERS = 3000
N_CAMPAIGNS = 25
N_PROMOTIONS = 40
INVENTORY_DAYS = 14
AD_SPEND_DAYS = 30

customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, N_CUSTOMERS + 1)]
product_ids = [f"P{str(i).zfill(5)}" for i in range(1, N_PRODUCTS + 1)]
warehouse_ids = [f"W{str(i).zfill(2)}" for i in range(1, N_WAREHOUSES + 1)]
supplier_ids = [f"SUP{str(i).zfill(4)}" for i in range(1, N_SUPPLIERS + 1)]
store_ids = [f"ST{str(i).zfill(3)}" for i in range(1, N_STORES + 1)]
order_ids = [f"O{str(i).zfill(6)}" for i in range(1, N_ORDERS + 1)]
campaign_ids = [f"CMP{str(i).zfill(4)}" for i in range(1, N_CAMPAIGNS + 1)]
promo_ids = [f"PROMO{str(i).zfill(4)}" for i in range(1, N_PROMOTIONS + 1)]

CATEGORIES = {
    "Electronics": ["Headphones", "Chargers", "Cameras", "Smart Home"],
    "Apparel": ["Men", "Women", "Kids", "Footwear"],
    "Home": ["Kitchen", "Furniture", "Decor", "Bedding"],
    "Beauty": ["Skincare", "Haircare", "Fragrance"],
    "Sporting Goods": ["Fitness", "Outdoor", "Team Sports"],
}
CHANNELS = ["Paid Search", "Paid Social", "Email", "Affiliate", "Display"]
PLATFORMS = ["Google Ads", "Meta Ads", "TikTok Ads"]
CARRIERS = ["UPS", "FedEx", "USPS", "DHL"]
ORDER_STATUSES_RAW = ["placed", "PLACED", "shipped", "Shipped", "delivered", "cancelled", "Cancelled "]

def now_minus(days_back_max):
    return datetime.now() - timedelta(days=random.uniform(0, days_back_max), hours=random.uniform(0, 24))

# COMMAND ----------

# MAGIC %md ## Bronze — raw synthetic ingestion (20 tables)

# COMMAND ----------

# 1. customers_raw
rows = [{
    "customer_id": cid, "email": fake.email(), "signup_ts": now_minus(720),
    "source_system": random.choice(["web", "mobile_app", "store_kiosk"]),
    "_ingest_ts": datetime.now(),
} for cid in customer_ids]
save_bronze(pd.DataFrame(rows), "customers_raw")

# 2. products_raw
product_category_map = {}
rows = []
for pid in product_ids:
    cat = random.choice(list(CATEGORIES.keys()))
    sub = random.choice(CATEGORIES[cat])
    product_category_map[pid] = (cat, sub)
    rows.append({
        "product_id": pid, "sku": f"SKU-{pid}", "category_raw": f" {cat}/{sub} ",
        "price_raw": str(round(random.uniform(5, 400), 2)), "_ingest_ts": datetime.now(),
    })
save_bronze(pd.DataFrame(rows), "products_raw")

# 3. warehouses_raw
rows = [{"warehouse_id": w, "name_raw": f"{fake.city()} DC", "region_raw": random.choice(["US-EAST", "US-WEST", "US-CENTRAL", "EU"])} for w in warehouse_ids]
save_bronze(pd.DataFrame(rows), "warehouses_raw")

# 4. suppliers_raw
rows = [{"supplier_id": s, "name_raw": fake.company(), "contact_raw": fake.company_email()} for s in supplier_ids]
save_bronze(pd.DataFrame(rows), "suppliers_raw")

# 5. store_locations_raw
rows = [{"store_id": s, "name_raw": f"{fake.city()} Store", "address_raw": fake.address().replace("\n", ", "), "region_raw": random.choice(["US-EAST", "US-WEST", "US-CENTRAL"])} for s in store_ids]
save_bronze(pd.DataFrame(rows), "store_locations_raw")

# 6. orders_raw (+ track per-order customer/date for downstream tables)
order_customer = {}
order_dt = {}
rows = []
for oid in order_ids:
    cust = random.choice(customer_ids)
    dt = now_minus(365)
    order_customer[oid] = cust
    order_dt[oid] = dt
    rows.append({
        "order_id": oid, "customer_id": cust, "order_ts": dt,
        "status_raw": random.choice(ORDER_STATUSES_RAW), "_ingest_ts": datetime.now(),
    })
save_bronze(pd.DataFrame(rows), "orders_raw")

# 7. order_items_raw
rows = []
line = 1
order_item_products = {}
for oid in order_ids:
    n_items = random.randint(1, 4)
    chosen = random.sample(product_ids, n_items)
    order_item_products[oid] = chosen
    for i, pid in enumerate(chosen, start=1):
        rows.append({
            "order_id": oid, "line_id": i, "product_id": pid,
            "qty": random.randint(1, 3), "unit_price_raw": str(round(random.uniform(5, 400), 2)),
        })
save_bronze(pd.DataFrame(rows), "order_items_raw")

# 8. payments_raw
rows = [{
    "payment_id": f"PAY{str(i).zfill(6)}", "order_id": oid,
    "amount_raw": str(round(random.uniform(10, 900), 2)),
    "method_raw": random.choice(["credit_card", "paypal", "gift_card", "apple_pay"]),
    "payment_ts": order_dt[oid] + timedelta(minutes=random.randint(1, 30)),
} for i, oid in enumerate(order_ids, start=1)]
save_bronze(pd.DataFrame(rows), "payments_raw")

# 9. refunds_raw (~5% of orders)
refund_orders = random.sample(order_ids, int(N_ORDERS * 0.05))
rows = [{
    "refund_id": f"REF{str(i).zfill(5)}", "order_id": oid,
    "amount_raw": str(round(random.uniform(5, 300), 2)),
    "reason_raw": random.choice(["damaged", "wrong_item", "changed_mind", "late_delivery"]),
    "refund_ts": order_dt[oid] + timedelta(days=random.randint(2, 20)),
} for i, oid in enumerate(refund_orders, start=1)]
save_bronze(pd.DataFrame(rows), "refunds_raw")

# 10. inventory_raw
rows = []
for pid in product_ids:
    for wid in warehouse_ids:
        for d in range(INVENTORY_DAYS):
            rows.append({
                "sku": f"SKU-{pid}", "warehouse_id": wid,
                "qty_on_hand_raw": random.randint(0, 500),
                "snapshot_ts": datetime.now() - timedelta(days=d),
            })
save_bronze(pd.DataFrame(rows), "inventory_raw")

# 11. shipments_raw (for delivered/shipped orders)
shippable = [o for o in order_ids if order_ids.index(o) % 1 == 0][: int(N_ORDERS * 0.9)]
rows = [{
    "shipment_id": f"SHP{str(i).zfill(6)}", "order_id": oid,
    "carrier_raw": random.choice(CARRIERS), "tracking_no": uuid.uuid4().hex[:12].upper(),
    "status_raw": random.choice(["in_transit", "delivered", "delayed"]),
} for i, oid in enumerate(shippable, start=1)]
save_bronze(pd.DataFrame(rows), "shipments_raw")

# 12. clickstream_events_raw
rows = []
for i in range(20000):
    cust = random.choice(customer_ids)
    rows.append({
        "event_id": f"EVT{str(i).zfill(7)}", "session_id_raw": f"{cust}-{random.randint(1, 40)}",
        "customer_id": cust, "event_type": random.choice(["page_view", "product_view", "search", "checkout_start"]),
        "url": f"/products/{random.choice(product_ids)}", "event_ts": now_minus(90),
    })
save_bronze(pd.DataFrame(rows), "clickstream_events_raw")

# 13. cart_events_raw
rows = [{
    "event_id": f"CART{str(i).zfill(6)}", "customer_id": random.choice(customer_ids),
    "product_id": random.choice(product_ids), "action": random.choice(["add", "remove"]),
    "event_ts": now_minus(90),
} for i in range(8000)]
save_bronze(pd.DataFrame(rows), "cart_events_raw")

# 14. reviews_raw
rows = [{
    "review_id": f"RVW{str(i).zfill(5)}", "product_id": random.choice(product_ids),
    "customer_id": random.choice(customer_ids), "rating_raw": random.randint(1, 5),
    "text_raw": fake.sentence(nb_words=12), "review_ts": now_minus(365),
} for i in range(1500)]
save_bronze(pd.DataFrame(rows), "reviews_raw")

# 15. marketing_campaigns_raw
rows = []
for cid in campaign_ids:
    start = now_minus(180)
    rows.append({
        "campaign_id": cid, "name_raw": f"{fake.bs().title()} Campaign",
        "channel_raw": random.choice(CHANNELS), "start_date": start.date(),
        "end_date": (start + timedelta(days=random.randint(7, 45))).date(),
    })
save_bronze(pd.DataFrame(rows), "marketing_campaigns_raw")

# 16. ad_spend_raw
rows = []
i = 0
for cid in campaign_ids:
    for platform in PLATFORMS:
        for d in range(AD_SPEND_DAYS):
            i += 1
            rows.append({
                "spend_id": f"SPD{str(i).zfill(6)}", "campaign_id": cid, "platform": platform,
                "spend_raw": str(round(random.uniform(20, 800), 2)),
                "spend_date": (datetime.now() - timedelta(days=d)).date(),
            })
save_bronze(pd.DataFrame(rows), "ad_spend_raw")

# 17. email_events_raw
rows = [{
    "event_id": f"EML{str(i).zfill(6)}", "campaign_id": random.choice(campaign_ids),
    "customer_id": random.choice(customer_ids), "event_type": random.choice(["sent", "open", "click", "unsubscribe"]),
    "event_ts": now_minus(180),
} for i in range(6000)]
save_bronze(pd.DataFrame(rows), "email_events_raw")

# 18. promotions_raw
rows = []
for pid in promo_ids:
    start = now_minus(180)
    rows.append({
        "promo_id": pid, "code_raw": fake.lexify(text="????-####").upper(),
        "discount_raw": f"{random.choice([10, 15, 20, 25, 30])}%",
        "valid_from": start.date(), "valid_to": (start + timedelta(days=random.randint(7, 60))).date(),
    })
save_bronze(pd.DataFrame(rows), "promotions_raw")

# 19. support_tickets_raw
rows = [{
    "ticket_id": f"TIX{str(i).zfill(5)}", "customer_id": random.choice(customer_ids),
    "order_id": random.choice(order_ids), "category_raw": random.choice(["shipping", "billing", "product_defect", "general"]),
    "opened_ts": now_minus(180),
} for i in range(600)]
save_bronze(pd.DataFrame(rows), "support_tickets_raw")

# 20. pos_transactions_raw
rows = [{
    "txn_id": f"POS{str(i).zfill(6)}", "store_id": random.choice(store_ids),
    "sku": f"SKU-{random.choice(product_ids)}", "qty": random.randint(1, 3),
    "amount_raw": str(round(random.uniform(5, 400), 2)), "txn_ts": now_minus(90),
} for i in range(4000)]
save_bronze(pd.DataFrame(rows), "pos_transactions_raw")

# COMMAND ----------

# MAGIC %md ## Silver — clean, dedupe, conform (18 tables)

# COMMAND ----------

def read_bronze(name):
    return spark.table(f"{CATALOG}.bronze.{name}")

def save_silver(df, name):
    df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.silver.{name}")
    print(f"silver.{name}: {df.count()} rows")

# silver.customers — dedupe on latest ingest, trim email, extract region-free full name
w_latest = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())
customers_s = (
    read_bronze("customers_raw")
    .withColumn("rn", F.row_number().over(w_latest))
    .filter("rn = 1").drop("rn")
    .withColumn("email", F.trim(F.lower("email")))
    .withColumnRenamed("signup_ts", "signup_date")
    .withColumn("signup_date", F.to_date("signup_date"))
    .select("customer_id", "email", "source_system", "signup_date")
)
save_silver(customers_s, "customers")

# silver.products — split category_raw "Cat/Sub" into columns, cast price
products_s = (
    read_bronze("products_raw")
    .withColumn("category_raw", F.trim("category_raw"))
    .withColumn("category", F.split("category_raw", "/").getItem(0))
    .withColumn("subcategory", F.split("category_raw", "/").getItem(1))
    .withColumn("unit_price", F.col("price_raw").cast("double"))
    .select("product_id", "sku", "category", "subcategory", "unit_price")
)
save_silver(products_s, "products")

# silver.warehouses / suppliers — light standardization
save_silver(read_bronze("warehouses_raw").withColumnRenamed("name_raw", "name").withColumnRenamed("region_raw", "region"), "warehouses")
save_silver(read_bronze("suppliers_raw").withColumnRenamed("name_raw", "name").withColumnRenamed("contact_raw", "contact_email"), "suppliers")

# silver.orders — normalize status enum, derive order_date
status_map = {"placed": "PLACED", "shipped": "SHIPPED", "delivered": "DELIVERED", "cancelled": "CANCELLED"}
orders_s = (
    read_bronze("orders_raw")
    .withColumn("status_clean", F.trim(F.lower("status_raw")))
    .replace(status_map, subset=["status_clean"])
    .withColumnRenamed("status_clean", "order_status")
    .withColumn("order_date", F.to_date("order_ts"))
    .select("order_id", "customer_id", "order_date", "order_status")
)
save_silver(orders_s, "orders")

# silver.order_items — cast qty/price, join to product for validation
order_items_s = (
    read_bronze("order_items_raw")
    .withColumn("qty", F.col("qty").cast("int"))
    .withColumn("unit_price", F.col("unit_price_raw").cast("double"))
    .join(products_s.select("product_id"), "product_id", "inner")
    .select("order_id", "line_id", "product_id", "qty", "unit_price")
)
save_silver(order_items_s, "order_items")

# silver.payments — cast amount, join order for validity
payments_s = (
    read_bronze("payments_raw")
    .withColumn("amount", F.col("amount_raw").cast("double"))
    .withColumn("payment_date", F.to_date("payment_ts"))
    .join(orders_s.select("order_id"), "order_id", "inner")
    .select("payment_id", "order_id", "amount", "method_raw", "payment_date")
    .withColumnRenamed("method_raw", "method")
)
save_silver(payments_s, "payments")

# silver.refunds
refunds_s = (
    read_bronze("refunds_raw")
    .withColumn("amount", F.col("amount_raw").cast("double"))
    .withColumn("refund_date", F.to_date("refund_ts"))
    .select("refund_id", "order_id", "amount", "reason_raw", "refund_date")
    .withColumnRenamed("reason_raw", "reason")
)
save_silver(refunds_s, "refunds")

# silver.inventory_snapshot — dedupe to one snapshot per sku/warehouse/day
inv_s = (
    read_bronze("inventory_raw")
    .withColumn("snapshot_date", F.to_date("snapshot_ts"))
    .withColumn("qty_on_hand", F.col("qty_on_hand_raw").cast("int"))
    .groupBy("sku", "warehouse_id", "snapshot_date")
    .agg(F.last("qty_on_hand").alias("qty_on_hand"))
)
save_silver(inv_s, "inventory_snapshot")

# silver.shipments
shipments_s = (
    read_bronze("shipments_raw")
    .join(orders_s.select("order_id"), "order_id", "inner")
    .withColumnRenamed("carrier_raw", "carrier")
    .withColumnRenamed("status_raw", "status")
)
save_silver(shipments_s, "shipments")

# silver.clickstream_sessions — sessionize on 30-min inactivity gap
click_raw = read_bronze("clickstream_events_raw").withColumn("event_ts", F.col("event_ts").cast("timestamp"))
w_cust = Window.partitionBy("customer_id").orderBy("event_ts")
sessions_s = (
    click_raw
    .withColumn("prev_ts", F.lag("event_ts").over(w_cust))
    .withColumn("gap_min", (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 60)
    .withColumn("new_session", F.when((F.col("gap_min").isNull()) | (F.col("gap_min") > 30), 1).otherwise(0))
    .withColumn("session_seq", F.sum("new_session").over(w_cust.rowsBetween(Window.unboundedPreceding, 0)))
    .withColumn("session_id", F.concat_ws("-", "customer_id", "session_seq"))
    .groupBy("session_id", "customer_id")
    .agg(F.min("event_ts").alias("start_ts"), F.max("event_ts").alias("end_ts"), F.count("*").alias("page_count"))
)
save_silver(sessions_s, "clickstream_sessions")

# silver.cart_events
cart_s = (
    read_bronze("cart_events_raw")
    .join(customers_s.select("customer_id"), "customer_id", "inner")
    .join(products_s.select("product_id"), "product_id", "inner")
)
save_silver(cart_s, "cart_events")

# silver.reviews — derive simple sentiment bucket from rating
reviews_s = (
    read_bronze("reviews_raw")
    .withColumnRenamed("rating_raw", "rating")
    .withColumn("sentiment", F.when(F.col("rating") >= 4, "positive").when(F.col("rating") == 3, "neutral").otherwise("negative"))
    .select("review_id", "product_id", "customer_id", "rating", "sentiment", "review_ts")
)
save_silver(reviews_s, "reviews")

# silver.marketing_campaigns
save_silver(read_bronze("marketing_campaigns_raw").withColumnRenamed("name_raw", "name").withColumnRenamed("channel_raw", "channel"), "marketing_campaigns")

# silver.ad_spend_daily — aggregate raw feed (already daily grain here, but roll up defensively)
ad_spend_s = (
    read_bronze("ad_spend_raw")
    .withColumn("spend", F.col("spend_raw").cast("double"))
    .groupBy("campaign_id", "platform", "spend_date")
    .agg(F.sum("spend").alias("spend"))
)
save_silver(ad_spend_s, "ad_spend_daily")

# silver.email_events
email_s = (
    read_bronze("email_events_raw")
    .join(customers_s.select("customer_id"), "customer_id", "inner")
)
save_silver(email_s, "email_events")

# silver.promotions
promos_s = (
    read_bronze("promotions_raw")
    .withColumn("discount_pct", F.regexp_replace("discount_raw", "%", "").cast("int"))
    .select("promo_id", "code_raw", "discount_pct", "valid_from", "valid_to")
    .withColumnRenamed("code_raw", "code")
)
save_silver(promos_s, "promotions")

# silver.support_tickets
tickets_s = (
    read_bronze("support_tickets_raw")
    .join(customers_s.select("customer_id"), "customer_id", "inner")
    .withColumnRenamed("category_raw", "category")
)
save_silver(tickets_s, "support_tickets")

# COMMAND ----------

# MAGIC %md ## Gold — dimensions, facts, business aggregates (12 tables)

# COMMAND ----------

def read_silver(name):
    return spark.table(f"{CATALOG}.silver.{name}")

def save_gold(df, name):
    df.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.gold.{name}")
    print(f"gold.{name}: {df.count()} rows")

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

# MAGIC %md
# MAGIC ### Reusable SCD2 merge (the `MERGE INTO` two-branch trick)
# MAGIC Union the incoming batch twice: once keyed on the real business key (matches
# MAGIC existing rows so unchanged rows are a no-op and brand-new keys fall through to
# MAGIC insert), and once with a **NULL** merge key for just the rows whose tracked
# MAGIC columns changed — a NULL key can never match an existing row, so Delta is forced
# MAGIC to insert it as a fresh version instead of colliding with the row being closed out.

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

    # current version of each business key, columns prefixed to avoid join ambiguity
    current_cols = [business_key] + tracked_cols
    target_current = target.toDF().filter("is_current = true").select(*current_cols)
    for c in current_cols:
        target_current = target_current.withColumnRenamed(c, f"tgt_{c}")

    joined = updates_df.join(
        target_current, updates_df[business_key] == target_current[f"tgt_{business_key}"], "inner"
    )
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
    insert_values.update({
        "effective_from": "current_date()",
        "effective_to": "CAST(NULL AS DATE)",
        "is_current": "true",
    })

    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.{business_key} = s.merge_key")
        .whenMatchedUpdate(condition=when_matched_cond, set={"is_current": "false", "effective_to": "current_date()"})
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    print(f"{target_table_full}: merged {updates_df.count()} source rows ({changed_rows.count()} changed)")

# COMMAND ----------

# gold.dim_customer — first pass = initial load (table doesn't exist yet)
customer_attrs = read_silver("customers").select("customer_id", "email", "source_system")
scd2_merge(f"{CATALOG}.gold.dim_customer", customer_attrs, "customer_id", ["email", "source_system"], "customer_sk")
dim_customer = spark.table(f"{CATALOG}.gold.dim_customer").filter("is_current = true")

# gold.dim_product — first pass = initial load (table doesn't exist yet)
product_attrs = read_silver("products").select("product_id", "category", "subcategory", "unit_price")
scd2_merge(f"{CATALOG}.gold.dim_product", product_attrs, "product_id", ["category", "subcategory", "unit_price"], "product_sk")
dim_product = spark.table(f"{CATALOG}.gold.dim_product").filter("is_current = true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo — a second incremental batch, so history actually forms
# MAGIC In production this second call would just be tomorrow's `silver.customers` /
# MAGIC `silver.products` load. Here we fabricate a small batch — a few emails changing,
# MAGIC a few product prices moving, plus some brand-new customers — so you can see the
# MAGIC old versions get closed out (`is_current = false`, `effective_to` set) and new
# MAGIC versions inserted, all inside this one run.

# COMMAND ----------

changed_customers = (
    customer_attrs.sample(fraction=0.05, seed=7)
    .withColumn("email", F.concat(F.lit("updated_"), F.col("email")))
)
new_customers = spark.createDataFrame(pd.DataFrame([{
    "customer_id": f"C{str(N_CUSTOMERS + i).zfill(5)}",
    "email": fake.email(), "source_system": "web",
} for i in range(1, 11)]))
customer_batch2 = changed_customers.unionByName(new_customers)

scd2_merge(f"{CATALOG}.gold.dim_customer", customer_batch2, "customer_id", ["email", "source_system"], "customer_sk")
dim_customer = spark.table(f"{CATALOG}.gold.dim_customer").filter("is_current = true")

changed_products = (
    product_attrs.sample(fraction=0.05, seed=7)
    .withColumn("unit_price", F.round(F.col("unit_price") * 1.1, 2))
)
scd2_merge(f"{CATALOG}.gold.dim_product", changed_products, "product_id", ["category", "subcategory", "unit_price"], "product_sk")
dim_product = spark.table(f"{CATALOG}.gold.dim_product").filter("is_current = true")

sample_id = changed_customers.select("customer_id").take(1)[0]["customer_id"]
display(
    spark.table(f"{CATALOG}.gold.dim_customer")
    .filter(F.col("customer_id") == sample_id)
    .orderBy("effective_from")
)

# gold.dim_store
dim_store = (
    read_bronze("store_locations_raw")
    .withColumn("store_sk", F.monotonically_increasing_id())
    .withColumnRenamed("name_raw", "store_name")
    .withColumnRenamed("region_raw", "region")
)
save_gold(dim_store, "dim_store")

# gold.fact_sales — 1 row per order line item
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

# gold.fact_inventory_daily
fact_inventory = (
    read_silver("inventory_snapshot")
    .withColumn("date_key", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
    .withColumn("days_of_supply", F.round(F.col("qty_on_hand") / F.lit(15), 1))  # ~15 units/day baseline demand assumption
)
save_gold(fact_inventory, "fact_inventory_daily")

# gold.fact_marketing_performance — spend joined with email engagement as proxy for impressions/clicks
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

# gold.customer_360
orders_agg = read_silver("orders").groupBy("customer_id").agg(F.count("*").alias("total_orders"), F.max("order_date").alias("last_order_date"))
revenue_agg = fact_sales.join(dim_customer.select("customer_sk", "customer_id"), "customer_sk").groupBy("customer_id").agg(F.sum("revenue").alias("total_revenue"))
reviews_agg = read_silver("reviews").groupBy("customer_id").agg(F.avg("rating").alias("avg_rating_given"))
tickets_agg = read_silver("support_tickets").groupBy("customer_id").agg(F.count("*").alias("support_tickets_count"))

customer_360 = (
    customers_s.select("customer_id")
    .join(orders_agg, "customer_id", "left")
    .join(revenue_agg, "customer_id", "left")
    .join(reviews_agg, "customer_id", "left")
    .join(tickets_agg, "customer_id", "left")
    .na.fill({"total_orders": 0, "total_revenue": 0.0, "support_tickets_count": 0})
)
save_gold(customer_360, "customer_360")

# gold.customer_ltv — simple heuristic LTV + churn risk for practice purposes
customer_ltv = (
    read_bronze("customers_raw").select("customer_id", "signup_ts")
    .join(customer_360, "customer_id")
    .withColumn("tenure_days", F.datediff(F.current_date(), F.to_date("signup_ts")))
    .withColumn("predicted_ltv", F.round(F.col("total_revenue") * 1.8, 2))
    .withColumn(
        "churn_risk_score",
        F.when(F.col("last_order_date") < F.date_sub(F.current_date(), 120), 0.8)
         .when(F.col("last_order_date") < F.date_sub(F.current_date(), 60), 0.5)
         .otherwise(0.2),
    )
    .select("customer_id", "predicted_ltv", "churn_risk_score", "tenure_days")
)
save_gold(customer_ltv, "customer_ltv")

# gold.daily_sales_summary — combine online fact_sales revenue with in-store POS by date/store
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

# gold.product_performance — units sold / revenue / return rate per product per month
sales_monthly = (
    fact_sales.join(dim_product.select("product_sk", "product_id"), "product_sk")
    .join(read_silver("orders").select("order_id", "order_date"), "order_id")
    .withColumn("month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("product_id", "month")
    .agg(F.sum("qty").alias("units_sold"), F.sum("revenue").alias("revenue"))
)
refunds_monthly = (
    read_silver("refunds")
    .join(order_items_s.select("order_id", "product_id"), "order_id")
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

# gold.campaign_roi
spend_total = read_silver("ad_spend_daily").groupBy("campaign_id").agg(F.sum("spend").alias("total_spend"))
# attribute revenue to campaigns via email-engaged customers as a simplified attribution model
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

# MAGIC %md ## Validate

# COMMAND ----------

for schema in ("bronze", "silver", "gold"):
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    print(f"{schema}: {len(tables)} tables")

# spot-check
display(spark.table(f"{CATALOG}.gold.customer_360").limit(10))

# COMMAND ----------

# MAGIC %md ## SCD2 version history — before/after
# MAGIC Full row history for one customer and one product that were touched by the
# MAGIC second incremental batch: the original row closed out (`is_current = false`,
# MAGIC `effective_to` populated) sitting alongside the new current row
# MAGIC (`is_current = true`, `effective_to` null).

# COMMAND ----------

sample_product_id = changed_products.select("product_id").take(1)[0]["product_id"]

print(f"customer_id = {sample_id}")
display(
    spark.table(f"{CATALOG}.gold.dim_customer")
    .filter(F.col("customer_id") == sample_id)
    .select("customer_sk", "customer_id", "email", "source_system", "effective_from", "effective_to", "is_current")
    .orderBy("effective_from")
)

print(f"product_id = {sample_product_id}")
display(
    spark.table(f"{CATALOG}.gold.dim_product")
    .filter(F.col("product_id") == sample_product_id)
    .select("product_sk", "product_id", "category", "subcategory", "unit_price", "effective_from", "effective_to", "is_current")
    .orderBy("effective_from")
)

# both dim tables should have more physical rows than distinct business keys once
# any row has been versioned twice
n_customer_rows = spark.table(f"{CATALOG}.gold.dim_customer").count()
n_customer_keys = spark.table(f"{CATALOG}.gold.dim_customer").select("customer_id").distinct().count()
n_product_rows = spark.table(f"{CATALOG}.gold.dim_product").count()
n_product_keys = spark.table(f"{CATALOG}.gold.dim_product").select("product_id").distinct().count()
print(f"dim_customer: {n_customer_rows} rows for {n_customer_keys} distinct customer_id ({n_customer_rows - n_customer_keys} versioned)")
print(f"dim_product:  {n_product_rows} rows for {n_product_keys} distinct product_id ({n_product_rows - n_product_keys} versioned)")
