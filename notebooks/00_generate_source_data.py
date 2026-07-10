# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Simulate the 5 upstream data sources
# MAGIC Lands synthetic files into a Unity Catalog Volume, shaped exactly like what each
# MAGIC real upstream system would hand off, so the DLT pipeline downstream ingests them
# MAGIC with the *real* mechanics (Auto Loader, CDC apply_changes, streaming) rather than
# MAGIC a plain in-notebook DataFrame.
# MAGIC
# MAGIC | Source | Real system | Landing format | Ingested by |
# MAGIC |---|---|---|---|
# MAGIC | Clickstream | Kafka / Event Hubs | JSON, one file per micro-batch | Structured Streaming + Auto Loader |
# MAGIC | Transactional (orders/payments/refunds) | PostgreSQL via Debezium CDC | JSON, Debezium-style change envelopes (`op`, `before`, `after`, `ts_ms`) | `dlt.apply_changes` |
# MAGIC | Product catalog | Internal REST API, daily pull | JSON, one file per day | Auto Loader (batch trigger) |
# MAGIC | Marketing (campaigns + ad spend) | Google Ads / Facebook Ads APIs, daily pull | JSON, one file per day | Auto Loader (batch trigger) |
# MAGIC | Customer demographics | Third-party vendor, daily S3 drop | CSV/Parquet | Auto Loader (schema evolution) |
# MAGIC
# MAGIC Everything else (warehouses, suppliers, shipments, reviews, cart events, promotions,
# MAGIC support tickets, POS, store locations, inventory) is landed the same way real internal
# MAGIC system exports usually arrive: files dropped for Auto Loader to pick up.

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

dbutils.widgets.text("catalog", "ecommerce_lakehouse_dev")
CATALOG = dbutils.widgets.get("catalog")
VOLUME_ROOT = f"/Volumes/{CATALOG}/landing/raw_files"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw_files")

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")

def land_json(records, folder, filename=None):
    path = f"{VOLUME_ROOT}/{folder}"
    dbutils.fs.mkdirs(path)
    filename = filename or f"{RUN_TS}_{uuid.uuid4().hex[:8]}.json"
    full_path = f"{path}/{filename}"
    body = "\n".join(json.dumps(r, default=str) for r in records)
    dbutils.fs.put(full_path, body, overwrite=True)
    print(f"landed {len(records)} records -> {full_path}")

def land_csv(rows, header, folder, filename=None):
    path = f"{VOLUME_ROOT}/{folder}"
    dbutils.fs.mkdirs(path)
    filename = filename or f"{RUN_TS}_{uuid.uuid4().hex[:8]}.csv"
    full_path = f"{path}/{filename}"
    lines = [",".join(header)] + [",".join(str(v) for v in row) for row in rows]
    dbutils.fs.put(full_path, "\n".join(lines), overwrite=True)
    print(f"landed {len(rows)} rows -> {full_path}")

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
PLATFORMS = ["Google Ads", "Meta Ads", "TikTok Ads"]
CARRIERS = ["UPS", "FedEx", "USPS", "DHL"]

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def now_minus(days_back_max):
    return datetime.now() - timedelta(days=random.uniform(0, days_back_max), hours=random.uniform(0, 24))

# COMMAND ----------

# MAGIC %md ## 1. Clickstream — Kafka/Event Hubs style JSON, landed as a streaming micro-batch

# COMMAND ----------

clickstream_events = []
for i in range(20000):
    cust = random.choice(customer_ids)
    clickstream_events.append({
        "event_id": f"EVT{str(i).zfill(7)}", "session_id_raw": f"{cust}-{random.randint(1, 40)}",
        "customer_id": cust, "event_type": random.choice(["page_view", "product_view", "search", "checkout_start"]),
        "url": f"/products/{random.choice(product_ids)}", "event_ts": iso(now_minus(90)),
    })
land_json(clickstream_events, "clickstream")

# COMMAND ----------

# MAGIC %md ## 2. Transactional — Debezium-style CDC envelopes (orders, payments, refunds)

# COMMAND ----------

def cdc_envelope(op, after, ts_ms):
    return {"op": op, "before": None, "after": after, "ts_ms": ts_ms, "source": {"db": "ecommerce", "table": after.get("__table")}}

order_customer = {}
order_dt = {}
order_events = []
for oid in order_ids:
    cust = random.choice(customer_ids)
    dt = now_minus(365)
    order_customer[oid] = cust
    order_dt[oid] = dt
    after = {
        "order_id": oid, "customer_id": cust, "order_ts": iso(dt),
        "status_raw": random.choice(["placed", "PLACED", "shipped", "Shipped", "delivered", "cancelled", "Cancelled "]),
        "__table": "orders",
    }
    order_events.append(cdc_envelope("c", after, int(dt.timestamp() * 1000)))
land_json(order_events, "cdc/orders")

payment_events = []
for i, oid in enumerate(order_ids, start=1):
    pay_ts = order_dt[oid] + timedelta(minutes=random.randint(1, 30))
    after = {
        "payment_id": f"PAY{str(i).zfill(6)}", "order_id": oid,
        "amount_raw": str(round(random.uniform(10, 900), 2)),
        "method_raw": random.choice(["credit_card", "paypal", "gift_card", "apple_pay"]),
        "payment_ts": iso(pay_ts), "__table": "payments",
    }
    payment_events.append(cdc_envelope("c", after, int(pay_ts.timestamp() * 1000)))
land_json(payment_events, "cdc/payments")

refund_orders = random.sample(order_ids, int(N_ORDERS * 0.05))
refund_events = []
for i, oid in enumerate(refund_orders, start=1):
    refund_ts = order_dt[oid] + timedelta(days=random.randint(2, 20))
    after = {
        "refund_id": f"REF{str(i).zfill(5)}", "order_id": oid,
        "amount_raw": str(round(random.uniform(5, 300), 2)),
        "reason_raw": random.choice(["damaged", "wrong_item", "changed_mind", "late_delivery"]),
        "refund_ts": iso(refund_ts), "__table": "refunds",
    }
    refund_events.append(cdc_envelope("c", after, int(refund_ts.timestamp() * 1000)))
land_json(refund_events, "cdc/refunds")

# COMMAND ----------

# MAGIC %md ## 3. Product catalog — internal REST API, one JSON dump per day

# COMMAND ----------

product_rows = []
for pid in product_ids:
    cat = random.choice(list(CATEGORIES.keys()))
    sub = random.choice(CATEGORIES[cat])
    product_rows.append({
        "product_id": pid, "sku": f"SKU-{pid}", "category_raw": f" {cat}/{sub} ",
        "price_raw": str(round(random.uniform(5, 400), 2)), "pulled_at": iso(datetime.now()),
    })
land_json(product_rows, "product_catalog", filename=f"catalog_{RUN_TS}.json")

# COMMAND ----------

# MAGIC %md ## 4. Marketing — Google/Facebook Ads API daily pulls (campaigns + spend)

# COMMAND ----------

campaign_rows = []
for cid in campaign_ids:
    start = now_minus(180)
    campaign_rows.append({
        "campaign_id": cid, "name_raw": f"{fake.bs().title()} Campaign",
        "channel_raw": random.choice(["Paid Search", "Paid Social", "Email", "Affiliate", "Display"]),
        "start_date": start.date().isoformat(), "end_date": (start + timedelta(days=random.randint(7, 45))).date().isoformat(),
    })
land_json(campaign_rows, "marketing/campaigns", filename=f"campaigns_{RUN_TS}.json")

ad_spend_rows = []
i = 0
for cid in campaign_ids:
    for platform in PLATFORMS:
        for d in range(30):
            i += 1
            ad_spend_rows.append({
                "spend_id": f"SPD{str(i).zfill(6)}", "campaign_id": cid, "platform": platform,
                "spend_raw": str(round(random.uniform(20, 800), 2)),
                "spend_date": (datetime.now() - timedelta(days=d)).date().isoformat(),
            })
land_json(ad_spend_rows, "marketing/ad_spend", filename=f"adspend_{RUN_TS}.json")

email_rows = [{
    "event_id": f"EML{str(i).zfill(6)}", "campaign_id": random.choice(campaign_ids),
    "customer_id": random.choice(customer_ids), "event_type": random.choice(["sent", "open", "click", "unsubscribe"]),
    "event_ts": iso(now_minus(180)),
} for i in range(6000)]
land_json(email_rows, "marketing/email_events")

# COMMAND ----------

# MAGIC %md ## 5. Customer demographics — third-party vendor, daily CSV drop (S3-style)

# COMMAND ----------

income_brackets = ["<40k", "40k-70k", "70k-100k", "100k-150k", "150k+"]
household_sizes = [1, 2, 3, 4, 5]
demo_rows = []
for cid in customer_ids:
    demo_rows.append([
        cid, random.randint(18, 75), random.choice(income_brackets),
        random.choice(household_sizes), round(random.uniform(0.2, 1.8), 2),
    ])
land_csv(
    demo_rows,
    header=["customer_id", "age", "income_bracket", "household_size", "region_affluence_index"],
    folder="vendor/customer_demographics",
    filename=f"demographics_{RUN_TS}.csv",
)

# COMMAND ----------

# MAGIC %md ## 6. Everything else — internal system file exports (Auto Loader ingestion)

# COMMAND ----------

customer_rows = [{
    "customer_id": cid, "email": fake.email(), "signup_ts": iso(now_minus(720)),
    "source_system": random.choice(["web", "mobile_app", "store_kiosk"]),
} for cid in customer_ids]
land_json(customer_rows, "internal/customers")

warehouse_rows = [{"warehouse_id": w, "name_raw": f"{fake.city()} DC", "region_raw": random.choice(["US-EAST", "US-WEST", "US-CENTRAL", "EU"])} for w in warehouse_ids]
land_json(warehouse_rows, "internal/warehouses")

supplier_rows = [{"supplier_id": s, "name_raw": fake.company(), "contact_raw": fake.company_email()} for s in supplier_ids]
land_json(supplier_rows, "internal/suppliers")

store_rows = [{"store_id": s, "name_raw": f"{fake.city()} Store", "address_raw": fake.address().replace("\n", ", "), "region_raw": random.choice(["US-EAST", "US-WEST", "US-CENTRAL"])} for s in store_ids]
land_json(store_rows, "internal/store_locations")

order_item_rows = []
line = 1
for oid in order_ids:
    n_items = random.randint(1, 4)
    for i, pid in enumerate(random.sample(product_ids, n_items), start=1):
        order_item_rows.append({
            "order_id": oid, "line_id": i, "product_id": pid,
            "qty": random.randint(1, 3), "unit_price_raw": str(round(random.uniform(5, 400), 2)),
        })
land_json(order_item_rows, "internal/order_items")

inventory_rows = []
for pid in product_ids:
    for wid in warehouse_ids:
        for d in range(14):
            inventory_rows.append({
                "sku": f"SKU-{pid}", "warehouse_id": wid,
                "qty_on_hand_raw": random.randint(0, 500),
                "snapshot_ts": iso(datetime.now() - timedelta(days=d)),
            })
land_json(inventory_rows, "internal/inventory")

shippable = order_ids[: int(N_ORDERS * 0.9)]
shipment_rows = [{
    "shipment_id": f"SHP{str(i).zfill(6)}", "order_id": oid,
    "carrier_raw": random.choice(CARRIERS), "tracking_no": uuid.uuid4().hex[:12].upper(),
    "status_raw": random.choice(["in_transit", "delivered", "delayed"]),
} for i, oid in enumerate(shippable, start=1)]
land_json(shipment_rows, "internal/shipments")

cart_rows = [{
    "event_id": f"CART{str(i).zfill(6)}", "customer_id": random.choice(customer_ids),
    "product_id": random.choice(product_ids), "action": random.choice(["add", "remove"]),
    "event_ts": iso(now_minus(90)),
} for i in range(8000)]
land_json(cart_rows, "internal/cart_events")

review_rows = [{
    "review_id": f"RVW{str(i).zfill(5)}", "product_id": random.choice(product_ids),
    "customer_id": random.choice(customer_ids), "rating_raw": random.randint(1, 5),
    "text_raw": fake.sentence(nb_words=12), "review_ts": iso(now_minus(365)),
} for i in range(1500)]
land_json(review_rows, "internal/reviews")

promo_rows = []
for pid in promo_ids:
    start = now_minus(180)
    promo_rows.append({
        "promo_id": pid, "code_raw": fake.lexify(text="????-####").upper(),
        "discount_raw": f"{random.choice([10, 15, 20, 25, 30])}%",
        "valid_from": start.date().isoformat(), "valid_to": (start + timedelta(days=random.randint(7, 60))).date().isoformat(),
    })
land_json(promo_rows, "internal/promotions")

ticket_rows = [{
    "ticket_id": f"TIX{str(i).zfill(5)}", "customer_id": random.choice(customer_ids),
    "order_id": random.choice(order_ids), "category_raw": random.choice(["shipping", "billing", "product_defect", "general"]),
    "opened_ts": iso(now_minus(180)),
} for i in range(600)]
land_json(ticket_rows, "internal/support_tickets")

pos_rows = [{
    "txn_id": f"POS{str(i).zfill(6)}", "store_id": random.choice(store_ids),
    "sku": f"SKU-{random.choice(product_ids)}", "qty": random.randint(1, 3),
    "amount_raw": str(round(random.uniform(5, 400), 2)), "txn_ts": iso(now_minus(90)),
} for i in range(4000)]
land_json(pos_rows, "internal/pos_transactions")

print("All source files landed.")
