# E-Commerce Lakehouse — Medallion Architecture on Databricks

A 50-table Bronze/Silver/Gold lakehouse for a synthetic e-commerce business, built on Databricks with Unity Catalog and Delta Lake. Generates its own realistic sample data (Faker) so the whole pipeline runs end to end with no external data source required.

## Problem

Retail/e-commerce teams typically need clean, trustworthy, analysis-ready data pulled together from many disconnected source systems — OMS, CRM, WMS, ad platforms, POS, support desks — before they can answer basic business questions (revenue by day, inventory health, campaign ROI, customer lifetime value). This project demonstrates a reference implementation of that pipeline using the medallion architecture:

- **Bronze** — raw data landed as-is from each source system
- **Silver** — cleaned, deduplicated, conformed, joined at entity grain
- **Gold** — business-ready dimensional model and reporting marts, including a proper SCD Type 2 customer/product dimension

## Architecture

```
Bronze (20 tables)          Silver (18 tables)         Gold (12 tables)
raw, append-only      -->   cleaned & conformed   -->  dims, facts, marts
1:1 with source              deduped, typed, joined     star schema + aggregates
```

Catalog / schema layout in Unity Catalog: `ecommerce_lakehouse.{bronze,silver,gold}`.

Full styled reference of all 50 tables (grain, keys, lineage): [`docs/medallion_catalog.html`](docs/medallion_catalog.html).

## Table catalog

### Bronze — raw ingestion (20 tables)

| Table | Grain / Source |
|---|---|
| customers_raw | 1 row per signup event · CRM API |
| products_raw | 1 row per catalog feed row · PIM export |
| orders_raw | 1 row per order event · OMS |
| order_items_raw | 1 row per line item · OMS |
| payments_raw | 1 row per payment attempt · gateway webhook |
| refunds_raw | 1 row per refund event · OMS |
| inventory_raw | 1 row per stock snapshot · WMS |
| warehouses_raw | 1 row per warehouse · WMS master |
| suppliers_raw | 1 row per supplier · vendor master |
| shipments_raw | 1 row per tracking update · carrier API |
| clickstream_events_raw | 1 row per web/app event · Kafka |
| cart_events_raw | 1 row per cart add/remove · Kafka |
| reviews_raw | 1 row per review submission · web form |
| marketing_campaigns_raw | 1 row per campaign · marketing CMS |
| ad_spend_raw | 1 row per platform/day feed · Ads APIs |
| email_events_raw | 1 row per email interaction · ESP webhook |
| promotions_raw | 1 row per coupon definition · promo engine |
| support_tickets_raw | 1 row per ticket event · helpdesk |
| pos_transactions_raw | 1 row per in-store sale · POS |
| store_locations_raw | 1 row per store · facilities master |

### Silver — conformed (18 tables)

| Table | Transform applied |
|---|---|
| customers | dedupe on latest ingest, standardize email/region |
| products | clean category hierarchy, cast price |
| orders | validate status enum, derive order_date |
| order_items | join to product, cast qty/price |
| payments | reconcile against order total |
| refunds | join to order, normalize reason codes |
| inventory_snapshot | dedupe per SKU/warehouse/day |
| warehouses | standardize region codes |
| suppliers | validate contact email |
| shipments | join to order, normalize carrier status |
| clickstream_sessions | sessionize via 30-min inactivity window |
| cart_events | join to customer + product |
| reviews | join to product + customer, derive sentiment |
| marketing_campaigns | standardize channel taxonomy |
| ad_spend_daily | aggregate raw feed to platform/day |
| email_events | join to campaign + customer |
| promotions | normalize discount type, validity window |
| support_tickets | join to customer + order, standardize category |

### Gold — served (12 tables)

| Table | Grain | Built from |
|---|---|---|
| dim_customer | 1 row per customer version (SCD2) | silver.customers |
| dim_product | 1 row per product version (SCD2) | silver.products |
| dim_date | 1 row per calendar date | generated |
| dim_store | 1 row per store | bronze.store_locations_raw |
| fact_sales | 1 row per order line item | silver.order_items + orders + dims |
| fact_inventory_daily | 1 row per SKU/warehouse/day | silver.inventory_snapshot |
| fact_marketing_performance | 1 row per campaign/channel/day | silver.ad_spend_daily + email_events |
| customer_360 | 1 row per customer | orders, reviews, tickets, clickstream |
| customer_ltv | 1 row per customer | gold.customer_360 |
| daily_sales_summary | 1 row per store/day | gold.fact_sales + pos_transactions |
| product_performance | 1 row per product/month | gold.fact_sales + refunds |
| campaign_roi | 1 row per campaign | gold.fact_marketing_performance + fact_sales |

## SCD Type 2 dimensions

`gold.dim_customer` and `gold.dim_product` use a reusable `scd2_merge()` function implementing the classic Databricks two-branch `MERGE INTO` pattern: the incoming batch is unioned with itself twice — once keyed on the real business key, once with a `NULL` merge key for just the rows whose tracked columns changed — so a single `MERGE` both closes out the old version (`is_current = false`, `effective_to` set) and inserts the new one (`is_current = true`), without the two colliding. The notebook includes a demo batch (simulated email/price changes + new customers/products) to show real version history forming.

## Repo layout

```
notebooks/medallion_ecommerce_notebook.py   Databricks notebook — generates data, builds all 50 tables
jobs/medallion_job.json                     Job spec used to run the notebook as a one-time / scheduled job
docs/medallion_catalog.html                 Styled visual reference of the full table catalog
```

## How to run

1. Import `notebooks/medallion_ecommerce_notebook.py` into your Databricks workspace (Workspace → Import, or `databricks workspace import`).
2. Create a job pointing at it — `jobs/medallion_job.json` is a ready-to-use spec for serverless job compute:
   ```
   databricks jobs create --json @jobs/medallion_job.json
   databricks jobs run-now <job_id>
   ```
   (Adjust `notebook_path` in the JSON to match where you imported it.)
3. The notebook provisions the `ecommerce_lakehouse` catalog and `bronze`/`silver`/`gold` schemas itself — requires `CREATE CATALOG` permission in Unity Catalog (or point it at an existing catalog you already have `CREATE SCHEMA` rights on).

Sized for interactive/small-scale use — low thousands of rows per table, runs in a few minutes on serverless compute.

## Results

- All 50 tables build successfully end to end (20 bronze / 18 silver / 12 gold) on Databricks serverless job compute, ~4 minutes total runtime.
- SCD2 merge verified: multiple historical versions form per changed customer/product, with zero customers ever having more than one `is_current = true` row.
