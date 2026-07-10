# E-Commerce Lakehouse — Medallion Architecture on Databricks

A 52-table Bronze/Silver/Gold lakehouse for a synthetic e-commerce business, built on Databricks with **Delta Live Tables, Auto Loader, Structured Streaming, Unity Catalog, Databricks Workflows, and MLflow**. Generates its own realistic sample data shaped like five real upstream systems, lands it as files/CDC events, and ingests it with the actual mechanics those systems would require — no external data source needed to run it end to end.

## Problem

Retail/e-commerce teams typically need clean, trustworthy, analysis-ready data pulled together from many disconnected source systems — a transactional database, a clickstream event bus, ad platforms, internal REST APIs, third-party vendors — before they can answer basic business questions (revenue by day, inventory health, campaign ROI, customer lifetime value, product recommendations). This project is a reference implementation of that pipeline using the medallion architecture, built to exercise the real ingestion mechanics for each source type rather than a single flat batch load.

## Data sources

Each source is simulated with data shaped exactly like the real system, landed in a Unity Catalog Volume, and ingested with the pattern that source actually requires:

| Source | Real system | Landing format | Ingested by |
|---|---|---|---|
| Clickstream events | Kafka / Event Hubs | JSON, one file per micro-batch | Auto Loader (Structured Streaming under the hood) |
| Transactional (orders, payments, refunds) | PostgreSQL via Debezium CDC | JSON, Debezium-style change envelopes (`op`, `before`, `after`, `ts_ms`) | `dlt.apply_changes` |
| Product catalog | Internal REST API, daily pull | JSON, one dump per day | Auto Loader (batch trigger) |
| Marketing (campaign spend + attribution) | Google Ads / Facebook Ads APIs, daily pull | JSON, one dump per day per platform | Auto Loader (batch trigger) |
| Customer demographics / enrichment | Third-party vendor, daily S3 drop | CSV | Auto Loader (schema evolution) |

Everything else (warehouses, suppliers, shipments, reviews, cart events, promotions, support tickets, POS transactions, store locations, inventory) lands the way ordinary internal system exports usually arrive — files dropped for Auto Loader to pick up.

## Tools used in Databricks

- **Delta Lake** — storage format for every table in every layer; ACID transactions, schema enforcement, time travel
- **Structured Streaming** — the clickstream bronze table is a genuine streaming read (`spark.readStream`), incrementally processing new files as they land
- **Auto Loader** (`cloudFiles`) — incremental file ingestion with schema inference/evolution for every file-based source (product catalog, marketing, demographics, and all the internal system exports)
- **Delta Live Tables** — a real declarative pipeline (`notebooks/01_dlt_pipeline.py`) building Bronze → Silver with `@dlt.table`, `dlt.apply_changes` for CDC, and `@dlt.expect` / `@dlt.expect_or_drop` / `@dlt.expect_or_fail` data quality rules at three severities
- **Unity Catalog** — governs the whole `ecommerce_lakehouse` catalog (bronze/silver/gold schemas, the landing volume, and the MLflow Model Registry entry), with lineage captured automatically across every join and merge
- **Databricks Workflows** — a 4-task job (`jobs/medallion_job.json`) chaining data generation → DLT pipeline → gold build → model training, each step depending on the last, with Slack alerting on any task failure
- **MLflow** — trains and tracks a real ALS collaborative-filtering recommendation model on `gold.fact_sales`, registered in the Unity Catalog Model Registry
- **Photon** — automatic on all Databricks serverless compute, which is what every task in this workflow runs on (this workspace is serverless-only)

## Architecture / pipeline design

```
00_generate_source_data  →  01_dlt_pipeline (DLT)      →  02_gold_layer        →  03_train_recommendation_model
lands synthetic files/       Auto Loader + apply_changes    dims, facts, SCD2       ALS on fact_sales, logged +
CDC events per source         + @dlt.expect quality rules    merges, marts           registered via MLflow
```

Each stage is a Databricks Workflows task depending on the one before it (`jobs/medallion_job.json`); a failure at any task posts to Slack via a `webhook_notifications.on_failure` destination.

Catalog / schema layout in Unity Catalog: `ecommerce_lakehouse.{bronze,silver,gold}`, plus `ecommerce_lakehouse.landing` for the staging Volume.

## Table catalog

### Bronze — raw ingestion (21 tables)

18 file-based sources ingested via Auto Loader, plus 3 CDC tables built with `dlt.apply_changes`:

`customers_raw` · `products_raw` · `warehouses_raw` · `suppliers_raw` · `store_locations_raw` · `order_items_raw` · `inventory_raw` · `shipments_raw` · `clickstream_events_raw` · `cart_events_raw` · `reviews_raw` · `marketing_campaigns_raw` · `ad_spend_raw` · `email_events_raw` · `promotions_raw` · `support_tickets_raw` · `pos_transactions_raw` · `customer_demographics_raw` · **`orders_raw`, `payments_raw`, `refunds_raw`** (CDC via `apply_changes`)

### Silver — conformed (19 tables)

Same list as bronze minus the three CDC staging flows, plus `customer_demographics` — each cleaned, deduped (`dedup_latest`, keeping the most recent row per business key since bronze streaming tables can carry more than one snapshot across runs), and joined at entity grain. Several carry `@dlt.expect*` data quality rules (valid email format, positive prices/amounts, plausible ages, valid ratings, non-null keys).

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
| customer_360 | 1 row per customer (now enriched with vendor demographics) | orders, reviews, tickets, demographics |
| customer_ltv | 1 row per customer (LTV weighted by vendor affluence signal) | gold.customer_360 |
| daily_sales_summary | 1 row per store/day | gold.fact_sales + pos_transactions |
| product_performance | 1 row per product/month | gold.fact_sales + refunds |
| campaign_roi | 1 row per campaign | gold.fact_marketing_performance + fact_sales |

Plus a registered MLflow model, `ecommerce_lakehouse.gold.product_recommender`.

## SCD Type 2 dimensions

`gold.dim_customer` and `gold.dim_product` use a reusable `scd2_merge()` function implementing the classic Databricks two-branch `MERGE INTO` pattern: the incoming batch is unioned with itself twice — once keyed on the real business key, once with a `NULL` merge key for just the rows whose tracked columns changed — so a single `MERGE` both closes out the old version (`is_current = false`, `effective_to` set) and inserts the new one (`is_current = true`), without the two colliding.

## Recommendation model

`notebooks/03_train_recommendation_model.py` trains an ALS (alternating least squares) implicit-feedback model on purchase counts from `gold.fact_sales`. Since ALS requires integer user/item IDs but `customer_sk`/`product_sk` are UUID strings (the SCD2 surrogate keys), they're mapped to small integers with a plain `dense_rank()` window function rather than an MLlib `StringIndexer` — Spark Connect ML's model cache on serverless compute rejected even a trivially small `StringIndexer` fit, so the mapping is done as an ordinary DataFrame transform instead. Similarly, `ALSModel.recommendForUserSubset` uses an internal RDD API that Unity Catalog's serverless compute blocks outright; the sample-recommendations cell instead cross-joins a handful of customers against all products and scores them with the plain (and already-proven) `model.transform()` call, ranked with a window function.

The trained model, params, and RMSE are logged to MLflow and registered in the Unity Catalog Model Registry as `ecommerce_lakehouse.gold.product_recommender`.

## Environments (dev / test / prod)

Deployed as a **Databricks Asset Bundle** (`databricks.yml`), with one target per environment — same job/pipeline definitions, different catalog and workspace path:

| Environment | Git branch | Catalog | Bundle target |
|---|---|---|---|
| Development | `develop` | `ecommerce_lakehouse_dev` | `dev` (`mode: development`) |
| Test | `test` | `ecommerce_lakehouse_test` | `test` (`mode: production`) |
| Production | `main` | `ecommerce_lakehouse_prod` | `prod` (`mode: production`) |

Each notebook takes the target catalog as a parameter instead of hardcoding `ecommerce_lakehouse` — `dbutils.widgets`/`base_parameters` for the three plain notebooks, and the DLT pipeline's `configuration` block (read via `spark.conf.get("catalog", ...)`) for the DLT notebook — so the exact same code deploys cleanly to all three without edits. `hotfixes` and `new-features` branch off `develop` and merge back into it before promotion.

## Repo layout

```
databricks.yml                               Asset Bundle: dev/test/prod targets, one job + one DLT pipeline resource
notebooks/00_generate_source_data.py         Lands synthetic files/CDC events simulating the 5 upstream sources
notebooks/01_dlt_pipeline.py                 Delta Live Tables pipeline: Bronze -> Silver
notebooks/02_gold_layer.py                   Dims, facts, SCD2 merges, reporting marts
notebooks/03_train_recommendation_model.py   ALS recommendation model, MLflow tracking + UC registration
pipelines/dlt_pipeline_spec.json             Standalone DLT pipeline spec (manual/single-env deploy, pre-dates the bundle)
jobs/medallion_job.json                      Standalone 4-task job spec (manual/single-env deploy, pre-dates the bundle)
```

## How to run

**Bundle deploy (recommended — handles dev/test/prod):**

1. Each environment's catalog and `/Shared/<catalog>` MLflow experiment folder must exist first (Unity Catalog `CREATE CATALOG` permission required):
   ```
   databricks catalogs create ecommerce_lakehouse_dev   # and _test, _prod
   databricks workspace mkdirs /Shared/ecommerce_lakehouse_dev   # and _test, _prod
   ```
2. Validate and deploy a target:
   ```
   databricks bundle validate --target dev
   databricks bundle deploy --target dev
   ```
3. Run it:
   ```
   databricks bundle run medallion_build --target dev
   ```
4. Repeat for `test` and `prod`. Each target gets its own job, DLT pipeline, and workspace path (`/Workspace/Users/<you>/.bundle/ecommerce_lakehouse/<target>`) — fully isolated from the others in the same workspace.
5. For Slack failure alerts, create a **Slack**-type notification destination in Databricks (Settings → Notifications) — not a generic Webhook destination, which Slack silently rejects the payload from — and swap its ID into `databricks.yml`'s `webhook_notifications.on_failure` blocks.

**Manual single-environment deploy** (the original approach, still works, no bundle): import the 4 notebooks, `databricks pipelines create --json @pipelines/dlt_pipeline_spec.json`, plug the returned `pipeline_id` into `jobs/medallion_job.json`, then `databricks jobs create --json @jobs/medallion_job.json`.

Sized for interactive/small-scale use — low thousands of rows per table, full workflow runs in under 10 minutes on serverless compute.

## Results

- All 52 tables (21 bronze / 19 silver / 12 gold) plus a registered MLflow model build successfully end to end on Databricks serverless compute — `generate_source_data` (~1 min) → `run_dlt_pipeline` (~4 min) → `build_gold_layer` (~1.5 min) → `train_recommendation_model` (~1 min), ~7.5 minutes total.
- CDC via `dlt.apply_changes` verified correct: `orders_raw`/`payments_raw`/`refunds_raw` hold exactly the source row counts with no duplication across repeated pipeline runs, unlike the plain Auto Loader streaming tables (by design — a real CDC feed reports true upserts, so `apply_changes` naturally stays consistent under re-processing).
- SCD2 merge verified: multiple historical versions form per changed customer/product, with zero customers or products ever having more than one `is_current = true` row.
- Vendor demographics enrichment confirmed flowing through the full chain: `bronze.customer_demographics_raw` → `silver.customer_demographics` → `gold.customer_360`/`customer_ltv`, all 500 synthetic customers enriched.
- Slack failure alerting verified live: a deliberately broken notebook was deployed, triggered a real task failure, and the alert was confirmed to land in the target Slack channel before the working notebook was restored.
- Multi-environment deployment verified: `dev` was deployed via `databricks bundle deploy --target dev` and run end to end (~7.5 min), landing all 12 gold tables and the registered model in the isolated `ecommerce_lakehouse_dev` catalog. `test` and `prod` are deployed the same way, each fully isolated by catalog and workspace path.
