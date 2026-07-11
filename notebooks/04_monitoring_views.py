# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Monitoring
# MAGIC Surfaces pipeline health on every run, backed by native Databricks observability
# MAGIC sources — no custom logging table needed:
# MAGIC - **Data quality**: DLT expectation drop/warn counts per table, read from the
# MAGIC   pipeline's own event log via the `event_log()` table function
# MAGIC - **Job run history**: recent run outcomes/durations, read from the
# MAGIC   `system.lakeflow` system tables (Databricks records this for every job run
# MAGIC   in the account automatically)
# MAGIC
# MAGIC Deliberately queries and `display()`s results rather than `CREATE VIEW` — this
# MAGIC workspace's Unity Catalog metastore is at its account-wide table quota (500),
# MAGIC shared with other pre-existing projects, so this step adds zero new catalog
# MAGIC objects. Wire the same queries into a scheduled alert or a BI tool's live query
# MAGIC once there's quota headroom to persist them as views.

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_lakehouse_dev")
dbutils.widgets.text("pipeline_id", "")
CATALOG = dbutils.widgets.get("catalog")
PIPELINE_ID = dbutils.widgets.get("pipeline_id")

# COMMAND ----------

# MAGIC %md ## Data quality — expectation drop/warn counts per table

# COMMAND ----------

# event_log() requires a literal argument, not a bind parameter — safe here since
# PIPELINE_ID comes from the job's own base_parameters (the pipeline's own ID),
# not user input.
dq = spark.sql(f"""
    SELECT
        origin.flow_name AS table_name,
        CAST(details:flow_progress.data_quality.dropped_records AS BIGINT) AS dropped_records,
        CAST(details:flow_progress.data_quality.warned_records AS BIGINT) AS warned_records,
        CAST(details:flow_progress.metrics.num_output_rows AS BIGINT) AS output_rows
    FROM event_log('{PIPELINE_ID}')
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.data_quality IS NOT NULL
""")

dq_summary = (
    dq.groupBy("table_name")
    .agg({"dropped_records": "sum", "warned_records": "sum", "output_rows": "max"})
    .withColumnRenamed("sum(dropped_records)", "total_dropped")
    .withColumnRenamed("sum(warned_records)", "total_warned")
    .withColumnRenamed("max(output_rows)", "latest_output_rows")
    .orderBy("total_dropped", ascending=False)
)
display(dq_summary)

bad_rows = dq_summary.filter("total_dropped > 0").count()
print(f"{CATALOG}: {bad_rows} table(s) with dropped records this run")

# COMMAND ----------

# MAGIC %md ## Job run history — recent outcomes and durations

# COMMAND ----------

run_history = spark.sql("""
    SELECT
        j.name AS job_name,
        t.run_id,
        t.period_start_time,
        t.result_state,
        t.trigger_type,
        t.run_duration_seconds
    FROM system.lakeflow.job_run_timeline t
    JOIN system.lakeflow.jobs j
      ON t.job_id = j.job_id AND t.workspace_id = j.workspace_id
    WHERE j.name LIKE '%medallion-ecommerce-build%'
    ORDER BY t.period_start_time DESC
    LIMIT 20
""")
display(run_history)
