# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Product recommendation model (MLflow)
# MAGIC Trains an ALS (alternating least squares) collaborative-filtering model on
# MAGIC `gold.fact_sales` — implicit feedback from purchase quantity — and logs the run
# MAGIC (params, metrics, model artifact) to MLflow, registering the model in the
# MAGIC Unity Catalog Model Registry.

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "ecommerce_lakehouse_dev")
CATALOG = dbutils.widgets.get("catalog")
MODEL_NAME = f"{CATALOG}.gold.product_recommender"

mlflow.set_registry_uri("databricks-uc")
experiment_path = f"/Shared/{CATALOG}/product_recommender"
mlflow.set_experiment(experiment_path)

# COMMAND ----------

# ALS requires integer user/item IDs, but customer_sk/product_sk are UUID strings
# (the SCD2 surrogate keys). Map each to a small integer with dense_rank — a plain
# DataFrame transform, not an MLlib Estimator, so it doesn't go through Spark
# Connect ML's model cache (which enforces a per-model size cap on serverless and
# rejects even tiny StringIndexer fits for reasons unrelated to actual data size).
raw_ratings = (
    spark.table(f"{CATALOG}.gold.fact_sales")
    .groupBy("customer_sk", "product_sk")
    .agg(F.sum("qty").alias("purchase_count"))
)

customer_map = (
    raw_ratings.select("customer_sk").distinct()
    .withColumn("customer_idx", (F.dense_rank().over(Window.orderBy("customer_sk")) - 1).cast("int"))
)
product_map = (
    raw_ratings.select("product_sk").distinct()
    .withColumn("product_idx", (F.dense_rank().over(Window.orderBy("product_sk")) - 1).cast("int"))
)

ratings = raw_ratings.join(customer_map, "customer_sk").join(product_map, "product_sk")

train_df, test_df = ratings.randomSplit([0.8, 0.2], seed=42)
print(f"train rows: {train_df.count()}, test rows: {test_df.count()}")

# COMMAND ----------

with mlflow.start_run(run_name="als_product_recommender") as run:
    rank = 10
    max_iter = 10
    reg_param = 0.1

    mlflow.log_params({"rank": rank, "maxIter": max_iter, "regParam": reg_param, "implicitPrefs": True})

    als = ALS(
        userCol="customer_idx", itemCol="product_idx", ratingCol="purchase_count",
        rank=rank, maxIter=max_iter, regParam=reg_param,
        implicitPrefs=True, coldStartStrategy="drop", nonnegative=True,
    )
    model = als.fit(train_df)

    predictions = model.transform(test_df)
    evaluator = RegressionEvaluator(metricName="rmse", labelCol="purchase_count", predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)
    mlflow.log_metric("rmse", rmse)
    print(f"RMSE on held-out purchases: {rmse:.4f}")

    signature = mlflow.models.infer_signature(
        train_df.select("customer_idx", "product_idx").limit(5).toPandas(),
        predictions.select("customer_idx", "product_idx", "prediction").limit(5).toPandas(),
    )
    mlflow.spark.log_model(
        model, artifact_path="model", registered_model_name=MODEL_NAME, signature=signature,
        dfs_tmpdir=f"/Volumes/{CATALOG}/landing/raw_files/_mlflow_tmp",
    )

    print(f"run_id: {run.info.run_id}")
    print(f"registered as: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md ## Sample recommendations — top 5 products for a handful of customers
# MAGIC `ALSModel.recommendForUserSubset` uses an internal RDD API that Unity Catalog's
# MAGIC serverless compute blocks outright, so recommendations are built manually here:
# MAGIC score every (sample customer, product) pair with the plain `model.transform()`
# MAGIC call already proven to work (it's what computed RMSE above), then rank with a
# MAGIC window function — pure DataFrame API, no RDD.

# COMMAND ----------

sample_customers = ratings.select("customer_idx").distinct().limit(5)
candidate_pairs = sample_customers.crossJoin(product_map.select("product_idx"))
scored = model.transform(candidate_pairs)

rank_window = Window.partitionBy("customer_idx").orderBy(F.col("prediction").desc())
readable_recs = (
    scored
    .withColumn("rank", F.row_number().over(rank_window))
    .filter("rank <= 5")
    .join(customer_map, "customer_idx")
    .join(product_map, "product_idx")
    .select("customer_sk", "product_sk", F.col("prediction").alias("score"))
)
display(readable_recs)
