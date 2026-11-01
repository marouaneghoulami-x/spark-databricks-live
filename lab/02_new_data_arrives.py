# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · New data arrives
# MAGIC Day 2: the last 7 days of trips land in the folder, **with a few dirty rows**.
# MAGIC
# MAGIC After running this notebook, go back to the pipeline and click **Run pipeline**. Watch:
# MAGIC * `bronze_trips` reads **only the new file(s)** (Auto Loader remembers what it already loaded)
# MAGIC * `silver_trips` → **Data quality** tab: the dirty rows are dropped
# MAGIC * the gold materialized views are refreshed

# COMMAND ----------

CATALOG = "bootcamp_lab"
SCHEMA = "nyc_taxi"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw/trips"

# COMMAND ----------

from datetime import datetime, time, timedelta
from pyspark.sql import functions as F

trips = spark.read.table("samples.nyctaxi.trips")
last_day = trips.agg(F.max(F.to_date("tpep_pickup_datetime"))).first()[0]
cutoff = last_day - timedelta(days=7)

new_trips = trips.filter(F.to_date("tpep_pickup_datetime") >= F.lit(cutoff))

# COMMAND ----------

# MAGIC %md ## Some dirty rows, the kind real systems send

# COMMAND ----------

t = datetime.combine(last_day, time(10, 0))
bad_rows = spark.createDataFrame(
    [
        (t, t + timedelta(minutes=12), 2.5, -8.0, 10001, 10002),   # negative fare
        (t, t + timedelta(minutes=5), 0.0, 6.5, 10003, 10003),     # zero distance
        (t, t - timedelta(minutes=20), 3.1, 14.0, 10011, 10019),   # dropoff before pickup
    ],
    "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, trip_distance double, "
    "fare_amount double, pickup_zip int, dropoff_zip int",
)

batch_02 = new_trips.unionByName(bad_rows, allowMissingColumns=True)
print("rows in batch 02:", batch_02.count(), "(3 of them are dirty)")

# COMMAND ----------

(batch_02.write.mode("overwrite")
    .option("header", True)
    .csv(f"{RAW_PATH}/batch_02"))

display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Now go to **Jobs & Pipelines → your pipeline → Run pipeline**.
