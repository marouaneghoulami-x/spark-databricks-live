# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — Unity Catalog objects + first batch of raw files
# MAGIC
# MAGIC **Story:** every day, raw NYC taxi trips land as CSV files in a folder.
# MAGIC We simulate that folder with a **Unity Catalog Volume**, filled from the sample table `samples.nyctaxi.trips`
# MAGIC (available in every Databricks workspace).
# MAGIC
# MAGIC What this notebook creates:
# MAGIC
# MAGIC | Object | Name |
# MAGIC |---|---|
# MAGIC | Catalog | `bootcamp_lab` |
# MAGIC | Schema | `bootcamp_lab.nyc_taxi` |
# MAGIC | Volume (files) | `bootcamp_lab.nyc_taxi.raw` → `/Volumes/bootcamp_lab/nyc_taxi/raw/trips/` |

# COMMAND ----------

CATALOG = "bootcamp_lab"   # no permission to create catalogs? use your existing one, e.g. "workspace"
SCHEMA = "nyc_taxi"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw/trips"

# COMMAND ----------

# MAGIC %md ## 1. Unity Catalog: catalog → schema → volume

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

# COMMAND ----------

# MAGIC %md ## 2. Look at the source data

# COMMAND ----------

trips = spark.read.table("samples.nyctaxi.trips")

print("rows:", trips.count())
display(trips.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Land batch 01 = all history except the last 7 days
# MAGIC The last week is kept for later (`02_new_data_arrives`) to show **incremental** processing.

# COMMAND ----------

from datetime import timedelta
from pyspark.sql import functions as F

last_day = trips.agg(F.max(F.to_date("tpep_pickup_datetime"))).first()[0]
cutoff = last_day - timedelta(days=7)
print("batch 01 = trips before", cutoff)

(trips
    .filter(F.to_date("tpep_pickup_datetime") < F.lit(cutoff))
    .write.mode("overwrite")
    .option("header", True)
    .csv(f"{RAW_PATH}/batch_01"))

display(dbutils.fs.ls(f"{RAW_PATH}/batch_01"))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ Done. Next: open **01_spark_under_the_hood**, then create the pipeline (see README).
