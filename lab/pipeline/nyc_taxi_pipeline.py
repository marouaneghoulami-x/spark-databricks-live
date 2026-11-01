# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi pipeline (Lakeflow Declarative Pipelines)
# MAGIC
# MAGIC ⚠️ **Don't run this notebook's cells yourself.** Attach it as the source of an ETL pipeline and click **Run pipeline**.
# MAGIC
# MAGIC ```
# MAGIC CSV files in a UC Volume
# MAGIC   -> bronze_trips          (streaming table, Auto Loader: only new files are read)
# MAGIC   -> silver_trips          (streaming table, typed + data quality expectations)
# MAGIC   -> gold_daily_kpis       (materialized view)
# MAGIC   -> gold_top_pickup_zips  (materialized view)
# MAGIC ```
# MAGIC
# MAGIC We only declare **what** each table is. The pipeline works out the order (the DAG),
# MAGIC the incremental processing, retries and the data quality metrics.
# MAGIC Target catalog & schema are set in the pipeline settings: `bootcamp_lab.nyc_taxi`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_PATH = "/Volumes/bootcamp_lab/nyc_taxi/raw/trips"   # same path as in 00_setup

# COMMAND ----------

# MAGIC %md ## 🥉 Bronze: raw data, as it arrived

# COMMAND ----------

@dp.table(comment="Raw taxi trips, loaded incrementally from CSV files with Auto Loader")
def bronze_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .load(RAW_PATH)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("ingested_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md ## 🥈 Silver: clean & validated

# COMMAND ----------

@dp.table(comment="Clean, typed trips. Invalid rows are dropped by expectations.")
@dp.expect_or_drop("valid_fare", "fare_amount > 0")
@dp.expect_or_drop("valid_distance", "trip_distance > 0")
@dp.expect_or_drop("dropoff_after_pickup", "dropoff_time > pickup_time")
@dp.expect("known_pickup_zip", "pickup_zip IS NOT NULL")   # warn only, row is kept
def silver_trips():
    return (
        spark.readStream.table("bronze_trips")
        .select(
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_time"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_time"),
            F.col("trip_distance").cast("double"),
            F.col("fare_amount").cast("double"),
            F.col("pickup_zip").cast("int"),
            F.col("dropoff_zip").cast("int"),
            "source_file",
        )
        .withColumn("pickup_date", F.to_date("pickup_time"))
        .withColumn(
            "trip_minutes",
            F.round((F.unix_timestamp("dropoff_time") - F.unix_timestamp("pickup_time")) / 60, 1),
        )
    )

# COMMAND ----------

# MAGIC %md ## 🥇 Gold: business-ready answers

# COMMAND ----------

@dp.materialized_view(comment="Daily KPIs for the operations dashboard")
def gold_daily_kpis():
    return (
        spark.read.table("silver_trips")
        .groupBy("pickup_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
            F.round(F.avg("trip_minutes"), 1).alias("avg_minutes"),
        )
    )

# COMMAND ----------

@dp.materialized_view(comment="Top 10 pickup zip codes by number of trips")
def gold_top_pickup_zips():
    return (
        spark.read.table("silver_trips")
        .filter("pickup_zip IS NOT NULL")
        .groupBy("pickup_zip")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("fare_amount"), 2).alias("revenue"),
        )
        .orderBy(F.desc("trips"))
        .limit(10)
    )
