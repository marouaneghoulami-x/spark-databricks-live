# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset (after a rehearsal)
# MAGIC Puts the lab back to "day 1": only batch 01 in the volume.

# COMMAND ----------

CATALOG = "live_spark_lab"
SCHEMA = "nyc_taxi"
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw/trips"

# COMMAND ----------

# MAGIC %md ## Option A (soft reset): remove batch 02, keep everything else

# COMMAND ----------

dbutils.fs.rm(f"{RAW_PATH}/batch_02", True)
display(dbutils.fs.ls(RAW_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC Then open the pipeline and click **Run pipeline → Full refresh all**:
# MAGIC every table is rebuilt from batch 01 only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option B (hard reset): delete everything
# MAGIC 1. **Delete the pipeline** first (Jobs & Pipelines → ⋮ → Delete): this also removes its tables
# MAGIC 2. Run the cell below, then run `00_setup` again

# COMMAND ----------

# spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")   # uncomment on purpose
