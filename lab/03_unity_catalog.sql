-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Explore & govern with Unity Catalog
-- MAGIC Everything the pipeline produced is a governed Unity Catalog object: `catalog.schema.table`.

-- COMMAND ----------

USE CATALOG bootcamp_lab;
USE SCHEMA nyc_taxi;

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md ## 1. The answers the business asked for

-- COMMAND ----------

SELECT * FROM gold_daily_kpis ORDER BY pickup_date;

-- COMMAND ----------

SELECT * FROM gold_top_pickup_zips;

-- COMMAND ----------

-- MAGIC %md ## 2. Data quality: what did the expectations drop?

-- COMMAND ----------

SELECT
  timestamp,
  details:flow_progress.data_quality.dropped_records AS dropped_records,
  details:flow_progress.data_quality.expectations    AS expectations
FROM event_log(TABLE(bootcamp_lab.nyc_taxi.silver_trips))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC;

-- COMMAND ----------

-- MAGIC %md ## 3. Files are governed too: query the Volume directly

-- COMMAND ----------

LIST '/Volumes/bootcamp_lab/nyc_taxi/raw/trips/';

-- COMMAND ----------

SELECT * FROM read_files('/Volumes/bootcamp_lab/nyc_taxi/raw/trips/batch_02/', format => 'csv', header => true)
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Access control: give analysts read access to the gold layer only
-- MAGIC A reader needs `USE CATALOG` + `USE SCHEMA` + `SELECT`.

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG bootcamp_lab TO `account users`;
GRANT USE SCHEMA ON SCHEMA bootcamp_lab.nyc_taxi TO `account users`;
GRANT SELECT ON TABLE bootcamp_lab.nyc_taxi.gold_daily_kpis TO `account users`;

SHOW GRANTS ON TABLE bootcamp_lab.nyc_taxi.gold_daily_kpis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Lineage
-- MAGIC Open **Catalog → bootcamp_lab → nyc_taxi → gold_daily_kpis → Lineage → See lineage graph**:
-- MAGIC Volume files → bronze_trips → silver_trips → gold_daily_kpis, captured automatically.
