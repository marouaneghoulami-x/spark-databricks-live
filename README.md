# Apache Spark & Databricks: Live Session

Material for the **DataMaroc Bootcamps** live (Facebook · LinkedIn · YouTube), Sunday 1 Nov 2026, 19:00 UTC.
Speakers: **Marouane Ghoulami** (Senior Data Engineer), **Mohammed Errakho** (Senior Data Engineer · Tech Lead).

| Folder | What's inside |
|---|---|
| [`slides/`](slides/) | The presentation (17 slides, speaker notes included) |
| [`lab/`](lab/) | The hands-on lab: notebooks + the Lakeflow pipeline |
| [`cover/`](cover/) | Announcement visuals (16:9 and 4:5) |

## The plan (~1h30)

| Part | Time | What |
|---|---|---|
| Concepts | ~35 min | 15 slides: why Spark (Hadoop was too slow), architecture, how code executes, Databricks, Unity Catalog, Lakeflow |
| Live lab | ~45 min | Build the pipeline in Databricks, with 3 audience challenges |
| Q&A | rest | |

## The lab

**Problem:** the NYC taxi operations team gets raw trip files every day and wants, per day: trips, revenue,
average fare, plus the busiest pickup zones. Some rows are dirty, and analysts should only see the final tables.

**Data:** `samples.nyctaxi.trips`, built into every Databricks workspace. `00_setup` exports it as CSV files into a
Unity Catalog Volume to simulate daily file drops, so there's nothing to download.

```
/Volumes/bootcamp_lab/nyc_taxi/raw/trips/*.csv
        │  Auto Loader
        ▼
bronze_trips (streaming table) ─► silver_trips (streaming table + expectations)
                                        ├─► gold_daily_kpis       (materialized view)
                                        └─► gold_top_pickup_zips  (materialized view)
All objects live in Unity Catalog: bootcamp_lab.nyc_taxi
```

### Run it

Import this repo into your workspace (**Workspace → Create → Git folder**, paste the repo URL), then:

1. **`lab/00_setup`**: creates the catalog `bootcamp_lab`, schema `nyc_taxi` and volume `raw`, then lands batch 01 of the files.
   *No permission to create a catalog? Use an existing one (e.g. `workspace`) and update the name in the notebooks and the pipeline file.*
2. **`lab/01_spark_under_the_hood`**: shows lazy evaluation, `explain()`, shuffles, jobs and stages.
3. **Create the pipeline**: **Jobs & Pipelines → Create → ETL pipeline**
   - Default catalog: `bootcamp_lab`, default schema: `nyc_taxi`
   - Source code: the notebook `lab/pipeline/nyc_taxi_pipeline` (add it with **Add existing assets**, or set it in **Settings → Source code**). Don't run its cells yourself: the pipeline runs it.
   - Serverless compute → **Run pipeline**
4. **`lab/02_new_data_arrives`**: lands batch 02 (the last 7 days + 3 dirty rows). **Run the pipeline again**:
   only the new files are read, and the dirty rows are dropped (see the *Data quality* tab of `silver_trips`).
5. **`lab/03_unity_catalog`**: query gold, check expectations in the event log, read the volume, `GRANT`, open the lineage graph.

**Live challenges**: each notebook has a "guess before you run" moment for the audience, plus a bonus
"break it on purpose" step (`expect_or_fail`) in `02_new_data_arrives`.

Everything runs on serverless compute, so it works with the free **Databricks Free Edition**.
