# Databricks notebook source
# MAGIC %md
# MAGIC # Day 01 — Hands-on: Minimal Star Schema in Delta Lake
# MAGIC
# MAGIC Goal: build the smallest possible star schema (one fact, three
# MAGIC dimensions) to make the Ch.1 vocabulary concrete, and take a first
# MAGIC look at the Delta transaction log (full deep dive in Phase 2).
# MAGIC
# MAGIC Grain of the fact table: **one row per product sold, per store, per day.**

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %sql
# MAGIC -- dwh_toolkit catalog must be pre-created via the UI (programmatic catalog creation
# MAGIC -- requires a MANAGED LOCATION in this workspace's Unity Catalog configuration).
# MAGIC CREATE SCHEMA IF NOT EXISTS dwh_toolkit.foundations;
# MAGIC USE CATALOG dwh_toolkit;
# MAGIC USE SCHEMA foundations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimension tables
# MAGIC
# MAGIC Note the **surrogate keys** (`*_key`, simple integers, meaningless
# MAGIC outside this warehouse) vs. the **natural/business keys**
# MAGIC (`product_code`, `store_code`) that come from the source system.
# MAGIC This separation is what makes SCD Type 2 possible later (Day 05) —
# MAGIC the surrogate key can have multiple versions per natural key.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dim_product (
# MAGIC   product_key     BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   product_code    STRING NOT NULL,   -- natural key from source
# MAGIC   product_name    STRING,
# MAGIC   category        STRING,
# MAGIC   brand           STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_store (
# MAGIC   store_key       BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   store_code      STRING NOT NULL,
# MAGIC   store_name      STRING,
# MAGIC   region          STRING,
# MAGIC   state           STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dim_date (
# MAGIC   date_key        INT NOT NULL,      -- yyyymmdd, a deliberate exception
# MAGIC                                      -- to "surrogate keys are meaningless"
# MAGIC                                      -- — date dims commonly use a smart key
# MAGIC   full_date       DATE NOT NULL,
# MAGIC   day_of_week     STRING,
# MAGIC   month_name      STRING,
# MAGIC   quarter         INT,
# MAGIC   year            INT
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dim_product (product_code, product_name, category, brand) VALUES
# MAGIC   ('P001', 'Espresso Beans 1kg', 'Coffee', 'RoastCo'),
# MAGIC   ('P002', 'Oat Milk 1L',        'Dairy Alt', 'OatBarn'),
# MAGIC   ('P003', 'Ceramic Mug',        'Merchandise', 'HouseBrand');
# MAGIC
# MAGIC INSERT INTO dim_store (store_code, store_name, region, state) VALUES
# MAGIC   ('S01', 'Downtown Edmonton', 'Prairies', 'AB'),
# MAGIC   ('S02', 'West End Calgary',  'Prairies', 'AB');
# MAGIC
# MAGIC INSERT INTO dim_date VALUES
# MAGIC   (20260617, DATE'2026-06-17', 'Wednesday', 'June', 2, 2026),
# MAGIC   (20260618, DATE'2026-06-18', 'Thursday',  'June', 2, 2026),
# MAGIC   (20260619, DATE'2026-06-19', 'Friday',    'June', 2, 2026);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fact table
# MAGIC
# MAGIC References dimensions by **surrogate key only**. `quantity_sold` and
# MAGIC `sales_amount` are **additive facts** — safe to SUM across any
# MAGIC dimension. That additivity is the entire point of getting the grain
# MAGIC right.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_sales (
# MAGIC   date_key         INT    NOT NULL,
# MAGIC   product_key      BIGINT NOT NULL,
# MAGIC   store_key        BIGINT NOT NULL,
# MAGIC   quantity_sold    INT,
# MAGIC   sales_amount     DECIMAL(10,2)
# MAGIC ) USING DELTA;

# COMMAND ----------

# Pull generated surrogate keys, then load facts referencing them —
# mirrors how an ETL job would resolve natural keys to surrogate keys.
products = spark.sql("SELECT product_key, product_code FROM dim_product").collect()
stores = spark.sql("SELECT store_key, store_code FROM dim_store").collect()

p_key = {r.product_code: r.product_key for r in products}
s_key = {r.store_code: r.store_key for r in stores}

print(p_key, s_key)

# COMMAND ----------

# DBTITLE 1,Cell 9
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DecimalType
from decimal import Decimal

schema = StructType([
    StructField("date_key",      IntegerType(), False),
    StructField("product_key",   LongType(),    False),
    StructField("store_key",     LongType(),    False),
    StructField("quantity_sold", IntegerType(), True),
    StructField("sales_amount",  DecimalType(10, 2), True),
])

rows = [
    Row(date_key=20260617, product_key=p_key['P001'], store_key=s_key['S01'], quantity_sold=12, sales_amount=Decimal('215.88')),
    Row(date_key=20260617, product_key=p_key['P002'], store_key=s_key['S01'], quantity_sold=8,  sales_amount=Decimal('39.92')),
    Row(date_key=20260618, product_key=p_key['P003'], store_key=s_key['S02'], quantity_sold=3,  sales_amount=Decimal('44.97')),
    Row(date_key=20260619, product_key=p_key['P001'], store_key=s_key['S02'], quantity_sold=20, sales_amount=Decimal('359.80')),
]
spark.createDataFrame(rows, schema).write.mode("append").saveAsTable("fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The star join — point of the whole exercise
# MAGIC
# MAGIC One query, four tables, additive facts summed at whatever grain the
# MAGIC `GROUP BY` asks for. This is what "queryable without knowing the
# MAGIC source system" (the restaurant metaphor's dining room) looks like.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.month_name,
# MAGIC   p.category,
# MAGIC   st.region,
# MAGIC   SUM(f.quantity_sold) AS total_units,
# MAGIC   SUM(f.sales_amount)  AS total_revenue
# MAGIC FROM fact_sales f
# MAGIC JOIN dim_date    d  ON f.date_key    = d.date_key
# MAGIC JOIN dim_product p  ON f.product_key = p.product_key
# MAGIC JOIN dim_store   st ON f.store_key   = st.store_key
# MAGIC GROUP BY d.month_name, p.category, st.region
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. First peek at the Delta transaction log
# MAGIC
# MAGIC Every `CREATE`, `INSERT`, `MERGE` is a versioned, atomic commit —
# MAGIC this is the ACID guarantee that makes Delta a real database storage
# MAGIC format and not "just files." Full internals (the `_delta_log` JSON
# MAGIC files, checkpoints, `OPTIMIZE`, time travel) are Phase 2 — this is
# MAGIC just enough to see it's there.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Takeaways for `notes.md`
# MAGIC - Grain is decided *before* anything else — it determines what's additive.
# MAGIC - Surrogate keys decouple the warehouse from source-system key changes.
# MAGIC - The star join pattern (1 fact + N dimension joins) is the same
# MAGIC   regardless of how many millions of rows are in `fact_sales` — that
# MAGIC   scalability is exactly what Myth #3 (Ch.1) was debunking.
# MAGIC - `DESCRIBE HISTORY` is our first hook into the storage layer we'll
# MAGIC   go deep on in Phase 2.
