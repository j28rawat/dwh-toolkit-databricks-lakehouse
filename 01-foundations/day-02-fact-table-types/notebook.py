# Databricks notebook source
# MAGIC %md
# MAGIC # Day 02 — The Four Fact Table Types in Delta Lake
# MAGIC
# MAGIC We build all four fact table types in a single schema, sharing the
# MAGIC dimension tables from Day 01. Focus: see the INSERT vs MERGE write
# MAGIC patterns side by side, and query semi-additive facts correctly.
# MAGIC
# MAGIC **Grain decisions (fill in the sentence before each DDL block):**
# MAGIC - Transaction: *one row per product sold per store per day*
# MAGIC - Periodic Snapshot: *one row per product per store per week*
# MAGIC - Accumulating Snapshot: *one row per customer order, cradle to grave*
# MAGIC - Factless: *one row per student per class per day they attended*

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dwh_toolkit;
# MAGIC USE SCHEMA foundations;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TYPE 1 — Transaction Fact Table
# MAGIC
# MAGIC Pure INSERT. Immutable. The receipt is printed and never touched again.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_sales_transaction (
# MAGIC   date_key        INT     NOT NULL,
# MAGIC   product_key     BIGINT  NOT NULL,
# MAGIC   store_key       BIGINT  NOT NULL,
# MAGIC   quantity_sold   INT,
# MAGIC   unit_price      DECIMAL(10,2),
# MAGIC   sales_amount    DECIMAL(10,2)   -- quantity_sold * unit_price, pre-computed
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Grain: one row per product sold per store per calendar day';

# COMMAND ----------

# Resolve surrogate keys (same pattern you'd use in a real ETL pipeline)
p = {r.product_code: r.product_key
     for r in spark.sql("SELECT product_key, product_code FROM dim_product").collect()}
s = {r.store_code: r.store_key
     for r in spark.sql("SELECT store_key, store_code FROM dim_store").collect()}
print("Product keys:", p)
print("Store keys:", s)

# COMMAND ----------

# DBTITLE 1,Cell 6
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, DecimalType
from pyspark.sql.functions import col

# Explicit schema: INT/LONG for integers (avoids Long-vs-INT mismatch).
# Decimal columns use DoubleType here — Arrow conversion on Serverless requires
# decimal.Decimal objects for DecimalType; floats are cast after DataFrame creation.
_schema = StructType([
    StructField("date_key",      IntegerType(), False),
    StructField("product_key",   LongType(),    False),
    StructField("store_key",     LongType(),    False),
    StructField("quantity_sold", IntegerType(), True),
    StructField("unit_price",    DoubleType(),  True),
    StructField("sales_amount",  DoubleType(),  True),
])

# Each row = one atomic transaction event. Note: immutable after insert.
transaction_rows = [
    Row(date_key=20260617, product_key=p['P001'], store_key=s['S01'],
        quantity_sold=12, unit_price=17.99, sales_amount=215.88),
    Row(date_key=20260617, product_key=p['P002'], store_key=s['S01'],
        quantity_sold=8,  unit_price=4.99,  sales_amount=39.92),
    Row(date_key=20260618, product_key=p['P001'], store_key=s['S02'],
        quantity_sold=5,  unit_price=17.99, sales_amount=89.95),
    Row(date_key=20260619, product_key=p['P003'], store_key=s['S02'],
        quantity_sold=3,  unit_price=14.99, sales_amount=44.97),
]
(spark.createDataFrame(transaction_rows, schema=_schema)
     .withColumn("unit_price",  col("unit_price").cast(DecimalType(10, 2)))
     .withColumn("sales_amount", col("sales_amount").cast(DecimalType(10, 2)))
     .write.mode("append").saveAsTable("fact_sales_transaction"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fully additive query — SUM across any dimension combination is safe
# MAGIC SELECT
# MAGIC   p.category,
# MAGIC   SUM(f.quantity_sold)  AS total_units,
# MAGIC   SUM(f.sales_amount)   AS total_revenue
# MAGIC FROM fact_sales_transaction f
# MAGIC JOIN dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TYPE 2 — Periodic Snapshot Fact Table
# MAGIC
# MAGIC One row per product per store per week — written at period end
# MAGIC regardless of whether any transactions occurred that week.
# MAGIC
# MAGIC **Semi-additive trap:** `quantity_on_hand` can be SUMMed across stores
# MAGIC (total stock in all stores), but CANNOT be SUMMed across weeks (summing
# MAGIC 4 weeks of balances gives you a fictional number, not a real one).
# MAGIC Use AVG or a point-in-time filter when querying across weeks.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_inventory_snapshot (
# MAGIC   week_key           INT     NOT NULL,  -- yyyyWww, e.g. 2026W25
# MAGIC   product_key        BIGINT  NOT NULL,
# MAGIC   store_key          BIGINT  NOT NULL,
# MAGIC   quantity_on_hand   INT,               -- SEMI-ADDITIVE (no SUM across time)
# MAGIC   quantity_on_order  INT,               -- SEMI-ADDITIVE
# MAGIC   quantity_sold_wtd  INT,               -- FULLY ADDITIVE (weekly delta, not balance)
# MAGIC   revenue_wtd        DECIMAL(10,2)      -- FULLY ADDITIVE
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Grain: one row per product per store per calendar week';

# COMMAND ----------

# DBTITLE 1,Cell 10
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, DecimalType
from pyspark.sql.functions import col

# Simulating two snapshot loads — Week 25 and Week 26.
# In a real pipeline: scheduled job runs every Sunday, loads all products × stores.
# Rows are appended (never updated) — if balance doesn't change, you still write
# the same value; the period row must exist for every period.

_schema = StructType([
    StructField("week_key",          IntegerType(), False),
    StructField("product_key",       LongType(),    False),
    StructField("store_key",         LongType(),    False),
    StructField("quantity_on_hand",  IntegerType(), True),
    StructField("quantity_on_order", IntegerType(), True),
    StructField("quantity_sold_wtd", IntegerType(), True),
    StructField("revenue_wtd",       DoubleType(),  True),
])

snapshot_rows = [
    # Week 25 snapshot
    Row(week_key=202625, product_key=p['P001'], store_key=s['S01'],
        quantity_on_hand=120, quantity_on_order=0,  quantity_sold_wtd=12, revenue_wtd=215.88),
    Row(week_key=202625, product_key=p['P002'], store_key=s['S01'],
        quantity_on_hand=45,  quantity_on_order=100, quantity_sold_wtd=8,  revenue_wtd=39.92),
    Row(week_key=202625, product_key=p['P001'], store_key=s['S02'],
        quantity_on_hand=80,  quantity_on_order=0,  quantity_sold_wtd=5,  revenue_wtd=89.95),
    # Week 26 snapshot (P002 at S01 restocked; P001 at S02 unchanged)
    Row(week_key=202626, product_key=p['P001'], store_key=s['S01'],
        quantity_on_hand=108, quantity_on_order=0,  quantity_sold_wtd=0,  revenue_wtd=0.00),
    Row(week_key=202626, product_key=p['P002'], store_key=s['S01'],
        quantity_on_hand=137, quantity_on_order=0,  quantity_sold_wtd=3,  revenue_wtd=14.97),
    Row(week_key=202626, product_key=p['P001'], store_key=s['S02'],
        quantity_on_hand=75,  quantity_on_order=0,  quantity_sold_wtd=5,  revenue_wtd=89.95),
]
(spark.createDataFrame(snapshot_rows, schema=_schema)
     .withColumn("revenue_wtd", col("revenue_wtd").cast(DecimalType(10, 2)))
     .write.mode("append").saveAsTable("fact_inventory_snapshot"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CORRECT: current week's on-hand balance across stores (SUM across stores = fine)
# MAGIC SELECT
# MAGIC   p.product_name,
# MAGIC   SUM(f.quantity_on_hand) AS total_on_hand_all_stores
# MAGIC FROM fact_inventory_snapshot f
# MAGIC JOIN dim_product p ON f.product_key = p.product_key
# MAGIC WHERE f.week_key = 202626   -- point-in-time filter required for semi-additive facts
# MAGIC GROUP BY p.product_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- WRONG (but won't error — Delta just returns incorrect numbers silently):
# MAGIC -- Summing on-hand across weeks = fictional total. Run it so you SEE the wrong answer.
# MAGIC SELECT
# MAGIC   p.product_name,
# MAGIC   SUM(f.quantity_on_hand) AS WRONG_total  -- adds up both weeks = meaningless
# MAGIC FROM fact_inventory_snapshot f
# MAGIC JOIN dim_product p ON f.product_key = p.product_key
# MAGIC GROUP BY p.product_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TYPE 3 — Accumulating Snapshot Fact Table
# MAGIC
# MAGIC One row per order. The row is CREATED when the order is placed and
# MAGIC UPDATED at every milestone. This is the only Kimball fact table type
# MAGIC that uses MERGE (upsert) instead of INSERT.
# MAGIC
# MAGIC Notice: multiple date foreign keys (one per milestone), and pre-computed
# MAGIC lag columns so users don't have to calculate durations themselves.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_order_pipeline (
# MAGIC   order_id             STRING  NOT NULL,   -- natural key / degenerate dimension
# MAGIC   customer_key         BIGINT,
# MAGIC   product_key          BIGINT  NOT NULL,
# MAGIC   -- One date_key column per milestone; starts NULL, filled as pipeline progresses
# MAGIC   date_ordered_key     INT,
# MAGIC   date_payment_key     INT,
# MAGIC   date_shipped_key     INT,
# MAGIC   date_delivered_key   INT,
# MAGIC   -- Pre-computed lag facts (days between milestones)
# MAGIC   days_to_payment      INT,
# MAGIC   days_to_ship         INT,
# MAGIC   days_to_deliver      INT,
# MAGIC   -- Status for easy filtering
# MAGIC   pipeline_status      STRING   -- 'ordered','paid','shipped','delivered'
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Grain: one row per customer order, from placement to delivery';

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from pyspark.sql.functions import col, lit, datediff, to_date

_schema = StructType([
    StructField("order_id",          StringType(),  False),
    StructField("customer_key",      LongType(),    True),
    StructField("product_key",       LongType(),    False),
    StructField("date_ordered_key",  IntegerType(), True),
    StructField("date_payment_key",  IntegerType(), True),
    StructField("date_shipped_key",  IntegerType(), True),
    StructField("date_delivered_key",IntegerType(), True),
    StructField("days_to_payment",   IntegerType(), True),
    StructField("days_to_ship",      IntegerType(), True),
    StructField("days_to_deliver",   IntegerType(), True),
    StructField("pipeline_status",   StringType(),  True),
])

# LOAD 1: New orders arrive — rows are created with mostly NULL milestones
initial_orders = [
    Row(order_id='ORD-001', customer_key=1001, product_key=p['P001'],
        date_ordered_key=20260617, date_payment_key=None,
        date_shipped_key=None,    date_delivered_key=None,
        days_to_payment=None, days_to_ship=None, days_to_deliver=None,
        pipeline_status='ordered'),
    Row(order_id='ORD-002', customer_key=1002, product_key=p['P002'],
        date_ordered_key=20260617, date_payment_key=None,
        date_shipped_key=None,    date_delivered_key=None,
        days_to_payment=None, days_to_ship=None, days_to_deliver=None,
        pipeline_status='ordered'),
]
(spark.createDataFrame(initial_orders, schema=_schema)
     .write.mode("append").saveAsTable("fact_order_pipeline"))

spark.sql("SELECT order_id, pipeline_status, date_shipped_key FROM fact_order_pipeline").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LOAD 2: Payment confirmed for ORD-001 — we MERGE (update the existing row)
# MAGIC -- This is why Delta's MERGE operation is the natural write pattern for this table type.
# MAGIC MERGE INTO fact_order_pipeline AS target
# MAGIC USING (
# MAGIC   SELECT 'ORD-001'  AS order_id,
# MAGIC          20260618   AS date_payment_key,
# MAGIC          1          AS days_to_payment,
# MAGIC          'paid'     AS pipeline_status
# MAGIC ) AS update
# MAGIC ON target.order_id = update.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.date_payment_key = update.date_payment_key,
# MAGIC   target.days_to_payment  = update.days_to_payment,
# MAGIC   target.pipeline_status  = update.pipeline_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LOAD 3: ORD-001 shipped, ORD-002 payment confirmed — MERGE handles both rows
# MAGIC MERGE INTO fact_order_pipeline AS target
# MAGIC USING (
# MAGIC   SELECT 'ORD-001' AS order_id, 20260620 AS date_shipped_key,
# MAGIC          3 AS days_to_ship, 'shipped' AS pipeline_status
# MAGIC   UNION ALL
# MAGIC   SELECT 'ORD-002', NULL, NULL, 'paid'
# MAGIC ) AS updates
# MAGIC ON target.order_id = updates.order_id
# MAGIC WHEN MATCHED AND updates.pipeline_status = 'shipped' THEN UPDATE SET
# MAGIC   target.date_shipped_key = updates.date_shipped_key,
# MAGIC   target.days_to_ship     = updates.days_to_ship,
# MAGIC   target.pipeline_status  = updates.pipeline_status
# MAGIC WHEN MATCHED AND updates.pipeline_status = 'paid' THEN UPDATE SET
# MAGIC   target.date_payment_key = 20260619,
# MAGIC   target.days_to_payment  = 2,
# MAGIC   target.pipeline_status  = updates.pipeline_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Final state: see the pipeline — milestones filled in as they happen
# MAGIC SELECT order_id, pipeline_status,
# MAGIC        date_ordered_key, date_payment_key, date_shipped_key, date_delivered_key,
# MAGIC        days_to_payment, days_to_ship
# MAGIC FROM fact_order_pipeline
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Key insight: Delta's transaction log records every MERGE as a versioned commit.
# MAGIC -- You can time-travel back to see the row's state before payment was confirmed.
# MAGIC DESCRIBE HISTORY fact_order_pipeline;

# COMMAND ----------

# MAGIC %md
# MAGIC ## TYPE 4 — Factless Fact Table
# MAGIC
# MAGIC Two flavours built here:
# MAGIC - **Event capture:** records that a student attended a class session
# MAGIC - **Coverage table:** records which promotions a product is eligible for
# MAGIC   (so you can ask "which eligible products had ZERO sales?")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Flavour A: pure event log. No numeric columns — the row's existence IS the fact.
# MAGIC CREATE OR REPLACE TABLE fact_student_attendance (
# MAGIC   date_key       INT    NOT NULL,
# MAGIC   student_key    BIGINT NOT NULL,
# MAGIC   class_key      BIGINT NOT NULL,
# MAGIC   teacher_key    BIGINT NOT NULL
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Grain: one row per student per class per day attended. No numeric facts.';
# MAGIC
# MAGIC -- Flavour B: coverage/eligibility table. Records what COULD happen.
# MAGIC CREATE OR REPLACE TABLE fact_promotion_eligibility (
# MAGIC   date_key       INT    NOT NULL,
# MAGIC   product_key    BIGINT NOT NULL,
# MAGIC   promotion_key  BIGINT NOT NULL
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Grain: one row per product eligible for a promotion per day. Used to find zero-sales products.';

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

_att_schema = StructType([
    StructField("date_key",    IntegerType(), False),
    StructField("student_key", LongType(),    False),
    StructField("class_key",   LongType(),    False),
    StructField("teacher_key", LongType(),    False),
])

# Attendance events — simple insert, rows never change
attendance_rows = [
    Row(date_key=20260617, student_key=2001, class_key=301, teacher_key=401),
    Row(date_key=20260617, student_key=2002, class_key=301, teacher_key=401),
    Row(date_key=20260618, student_key=2001, class_key=302, teacher_key=402),
    # student 2002 absent on 20260618 — no row is written, that's the point
]
(spark.createDataFrame(attendance_rows, schema=_att_schema)
     .write.mode("append").saveAsTable("fact_student_attendance"))

_promo_schema = StructType([
    StructField("date_key",       IntegerType(), False),
    StructField("product_key",    LongType(),    False),
    StructField("promotion_key",  LongType(),    False),
])

# Promotion eligibility — every product eligible for promo P-SUMMER on these dates
promo_rows = [
    Row(date_key=20260617, product_key=p['P001'], promotion_key=9001),
    Row(date_key=20260617, product_key=p['P002'], promotion_key=9001),
    Row(date_key=20260617, product_key=p['P003'], promotion_key=9001),
]
(spark.createDataFrame(promo_rows, schema=_promo_schema)
     .write.mode("append").saveAsTable("fact_promotion_eligibility"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count attendance: "how many students attended class 301?" — COUNT(*), not SUM
# MAGIC SELECT class_key, COUNT(*) AS students_attended
# MAGIC FROM fact_student_attendance
# MAGIC WHERE date_key = 20260617
# MAGIC GROUP BY class_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The coverage table's killer query: which promoted products had ZERO sales?
# MAGIC -- Without fact_promotion_eligibility this question is unanswerable.
# MAGIC SELECT
# MAGIC   pe.product_key,
# MAGIC   p.product_name,
# MAGIC   COALESCE(SUM(fs.sales_amount), 0) AS sales_during_promo
# MAGIC FROM fact_promotion_eligibility pe
# MAGIC JOIN dim_product p ON pe.product_key = p.product_key
# MAGIC LEFT JOIN fact_sales_transaction fs
# MAGIC   ON pe.product_key = fs.product_key
# MAGIC   AND pe.date_key   = fs.date_key
# MAGIC WHERE pe.date_key = 20260617
# MAGIC GROUP BY pe.product_key, p.product_name
# MAGIC HAVING COALESCE(SUM(fs.sales_amount), 0) = 0;  -- only show zero-sellers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: the four tables, side by side
# MAGIC
# MAGIC | Table | Type | Rows loaded via | On-hand SUM across time? |
# MAGIC |---|---|---|---|
# MAGIC | `fact_sales_transaction` | Transaction | INSERT | ✅ Yes (fully additive) |
# MAGIC | `fact_inventory_snapshot` | Periodic Snapshot | INSERT | ❌ No (semi-additive) |
# MAGIC | `fact_order_pipeline` | Accumulating Snapshot | MERGE | N/A (use AVG on lags) |
# MAGIC | `fact_student_attendance` | Factless | INSERT | N/A (use COUNT) |
# MAGIC
# MAGIC **Key Delta Lake observation:** Run `DESCRIBE HISTORY fact_order_pipeline`
# MAGIC and compare to `DESCRIBE HISTORY fact_sales_transaction`. The transaction
# MAGIC table shows only INSERT operations. The accumulating snapshot shows MERGE
# MAGIC operations — one commit per milestone update batch. This is the storage-
# MAGIC layer fingerprint of each fact table type, which we'll go deep on in Phase 2.
