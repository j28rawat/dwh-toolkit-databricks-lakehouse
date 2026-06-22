# Phase 2 — Storage Layer Deep Dive (cross-cutting, not tied to a book chapter)

This is the "Databricks internals" phase that justifies doing this project on
Databricks instead of just reading the book. Built before the industry case
studies so every later chapter can use this knowledge instead of treating
`CREATE TABLE` as a black box.

Planned topics (days added one at a time as we reach them):
- Delta Lake transaction log (`_delta_log`), ACID guarantees, `DESCRIBE HISTORY`
- Time travel, `VACUUM`, `OPTIMIZE`, Z-ORDER, Liquid Clustering
- Deletion vectors, Change Data Feed (CDF) — natural fit for SCD Type 2
- Apache Iceberg table format: manifests, snapshots, catalog behavior
- Delta vs. Iceberg: same logical table, two formats, compare metadata
- Lakebase (serverless Postgres): when row-store beats column-store for
  dimension lookups, OLTP-style point updates vs. lakehouse batch merges
- Choosing engine per Kimball technique (e.g., why SCD Type 2 merges are
  natural in Delta but mini-dimension point-lookups may favor Lakebase)
