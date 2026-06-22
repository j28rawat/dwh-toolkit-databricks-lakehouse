# Day 00 — Environment Setup

One-time setup. Not part of the daily 1-hour cadence — budget extra time for this.

## 1. Unity Catalog: catalog & schema

In a Databricks SQL editor or notebook:

```sql
CREATE CATALOG IF NOT EXISTS dwh_toolkit;

CREATE SCHEMA IF NOT EXISTS dwh_toolkit.foundations;
CREATE SCHEMA IF NOT EXISTS dwh_toolkit.storage_layer;
CREATE SCHEMA IF NOT EXISTS dwh_toolkit.retail_sales;
CREATE SCHEMA IF NOT EXISTS dwh_toolkit.inventory;
-- one schema per industry chapter folder, added as we reach it
```

Convention used throughout this repo: `dwh_toolkit.<chapter_slug>.<table_name>`,
with a `_delta`, `_iceberg`, or `_lakebase` suffix on the table name where the
same logical table is built on more than one engine for comparison, e.g.
`dwh_toolkit.retail_sales.fact_sales_delta` vs `..._iceberg`.

## 2. Cluster / SQL Warehouse

- Use a cluster on a recent Databricks Runtime (LTS, Photon enabled) for the
  Delta Lake and Iceberg notebooks.
- A serverless SQL warehouse works fine for most chapters; switch to a
  dedicated cluster only when we get to Phase 2 (storage internals — we'll
  want `DESCRIBE DETAIL`, file-level inspection, etc.).

## 3. Iceberg access

Two paths, we'll use both at different points:
- **Delta Lake UniForm** (Iceberg-compatible metadata generated alongside
  Delta) — zero extra infra, good for the "same table, two readers" comparison.
- **Native Iceberg tables** via a REST catalog — gives a true side-by-side
  of catalog/metadata behavior. Confirm with your workspace admin whether a
  REST catalog (e.g. via Unity Catalog's Iceberg REST endpoint) is already
  available on your Enterprise workspace before Phase 2.

## 4. Lakebase (serverless Postgres)

Lakebase availability/region support should be confirmed directly in your
workspace (Compute → Lakebase / Database instances) since this varies by
workspace tier and region. Provision one instance now:

- Workspace → **Compute** → **Database Instances** → **Create**
- Note the connection string/host — we'll connect to it from notebooks via
  `psycopg2` or the Databricks-native Lakebase connector.

If Lakebase isn't visible in your workspace, flag it before Phase 2 — the
curriculum will need to substitute a regular Postgres-on-Databricks-SQL
comparison instead, and that's a real (not silent) scope change worth a note
in `PROGRESS_LOG.md`.

## 5. Repo

```bash
# from inside this folder, after git init / clone of your new empty repo
git init
git add .
git commit -m "Day 00: repo scaffold + setup notes"
git branch -M main
git remote add origin https://github.com/<your-username>/dwh-toolkit-databricks-lakehouse.git
git push -u origin main
```

Use a new commit per day going forward, message format:
`Day NN: <topic>` — keeps history reviewable chapter by chapter.
