# The Data Warehouse Toolkit — Implemented on Databricks (Delta Lake / Iceberg / Lakebase)

Practical, day-by-day implementation of every technique in *The Data Warehouse
Toolkit, 3rd Edition* (Kimball & Ross), built on Databricks across three
storage engines — **Delta Lake**, **Apache Iceberg**, and **Lakebase
(serverless Postgres)** — to build a deep, hands-on understanding of the
Databricks lakehouse storage layer alongside classic dimensional modeling
theory.

## How this repo works

- **One folder per day**, named `day-NN-topic-slug/`, inside the relevant
  phase folder. Each day folder is **never edited or overwritten** once
  committed — it's a permanent record of what was built that session.
  Later corrections or extensions go in a *new* day folder that references
  the earlier one.
- `PROGRESS_LOG.md` is append-only — one entry per session.
- Phase-level `README.md` files and the checklist below are the only files
  that get updated as we go (to check off progress) — everything inside a
  `day-NN-*` folder is frozen history.
- Each day folder contains:
  - `notes.md` — theory recap (book chapter/page references, key
    definitions, the "why")
  - `notebook.py` — Databricks notebook source format (`.py`), importable
    directly into a Databricks workspace
- Session format: ~15 min theory / ~45 min hands-on, 1 hour/day.

## Prerequisites

- Databricks Enterprise workspace (Unity Catalog enabled)
- A cluster or serverless SQL warehouse with access to create catalogs/schemas
- Lakebase (Postgres) instance provisioning rights — see `00-setup/`
- Git + GitHub repo (this one)

## Roadmap (consolidated approach — locked in)

Decision: **deep-dive the chapters that introduce a new technique, fast/
applied-practice pass on chapters that recombine earlier techniques in a new
industry vocabulary.** This was a deliberate trade — see
`03-industry-case-studies/README.md` for the full reasoning and the per-
chapter bucket assignment. Net effect: ~14-18 fewer sessions on Phase 3,
reinvested into Phase 2 (storage internals) and Phase 5 (ETL subsystems),
which is where the actual Delta/Iceberg/Lakebase depth this project is for
actually lives.

| Phase | Folder | Book Chapters | Est. Sessions | Status |
|---|---|---|---|---|
| 0 | `00-setup/` | — | 1 | ✅ Done |
| 1 | `01-foundations/` | Ch 1-2 | 6-8 | 🔄 In progress (Day 01 done) |
| 2 | `02-storage-layer-deep-dive/` | — (cross-cutting: Delta internals, Iceberg, Lakebase) | 11-13 | ⬜ Not started |
| 3 | `03-industry-case-studies/` | Ch 3-16 (7 deep + 7 fast-pass) | 26-28 | ⬜ Not started |
| 4 | `04-lifecycle-and-process/` | Ch 17-18 | 2-3 | ⬜ Not started |
| 5 | `05-etl-subsystems/` | Ch 19-20 | 12-14 | ⬜ Not started |
| 6 | `06-big-data-analytics/` | Ch 21 (+ modern lakehouse update) | 3 | ⬜ Not started |
| 7 | `07-capstone/` | Bus matrix, end-to-end multi-domain mini warehouse | 4-5 | ⬜ Not started |
| | | **Total** | **65-74 sessions** | 2 done |

At 1hr/day: **~10-12 weeks at 6-7 sessions/week, ~13-15 weeks at 5/week.**
(For reference: the full-equal-depth path this replaced would have run
~75-90 sessions, ~3+ months.)

**Why Phase 2 sits before the industry case studies:** Chapters 3-16 will
each be implemented in Delta, Iceberg, and Lakebase. Doing the storage-layer
internals deep dive first means every subsequent chapter applies that
knowledge instead of hand-waving "just create a table."

## Industry case-study chapters (Phase 3 detail)

| # | Chapter | New technique(s) introduced |
|---|---|---|
| 3 | Retail Sales | Foundational star schema, transaction grain |
| 4 | Inventory | Periodic & accumulating snapshot fact tables |
| 5 | Procurement | Slowly changing dimensions in practice |
| 6 | Order Management | Header/line fact tables, multiple fact granularities |
| 7 | Accounting | Year-to-date facts, multiple currencies |
| 8 | CRM | Complex customer dimension, behavior tags |
| 9 | HR Management | Multi-valued dimensions, point-in-time effective dating |
| 10 | Financial Services | Mini-dimensions, hot-swappable attributes |
| 11 | Telecommunications | Geographic/network hierarchies |
| 12 | Transportation | Date/time dimension nuances, multiple time zones |
| 13 | Education | Bridge tables, many-to-many relationships |
| 14 | Healthcare | Type 6/7 SCDs, complex bridge tables |
| 15 | Electronic Commerce | Clickstream/factless fact tables |
| 16 | Insurance | Audit/lineage, allocations |

Each chapter session set: design the star schema → build in Delta Lake →
port to Iceberg (compare metadata/catalog behavior) → port relevant
dimensions to Lakebase (compare OLTP vs OLAP fit).

## If the pace still feels slow

The remaining lever is Phase 3's 7 "deep" chapters — if needed later, the
last 2-3 (Healthcare, Education) could drop to a 2-session treatment
(Delta + a comparison writeup instead of a full Iceberg+Lakebase port)
without losing technique coverage, since by that point the engine-specific
patterns (MERGE-based SCDs, bridge table joins) will already be well
exercised. Not applied yet — flag it if/when it's actually needed.
