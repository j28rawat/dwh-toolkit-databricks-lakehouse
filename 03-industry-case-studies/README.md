# Phase 3 — Industry Case Studies (Book Ch 3-16), Consolidated Approach

Split into two tracks based on whether the chapter introduces a genuinely
new Kimball technique or mostly recombines techniques from an earlier
chapter in new industry vocabulary. See root README for the reasoning.

## Deep track — full 3-engine treatment (Delta → Iceberg → Lakebase)

Each gets its own `NN-domain-name/` subfolder with day folders for schema
design, Delta build, Iceberg port, Lakebase port (~3 sessions each).

| # | Chapter | New technique |
|---|---|---|
| 3 | Retail Sales | Foundational star schema, transaction grain |
| 4 | Inventory | Periodic & accumulating snapshot fact tables |
| 5 | Procurement | Slowly changing dimensions in practice |
| 6 | Order Management | Header/line fact tables, multiple fact granularities |
| 9 | HR Management | Multi-valued dimensions, point-in-time effective dating |
| 13 | Education | Bridge tables, many-to-many relationships |
| 14 | Healthcare | Type 6/7 SCDs, complex bridge tables |

## Fast-pass track — single session, applied practice, no new notebook build

One `notes.md` per chapter (in a shared `fast-pass/` subfolder): read the
chapter, name which earlier technique(s) it reuses, note what's genuinely
domain-specific about the model, and capture any small wrinkle worth
remembering. No new Delta/Iceberg/Lakebase build — the point is recognizing
the pattern, not re-typing it.

| # | Chapter | Reuses pattern from |
|---|---|---|
| 7 | Accounting | Periodic snapshot (Ch.4) + year-to-date facts |
| 8 | CRM | SCD Types (Ch.5) + behavior tagging |
| 10 | Financial Services | SCD Types (Ch.5), mini-dimension variant |
| 11 | Telecommunications | Bridge tables (Ch.13), hierarchy handling |
| 12 | Transportation | Accumulating snapshot (Ch.4) + date/time dimension nuances |
| 15 | Electronic Commerce | Factless fact tables, transaction grain (Ch.3) |
| 16 | Insurance | SCD Types (Ch.5) + allocations |

Total Phase 3: 7 × ~3 sessions + 7 × 1 session = **26-28 sessions**
(vs. 40-45 under full equal depth).
