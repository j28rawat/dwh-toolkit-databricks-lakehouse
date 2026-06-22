# Phase 1 — Foundations (Book Ch 1-2)

Covers the conceptual vocabulary used for the rest of the project: star
schemas, fact/dimension table structure, the full Slowly Changing Dimension
taxonomy (Types 0-7), hierarchies, conformed dimensions, and the bus matrix.
Light on code, heavy on getting the mental model right — everything after
this phase assumes these terms are second nature.

| Day | Topic | Book ref |
|---|---|---|
| 01 | DW/BI primer, star schema vs. OLAP cube, Kimball architecture | Ch 1 |
| 02 | Fact table types: transaction, periodic snapshot, accumulating snapshot, factless | Ch 2 |
| 03 | Dimension table techniques: surrogate keys, degenerate dims, role-playing, junk dims | Ch 2 |
| 04 | Conformed dimensions & the enterprise bus matrix | Ch 2 |
| 05 | SCD Types 0-3 (retain, overwrite, add row, add attribute) | Ch 2 |
| 06 | SCD Types 4-7 (mini-dims, hybrid Type 6/7) | Ch 2 |
| 07 | Dimension hierarchies (fixed, ragged, bridge tables, pathstrings) | Ch 2 |
| 08 | Advanced fact/dimension techniques wrap-up | Ch 2 |

(Day 01 is built; Days 02-08 get added one at a time as we go — see root
`PROGRESS_LOG.md` for actual dates/status.)
