# Day 01 — DW/BI, Business Intelligence, and Dimensional Modeling Primer

**Book:** Chapter 1
**Time budget:** ~15 min theory / ~45 min hands-on

## Theory recap

### Two worlds: data capture vs. data analysis
Operational (OLTP) systems are optimized for fast, reliable transaction
capture — heavily normalized, one record touched at a time. Analytical (DW/BI)
systems are optimized for flexible, fast querying across millions of rows —
denormalized, read-heavy. Trying to serve analytics directly off OLTP schemas
is the root cause of most "the reports are slow" complaints.

### Star schema vs. OLAP cube
A **star schema** is a relational implementation of a dimensional model: one
central **fact table** surrounded by **dimension tables**, joined on keys.
An **OLAP cube** is a multidimensional (often pre-aggregated) structure built
*from* a star schema for fast slice-and-dice. The star schema is the
foundation either way — this project builds star schemas directly as tables.

### Fact tables
Store **measurements** (numeric, mostly additive) at a specific **grain**
(the level of detail one row represents — e.g., one row per line item per
order). Every fact table technique in this book is a variation on: what's
the grain, and which numbers are additive across which dimensions.

### Dimension tables
Store **descriptive context** — the who/what/where/when/why/how around a
measurement. Wide, denormalized, full of text attributes used for filtering,
grouping, and labeling. A dimension table's surrogate key (not its natural/
business key) is what the fact table references.

### Kimball DW/BI architecture (the "restaurant metaphor")
- **Operational source systems** = the *farms* (where raw ingredients come from)
- **ETL system** = the *kitchen* (back room — cleans, conforms, combines)
- **Presentation area** (the star schemas) = the *dining room display case* —
  must be query-ready, dimensional, never just a copy of source-system tables
- **BI applications** = what the *diners* (end users) actually interact with

The architectural rule worth internalizing now: the presentation area must
always be dimensional, and must never require users to understand the
original source system's structure.

### Alternative architectures (context, not the path we're taking)
- **Independent data marts** — fast but uncoordinated, integration nightmare
- **Hub-and-spoke (Inmon/Corporate Information Factory)** — normalized
  enterprise data warehouse feeding downstream dimensional marts
- **Hybrid hub-and-spoke + Kimball** — normalized staging feeding a single
  dimensional presentation layer (closest to what most lakehouse
  medallion-architecture setups look like today — worth noting since this
  is functionally what Databricks' bronze/silver/gold pattern implements)

### Five dimensional modeling myths debunked
Dimensional models are *not* inherently summary-only, departmental-only,
unscalable, or limited to predictable usage, and they integrate fine across
the enterprise *when built on conformed dimensions* (Day 04 topic).

## Why this matters for the Databricks track
The medallion architecture (bronze/silver/gold) you'll see throughout
Databricks docs is a direct descendant of this restaurant metaphor: bronze
≈ raw capture, silver ≈ cleaned/conformed, gold ≈ the dimensional
presentation layer. Today's hands-on builds the gold-layer star schema
directly to get the shape right before we worry about the ETL plumbing
(that's Phase 5).
