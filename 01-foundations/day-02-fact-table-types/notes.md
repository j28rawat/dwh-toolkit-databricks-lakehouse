# Day 02 — The Four Types of Fact Tables

**Book:** Chapter 2 (Fact Table Techniques section)
**Time budget:** ~15 min theory / ~45 min hands-on

---

## The core question every fact table must answer first: what is one row?

Before you type a single column name, you must decide what one row in your
fact table represents. This decision is called the **grain**. Everything else
— which columns go in, what you can and cannot SUM — flows from this one
decision. Getting the grain wrong is the single most common and costly
modeling mistake.

A useful test: complete this sentence before writing any DDL:
> *"One row in this fact table represents one ______."*

If you can't complete it cleanly, stop and go back to the business.

---

## The four fact table types — a plain English mental model

Think of a fact table as a camera taking a photo of your business. The four
types differ only in *when* the camera shoots and *how many times it fires*
per subject.

---

### 1. Transaction Fact Table — the "receipt" table

**Mental model:** A cash register receipt. One row is printed the moment a
transaction happens. Once printed, the receipt never changes.

**Grain:** One row per transaction event (a sale, a click, a payment, a call).

**Key characteristic:** Immutable. The row is written once and never updated.
If you sold 3 items in one order on one day, you get 3 rows — one per line
item sold. If nothing happened, there's no row for that day.

**What it answers:** *"What happened, when, for how much?"*

**Classic example:**
```
fact_sales: one row per product sold per store per day
- date_key, store_key, product_key
- quantity_sold, sales_amount
```

**Additive facts:** Almost everything here can be SUMmed across all
dimensions — total sales this week, total units per product, etc.

**Watch out for:** Very dense tables (billions of rows for high-volume
systems). This is fine — Delta Lake handles it well via file pruning.

---

### 2. Periodic Snapshot Fact Table — the "monthly statement" table

**Mental model:** A bank statement. At the end of every month (or week, or
day), the bank takes a photo of your balance and writes one row per account.
It doesn't matter if you had 200 transactions that month or zero — one row
gets written regardless.

**Grain:** One row per subject (account, product, employee) per time period.

**Key characteristic:** A row is written for every subject at every period,
even if nothing changed. Nulls or zeros appear for inactive periods rather
than gaps in the data.

**What it answers:** *"What was the status at the end of each period?"*

**Classic example:**
```
fact_inventory_snapshot: one row per product per store per week
- week_key, store_key, product_key
- quantity_on_hand, quantity_on_order, quantity_sold_this_week
```

**Additive facts:** Some are fully additive (quantity sold this week sums
across stores), some are semi-additive (quantity on hand can be SUMmed
across stores but NOT across time — adding up 52 weeks of balances gives
you nonsense).

**Why not just use the transaction table for this?** You could *reconstruct*
inventory levels from transaction records, but you'd need to replay every
transaction in the right order. Periodic snapshots pre-compute the state at
each period so queries are fast and simple.

---

### 3. Accumulating Snapshot Fact Table — the "pipeline tracker" table

**Mental model:** A fulfilment tracking page (like an Amazon order status).
One row is created when you place the order, then that *same row* gets
updated each time the order hits a new milestone: order placed, payment
confirmed, shipped, out for delivery, delivered. Multiple date columns, one
per milestone.

**Grain:** One row per pipeline instance (order, loan application, student
admission, insurance claim) — tracked from start to finish.

**Key characteristic:** Unlike the other two types, rows are **updated**
as the pipeline progresses. Date columns start NULL and get filled in as
each milestone is reached. This is the only Kimball fact table type that
gets UPDATE-d rather than INSERT-only.

**What it answers:** *"Where is each item in the process, and how long did
each stage take?"*

**Classic example:**
```
fact_order_pipeline: one row per order
- order_key
- date_ordered_key, date_payment_key, date_shipped_key, date_delivered_key
- days_to_ship, days_to_deliver  ← pre-computed lag facts
```

**Semi-additive / non-additive:** Lag facts (days between stages) are
non-additive — averaging them makes sense, summing them usually doesn't.

**Databricks relevance (preview for Phase 2):** This is the only fact table
type that maps naturally to a Delta Lake `MERGE` (upsert) operation rather
than a simple `INSERT`. The row exists, you update it. This has real
performance implications we'll explore in the storage layer phase.

---

### 4. Factless Fact Table — the "event log with no numbers"

**Mental model:** A security badge log. The fact that an employee swiped
their card at a door is a meaningful event worth recording — but there's
no numeric measurement attached to it. The event itself is the fact.

**Two flavours:**

**a) Pure event capture** — records that something happened
```
fact_student_attendance: one row per student per class per day
- date_key, student_key, class_key, teacher_key
(no numeric columns — the row's existence IS the fact)
```

**b) Coverage table** — records what *could* happen (what promotions a
product is *eligible* for, what classes a student *could* take), so you
can ask: "Of the things that were eligible, which ones actually happened?"
```
fact_promotion_eligibility: one row per product per promotion per day
- date_key, product_key, promotion_key
```
Joined against `fact_sales` to answer: "Which eligible products had zero
sales during the promotion?" — impossible without the coverage table.

**What it answers:** *"Did this event occur?"* or *"What was eligible?"*

---

## The comparison table you'll use repeatedly

| Type | One row = | Updated? | Additive? | Delta write pattern |
|---|---|---|---|---|
| Transaction | One event | Never | Fully | INSERT |
| Periodic Snapshot | Subject × Period | Never | Semi (time) | INSERT |
| Accumulating Snapshot | One pipeline instance | Yes | Non/semi | MERGE |
| Factless | One occurrence | Never | N/A (count rows) | INSERT |

---

## One rule that applies to all four types

**Never join two fact tables directly.** If you need to compare sales vs.
returns (two different fact tables), query them separately and merge the
result sets in your BI layer or via a CTE. Joining fact tables produces
a Cartesian-style explosion that silently returns wrong numbers.

---

## What today's hands-on builds

One schema, all four fact table types, sharing the same `dim_date`,
`dim_product`, `dim_store` dimensions from Day 01. The goal is to see all
four patterns side by side and feel the difference in how rows are loaded
and queried — particularly the MERGE on the accumulating snapshot.
