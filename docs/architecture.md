# Architecture & Design Decisions

## Data flow

```
 /Volumes/workspace/retail/raw/final_project_retail/          (raw parquet, never modified)
   ├─ transactions/*.parquet (8)  products  stores  returns
   │
   ▼  01_bronze_ingestion.py — faithful copy + _ingest_timestamp, _source_file, _batch_id
 retail_bronze.{transactions (partitioned year/month), products, stores, returns}
   │
   ▼  02_silver_transform.py — quality rules, dedupe, returns integration, audit log
 retail_silver.{unified_txns, products, stores, quality_audit}
   │
   ▼  03_gold_aggregate.py — dimension joins, 13 derived columns, pre-aggregates
 retail_gold.{retail_analytics, store_performance, category_trends}
   │
   ▼  04_analysis_queries.py — Q1–Q9 + CSV exports (~245 KB total)
 app/data/*.csv  ──►  app/app.py (Streamlit, pandas re-aggregation on every filter change)
```

## Layer contracts

| Layer | Contract | What is *not* allowed |
|---|---|---|
| Bronze | Same rows as the source, plus provenance metadata | Filtering, casting, dedupe, business logic |
| Silver | One clean row per business key at the right grain; every rule counted | Aggregation for reporting, presentation formatting |
| Gold | Query-ready: every derived column computed once | Re-implementing logic at query / app time |

## Key decisions (and what breaks without them)

### 1. Bronze append is idempotent
`mode="append"` + `mergeSchema=True` simulates incremental ingestion, but plain append loads the same
file twice if the notebook is re-run. Before writing, we left-anti-join the incoming rows against the
`_source_file` values already in the table. Re-running Bronze writes 0 rows (verified locally). Set
`FULL_REFRESH=True` to rebuild from scratch.

### 2. Partition only where it pays
Bronze transactions are partitioned by `year`/`month` (required; enables partition pruning and makes
the out-of-range years visible as partitions 1970/2023/2026). Silver/Gold are *not* partitioned: ~1M rows
is a small table, and partitioning it would create many tiny files that are slower to read.

### 3. Drop rules are applied in a fixed order
A row can fail several rules (e.g. negative quantity *and* zero price). Counts are attributed to the
first failing rule in the order Quantity → UnitPrice → InvoiceDate, matching the brief. Changing the order
changes per-rule counts but **not** the final row count.

### 4. Retain, don't drop: guests and unmatched SKUs
* `CustomerID IS NULL` = guest checkout → valid revenue, flagged `is_guest`.
* 5,922 lines use StockCodes absent from the catalog (`POST`, `ADJUST`, `AMAZONFEE`, `BANK CHARGES`, …):
  postage, adjustments and fees. They are real money, so revenue totals keep them; category-level
  queries exclude them with `WHERE Category IS NOT NULL`.

### 5. Dimension dedupe = latest `LastUpdated`, dates cast before sorting
`row_number() OVER (PARTITION BY key ORDER BY LastUpdated DESC) = 1`. `LastUpdated` is cast from text to
`DATE` first so we sort dates, not strings. The stale store rows also carried the dirty text
(`ATLANTA FLAGSHIP 2`, `'southeast '`); Region/StoreType are additionally trimmed + initcapped and the
notebook **asserts** exactly 5 regions and 4 store types, so a new dirty value fails the run loudly.
Gold asserts the dimension joins don't change the row count (which would happen with duplicate keys).

### 6. Returns: aggregate to sales-line grain before joining (the fan-out)
Returns are one row per *event*; 3,454 (invoice, SKU) pairs have 2–3 return events. Joining events directly
turns 981,011 sales lines into 984,868 rows (**+3,857 phantom rows**), inflating lines/units/revenue and
making `net_quantity` wrong (each duplicate subtracts only one event's quantity). Aggregating returns to
`(OriginalInvoiceNo, StockCode)` first guarantees ≤ 1 match per line. Two assertions guard it:
row count unchanged, and no `net_quantity < 0`.

### 7. `at_reorder_point` keeps NULL
`StockOnHand <= ReorderPoint` with no `coalesce`. 25 products have no ReorderPoint; for them the risk is
*unknown*, not *false*. Coalescing to FALSE would silently tell the business "no reorder needed".
Q8's `WHERE at_reorder_point = TRUE` correctly excludes unknowns.

### 8. Store medians via aggregate + join; rolling averages with a gap check
`regional_median_revenue` = exact `median()` of store monthly revenue per region-month, joined back.
`category_trends` rolling averages use `rowsBetween(-3, 0)`; the notebook asserts each category has a
contiguous series of weeks — otherwise a row-based window would span more than 4 calendar weeks.

### 9. App reads exported CSVs, re-aggregates from counts
No live warehouse connection (idle compute, secrets). Exports keep `invoice_year`, `Channel`,
`Category` as columns so filters apply *before* aggregation. Rates are exported as counts
(`return_lines`, `lines`) and recomputed as `Σreturn_lines / Σlines` — averaging pre-computed rates would
weight a tiny segment the same as a huge one. The export notebook reconciles export totals to Gold.

### 10. Anomaly definition (Chart 7)
A week is anomalous if its revenue is ≥ 2× the mean of the **four preceding** weeks (excluding the week
itself, so a spike can't inflate its own baseline) and ≥ 2× the category's average week. The final week
(starting 2025-12-29) has only 3 days and is excluded from detection.

## Expected results (reference run, local Spark 4.0 + Delta 4.0)

Use these to check a Databricks run.

| Check | Value |
|---|---|
| Bronze transactions / products / stores / returns | 987,739 / 624 / 63 / 73,617 |
| Bronze year partitions 1970 / 2023 / 2024 / 2025 / 2026 | 85 / 360 / 483,572 / 503,361 / 361 |
| Drop `Quantity ≤ 0` | −3,457 → 984,282 |
| Drop `UnitPrice ≤ 0` | −2,469 → 981,813 |
| Drop date outside 2024–2025 | −802 → **981,011** |
| Guest lines (retained) | 194,646 |
| Unmatched StockCode lines (retained) | 5,922 |
| Naive returns join (fan-out) | 984,868 (+3,857) |
| Returns at line grain / matched / orphan | 69,760 / 69,540 / 220 |
| Silver products / stores | 600 / 60 |
| Gold retail_analytics / store_performance / category_trends | 981,011 / 1,440 / 1,260 |
| `at_reorder_point` TRUE / FALSE / NULL (lines) | 59,827 / 862,327 / 58,857 |
| Total net revenue | $31,069,016.81 |

### Headline findings

| Query | Finding |
|---|---|
| Q1 | In-Store 53.6% · Online 33.9% · Mobile App 12.5% of net revenue |
| Q2/Q9 | 2025 revenue +3.6% vs 2024; only Aug (−4.2%) and Dec (−3.2%) declined |
| Q3 | In-Store trades 08:00–21:59, peaks at 13:00; Online/Mobile peak in the evening (21:00/22:00) |
| Q4 | Electronics Accessories #1 ($4.28M) at ~$37/unit — high-ticket, not high-volume |
| Q5 | Northeast Standard is the largest cell ($6.18M) |
| Q6 | Apparel & Accessories via Online returns 24.9% of lines; Food & Snacks In-Store 1.0% |
| Q7 | Gifts & Seasonal, week of 2024-12-16: $189.2K = 3.6× prior 4-week avg, 7.2× an average week; no repeat in 2025 |
| Q8 | 46 SKUs at/below reorder point |
