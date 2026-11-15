# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — Cleansed & Conformed
# MAGIC
# MAGIC **Goal:** apply business rules, quality filters and schema enforcement; integrate returns.
# MAGIC
# MAGIC | Table | Grain | Built from |
# MAGIC |---|---|---|
# MAGIC | `retail_silver.products` | one row per `StockCode` (latest version) | `retail_bronze.products` |
# MAGIC | `retail_silver.stores` | one row per `StoreID` (latest version) | `retail_bronze.stores` |
# MAGIC | `retail_silver.unified_txns` | one row per sales line | `retail_bronze.transactions` ⟕ aggregated `retail_bronze.returns` |
# MAGIC | `retail_silver.quality_audit` | one row per quality step | every rule below, with before/after counts |
# MAGIC
# MAGIC Quality rules (brief Section 3.3):
# MAGIC 1. `Quantity ≤ 0` → **drop**
# MAGIC 2. `UnitPrice ≤ 0` → **drop**
# MAGIC 3. `InvoiceDate` outside 2024–2025 → **drop**
# MAGIC 4. `CustomerID IS NULL` → **retain**, add `is_guest`
# MAGIC 5. `StockCode` with no product match → **retain** (product columns NULL in Gold), count it
# MAGIC 6. Duplicate / stale dimension rows → keep latest `LastUpdated` per key, cast dates, trim / standardise text
# MAGIC 7. Orphan returns → excluded by the left join, count it

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

BRONZE, SILVER = "retail_bronze", "retail_silver"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")

# Every quality decision is recorded here and written to retail_silver.quality_audit at the end.
audit: list[tuple] = []


def log_step(step: int, table: str, rule: str, action: str, before: int, after: int, affected: int):
    audit.append((step, table, rule, action, before, after, affected))
    print(f"[{step}] {table:<12} {rule:<45} {action:<8} before={before:>9,} after={after:>9,} affected={affected:>7,}")


def write_delta(df, table: str):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{SILVER}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Products — dedupe to the latest version per StockCode
# MAGIC The raw catalog has 624 rows for 600 SKUs: 9 exact duplicates plus 15 stale versions
# MAGIC (e.g. a 2024 stock snapshot superseded by a 2025-06-30 one). `row_number()` over
# MAGIC `StockCode ORDER BY LastUpdated DESC` keeps exactly one row per key. Text is trimmed and
# MAGIC `LastUpdated` is cast from `yyyy-MM-dd` text to `DATE` *before* ordering, so we sort dates, not strings.

# COMMAND ----------

latest_first = lambda key: Window.partitionBy(key).orderBy(F.col("LastUpdated").desc())

products_raw = spark.table(f"{BRONZE}.products")
products = (
    products_raw
    .select(
        F.trim("StockCode").alias("StockCode"),
        F.trim("Description").alias("Description"),
        F.trim("Category").alias("Category"),
        F.col("UnitCost").cast("double"),
        F.col("ListPrice").cast("double"),
        F.col("ReorderPoint").cast("int"),
        F.col("StockOnHand").cast("int"),
        F.col("IsActive").cast("boolean"),
        F.to_date("LastUpdated", "yyyy-MM-dd").alias("LastUpdated"),
    )
    .withColumn("_rn", F.row_number().over(latest_first("StockCode")))
    .filter("_rn = 1")
    .drop("_rn")
)

n_before, n_after = products_raw.count(), products.count()
log_step(6, "products", "dedupe: latest LastUpdated per StockCode", "dedupe", n_before, n_after, n_before - n_after)
assert n_after == products.select("StockCode").distinct().count(), "products still has duplicate keys"
write_delta(products, "products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Stores — dedupe + standardise Region / StoreType
# MAGIC 63 rows for 60 stores. The stale 2023 versions also carry dirty text (`ATLANTA FLAGSHIP 2`,
# MAGIC `'southeast '`). We standardise *and* dedupe, then assert the dimension resolves to exactly
# MAGIC 5 regions and 4 store types — if a new dirty value ever arrives, the pipeline fails loudly
# MAGIC instead of silently producing a 6th region in the dashboard.

# COMMAND ----------

stores_raw = spark.table(f"{BRONZE}.stores")
stores = (
    stores_raw
    .select(
        F.trim("StoreID").alias("StoreID"),
        F.trim("StoreName").alias("StoreName"),
        F.trim("City").alias("City"),
        F.upper(F.trim("State")).alias("State"),
        F.initcap(F.trim("Region")).alias("Region"),
        F.initcap(F.trim("StoreType")).alias("StoreType"),
        F.col("SquareFeet").cast("int"),
        F.to_date("OpenDate", "yyyy-MM-dd").alias("OpenDate"),
        F.to_date("LastUpdated", "yyyy-MM-dd").alias("LastUpdated"),
    )
    .withColumn("_rn", F.row_number().over(latest_first("StoreID")))
    .filter("_rn = 1")
    .drop("_rn")
)

n_before, n_after = stores_raw.count(), stores.count()
log_step(6, "stores", "dedupe: latest LastUpdated per StoreID", "dedupe", n_before, n_after, n_before - n_after)

regions = sorted(r[0] for r in stores.select("Region").distinct().collect())
store_types = sorted(r[0] for r in stores.select("StoreType").distinct().collect())
print("Regions:", regions, "\nStoreTypes:", store_types)
assert regions == ["Midwest", "Northeast", "Southeast", "Southwest", "West"], regions
assert store_types == ["Express", "Flagship", "Outlet", "Standard"], store_types
write_delta(stores, "stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Transactions — the three drop rules + guest flag
# MAGIC Rules are applied **in order** and each step's count is measured against the previous step,
# MAGIC so the audit shows exactly how many rows each rule removed (a row failing two rules is
# MAGIC attributed to the first one). Bronze metadata is dropped except `_source_file` for lineage.

# COMMAND ----------

bronze_txns = spark.table(f"{BRONZE}.transactions").drop("_ingest_timestamp", "_batch_id", "year", "month")
n0 = bronze_txns.count()
log_step(0, "transactions", "bronze input", "-", n0, n0, 0)

s1 = bronze_txns.filter(F.col("Quantity") > 0)
n1 = s1.count()
log_step(1, "transactions", "Quantity <= 0", "drop", n0, n1, n0 - n1)

s2 = s1.filter(F.col("UnitPrice") > 0)
n2 = s2.count()
log_step(2, "transactions", "UnitPrice <= 0", "drop", n1, n2, n1 - n2)

s3 = s2.filter(F.year("InvoiceDate").between(2024, 2025))
n3 = s3.count()
log_step(3, "transactions", "InvoiceDate outside 2024-2025", "drop", n2, n3, n2 - n3)

# Guest checkouts are valid sales — flag, don't drop. The reference snippet expects this name.
clean_txns = s3.withColumn("is_guest", F.col("CustomerID").isNull())
n_guest = clean_txns.filter("is_guest").count()
log_step(4, "transactions", "CustomerID IS NULL", "flag", n3, n3, n_guest)

# StockCodes with no product match (POST, ADJUST, AMAZONFEE, BANK CHARGES, ...) are non-merchandise
# lines. They're real money movements, so we keep them; Gold's left join gives them NULL product columns.
n_unmatched = clean_txns.join(products.select("StockCode"), "StockCode", "left_anti").count()
log_step(5, "transactions", "StockCode not in products", "retain", n3, n3, n_unmatched)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Returns integration — aggregate first, then join (no fan-out)
# MAGIC `retail_bronze.returns` is at **return-event** grain; a single sales line can be returned in
# MAGIC 2–3 separate events. Joining events directly multiplies sales lines. So we first collapse
# MAGIC returns to the sales-line grain `(OriginalInvoiceNo, StockCode)`, then left join — each sales line
# MAGIC can match at most one row, so the join cannot change the row count.
# MAGIC
# MAGIC First, measure the problem we're preventing (the naive join):

# COMMAND ----------

returns_raw = spark.table(f"{BRONZE}.returns")
naive = clean_txns.join(
    returns_raw,
    (clean_txns.InvoiceNo == returns_raw.OriginalInvoiceNo) & (clean_txns.StockCode == returns_raw.StockCode),
    "left",
)
n_naive = naive.count()
print(f"clean_txns rows: {n3:,} | naive join rows: {n_naive:,} | fan-out: +{n_naive - n3:,} phantom rows")

display(
    returns_raw.groupBy("OriginalInvoiceNo", "StockCode").count()
    .groupBy(F.col("count").alias("return_events_per_line")).count()
    .orderBy("return_events_per_line")
)

# COMMAND ----------

# Reference snippet from the brief (Section 3.3), unchanged.
returns_agg = (spark.table("retail_bronze.returns")
    .groupBy("OriginalInvoiceNo","StockCode")
    .agg(F.sum("Quantity").alias("returned_qty"),F.count("*").alias("return_events")))
# clean_txns = Bronze transactions after the three drop rules above
unified = (clean_txns
    .join(returns_agg,(clean_txns.InvoiceNo==returns_agg.OriginalInvoiceNo)&
          (clean_txns.StockCode==returns_agg.StockCode),how="left")
    .withColumn("returned_quantity",F.coalesce("returned_qty",F.lit(0)))
    .withColumn("net_quantity",F.col("Quantity")-F.col("returned_quantity"))
    .withColumn("is_return",F.col("returned_qty").isNotNull()))

# The join condition is an expression, so both StockCode columns survive — drop the returns side.
unified = unified.drop(returns_agg.StockCode).drop("OriginalInvoiceNo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify before writing — both checks must pass

# COMMAND ----------

n_unified = unified.count()
assert n_unified == clean_txns.count(), "fan-out: row count changed after the join"
assert unified.filter("net_quantity < 0").count() == 0, "returned more units than sold"

# Orphan returns: return lines/events whose (OriginalInvoiceNo, StockCode) matches no retained sale.
# They drop out of the left join silently — count them with left_anti so we can report it.
sale_keys = clean_txns.select(F.col("InvoiceNo").alias("OriginalInvoiceNo"), "StockCode")
orphan_lines = returns_agg.join(sale_keys, ["OriginalInvoiceNo", "StockCode"], "left_anti")
n_orphan_lines = orphan_lines.count()
n_orphan_events = orphan_lines.agg(F.sum("return_events")).first()[0] or 0
n_matched_lines = unified.filter("is_return").count()
log_step(7, "returns", "orphan returns (no matching retained sale)", "exclude",
         returns_raw.count(), returns_raw.count() - n_orphan_events, n_orphan_events)
print(f"return lines (agg grain): {returns_agg.count():,} | matched to sales: {n_matched_lines:,} | orphan lines: {n_orphan_lines:,}")

write_delta(unified, "unified_txns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lines returned in multiple events — proof that `net_quantity` is correct
# MAGIC 📸 `grading-artifacts/pipeline/silver_net_qty_sample.png`

# COMMAND ----------

display(
    spark.table(f"{SILVER}.unified_txns")
    .filter("return_events > 1")
    .select("InvoiceNo", "StockCode", F.col("Quantity").alias("gross_qty"), "returned_qty", "return_events", F.col("net_quantity").alias("net_qty"))
    .orderBy("InvoiceNo", "StockCode")
    .limit(5)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Quality audit log
# MAGIC 📸 `grading-artifacts/pipeline/silver_quality_audit.png`

# COMMAND ----------

audit_df = spark.createDataFrame(
    audit, "step INT, table STRING, rule STRING, action STRING, rows_before LONG, rows_after LONG, rows_affected LONG"
).orderBy("step", "table")
write_delta(audit_df, "quality_audit")
display(spark.table(f"{SILVER}.quality_audit"))

# COMMAND ----------

display(spark.createDataFrame(
    [(t, spark.table(f"{SILVER}.{t}").count()) for t in ["unified_txns", "products", "stores"]],
    "table STRING, row_count LONG",
))
