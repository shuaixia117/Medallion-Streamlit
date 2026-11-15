# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Raw Ingestion
# MAGIC
# MAGIC **Goal:** land the four source datasets as Delta tables, *faithfully*. No business logic, no filtering.
# MAGIC Every bad quantity, bad price and out-of-range date is kept — Silver decides what to do with them.
# MAGIC
# MAGIC | Table | Source | Notes |
# MAGIC |---|---|---|
# MAGIC | `retail_bronze.transactions` | `transactions/*.parquet` (8 quarterly files) | partitioned by `year`, `month` derived from `InvoiceDate` |
# MAGIC | `retail_bronze.products` | `products.parquet` | raw snapshot, duplicates included |
# MAGIC | `retail_bronze.stores` | `stores.parquet` | raw snapshot, duplicates included |
# MAGIC | `retail_bronze.returns` | `returns.parquet` | one row per return *event* |
# MAGIC
# MAGIC Every table gets three metadata columns:
# MAGIC `_ingest_timestamp` (when), `_source_file` (which file — via `_metadata.file_path`), `_batch_id` (which run).

# COMMAND ----------

import os
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Source location. On Databricks this is the Unity Catalog volume from the brief (Section 3.1).
# The env-var override only exists so the same notebook can be run by local/run_pipeline.py.
SRC = os.environ.get("RETAIL_SRC", "/Volumes/workspace/retail/raw/final_project_retail")

# FULL_REFRESH=True drops the Bronze tables and re-ingests everything.
# FULL_REFRESH=False is incremental: files already present in a table (by _source_file) are skipped,
# so re-running the notebook never double-counts rows.
FULL_REFRESH = os.environ.get("RETAIL_FULL_REFRESH", "false").lower() == "true"

BRONZE = "retail_bronze"
BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]

# InvoiceDate is stored in UTC and represents local shopping time. Keep the session in UTC,
# otherwise year/month partitions (and invoice_hour in Gold) shift.
spark.conf.set("spark.sql.session.timeZone", "UTC")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
print(f"SRC={SRC}\nBATCH_ID={BATCH_ID}\nFULL_REFRESH={FULL_REFRESH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers
# MAGIC `read_with_metadata` adds the three audit columns. `ingest` handles the write:
# MAGIC `mode="append"` + `mergeSchema=True` simulates incremental ingestion (new files append,
# MAGIC new columns in future files don't break the job), and the anti-join on `_source_file`
# MAGIC makes the append **idempotent** — the classic risk of append mode is silently loading the same file twice.

# COMMAND ----------

def read_with_metadata(path: str) -> DataFrame:
    """Read parquet and attach Bronze audit columns. _metadata.file_path is the
    Unity-Catalog-safe replacement for input_file_name()."""
    return (
        spark.read.parquet(path)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )


def table_exists(name: str) -> bool:
    return spark.catalog.tableExists(name)


def ingest(df: DataFrame, table: str, partition_by: list[str] | None = None) -> int:
    """Append df to a Bronze Delta table, skipping source files that were already loaded.
    Returns the number of rows written in this batch."""
    fq = f"{BRONZE}.{table}"
    if FULL_REFRESH and table_exists(fq):
        spark.sql(f"DROP TABLE {fq}")

    if table_exists(fq):
        loaded = spark.table(fq).select("_source_file").distinct()
        df = df.join(loaded, on="_source_file", how="left_anti")

    n_new = df.count()
    if n_new == 0:
        print(f"{fq}: nothing new to ingest (all source files already loaded)")
        return 0

    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(fq)
    print(f"{fq}: appended {n_new:,} rows")
    return n_new

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions — 8 quarterly files → one partitioned table
# MAGIC `year` / `month` are derived from `InvoiceDate` purely for physical layout (partition pruning).
# MAGIC Out-of-range dates are *kept*, which is why partitions 1970, 2023 and 2026 appear.

# COMMAND ----------

txn = (
    read_with_metadata(f"{SRC}/transactions/")
    .withColumn("year", F.year("InvoiceDate"))
    .withColumn("month", F.month("InvoiceDate"))
)
ingest(txn, "transactions", partition_by=["year", "month"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension snapshots + returns

# COMMAND ----------

for name in ["products", "stores", "returns"]:
    ingest(read_with_metadata(f"{SRC}/{name}.parquet"), name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation — row counts must match the source exactly
# MAGIC Expected (brief Section 3.1): transactions 987,739 · products 624 · stores 63 · returns 73,617.
# MAGIC 📸 `grading-artifacts/pipeline/bronze_row_counts.png`

# COMMAND ----------

EXPECTED = {"transactions": 987_739, "products": 624, "stores": 63, "returns": 73_617}

bronze_counts = spark.createDataFrame(
    [(t, spark.table(f"{BRONZE}.{t}").count(), EXPECTED[t]) for t in EXPECTED],
    "table STRING, row_count LONG, expected LONG",
).withColumn("match", F.col("row_count") == F.col("expected"))
display(bronze_counts)

mismatches = [r["table"] for r in bronze_counts.collect() if not r["match"]]
assert not mismatches, f"Bronze row counts differ from source for: {mismatches}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema check — metadata columns present
# MAGIC 📸 `grading-artifacts/pipeline/bronze_schema.png`

# COMMAND ----------

spark.table(f"{BRONZE}.transactions").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provenance & partitions
# MAGIC Rows per source file prove `_source_file` works; rows per `year` show the out-of-range partitions
# MAGIC that Silver will remove.

# COMMAND ----------

display(
    spark.table(f"{BRONZE}.transactions")
    .groupBy(F.element_at(F.split("_source_file", "/"), -1).alias("source_file"))
    .count()
    .orderBy("source_file")
)

# COMMAND ----------

display(spark.table(f"{BRONZE}.transactions").groupBy("year").count().orderBy("year"))
