# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Business-Ready Analytical Tables
# MAGIC
# MAGIC **Goal:** wide, denormalised tables shaped for the dashboard queries. Every derived column is
# MAGIC computed *once* here, so queries and charts never re-implement business logic.
# MAGIC
# MAGIC | Table | Grain | Feeds |
# MAGIC |---|---|---|
# MAGIC | `retail_gold.retail_analytics` | one row per sales line (= Silver grain) + product & store attributes | Q1–Q4, Q6, Q8, Q9 |
# MAGIC | `retail_gold.store_performance` | store × month | Q5 |
# MAGIC | `retail_gold.category_trends` | category × week | Q7 |

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER, GOLD = "retail_silver", "retail_gold"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")


def write_delta(df, table: str):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{GOLD}.{table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · `retail_analytics` — the primary fact table
# MAGIC Silver rows **left** join products and stores, so non-merchandise StockCodes survive with NULL
# MAGIC product columns. Dimension columns are selected explicitly (both dims have `LastUpdated`; an
# MAGIC implicit `SELECT *` would collide).
# MAGIC
# MAGIC The 13 derived columns, defined exactly as the brief specifies:
# MAGIC
# MAGIC | Group | Column | Definition |
# MAGIC |---|---|---|
# MAGIC | Temporal | `invoice_date` | `DATE(InvoiceDate)` |
# MAGIC | | `invoice_year`, `invoice_month`, `invoice_month_name` | from `InvoiceDate` |
# MAGIC | | `invoice_dayofweek` | **1 = Monday … 7 = Sunday** (Spark's `dayofweek` is 1 = Sunday, so we remap) |
# MAGIC | | `invoice_hour` | 0–23, UTC = local shopping time |
# MAGIC | | `day_type` | Weekend for 6–7 else Weekday |
# MAGIC | Financial | `revenue` | `ROUND(net_quantity × UnitPrice, 2)` |
# MAGIC | | `gross_revenue` | `ROUND(Quantity × UnitPrice, 2)` |
# MAGIC | Flags | `is_return` | from Silver |
# MAGIC | | `has_partial_return` | `is_return AND returned_quantity < Quantity` |
# MAGIC | | `fully_returned` | `is_return AND returned_quantity = Quantity` |
# MAGIC | | `at_reorder_point` | `StockOnHand <= ReorderPoint` — **NULL if either side is NULL** (unknown ≠ "not at risk") |

# COMMAND ----------

unified = spark.table(f"{SILVER}.unified_txns")
products = spark.table(f"{SILVER}.products").select(
    "StockCode", "Description", "Category", "UnitCost", "ListPrice", "ReorderPoint", "StockOnHand", "IsActive"
)
stores = spark.table(f"{SILVER}.stores").select(
    "StoreID", "StoreName", "City", "State", "Region", "StoreType", "SquareFeet", "OpenDate"
)

# ISO day of week: Spark dayofweek() → 1=Sun..7=Sat; ((d + 5) % 7) + 1 → 1=Mon..7=Sun
iso_dow = ((F.dayofweek("InvoiceDate") + 5) % 7) + 1

retail_analytics = (
    unified
    .join(products, "StockCode", "left")
    .join(stores, "StoreID", "left")
    # temporal
    .withColumn("invoice_date", F.to_date("InvoiceDate"))
    .withColumn("invoice_year", F.year("InvoiceDate"))
    .withColumn("invoice_month", F.month("InvoiceDate"))
    .withColumn("invoice_month_name", F.date_format("InvoiceDate", "MMMM"))
    .withColumn("invoice_dayofweek", iso_dow)
    .withColumn("invoice_hour", F.hour("InvoiceDate"))
    .withColumn("day_type", F.when(F.col("invoice_dayofweek") >= 6, "Weekend").otherwise("Weekday"))
    # financial
    .withColumn("revenue", F.round(F.col("net_quantity") * F.col("UnitPrice"), 2))
    .withColumn("gross_revenue", F.round(F.col("Quantity") * F.col("UnitPrice"), 2))
    # flags (is_return already comes from Silver)
    .withColumn("has_partial_return", F.col("is_return") & (F.col("returned_quantity") < F.col("Quantity")))
    .withColumn("fully_returned", F.col("is_return") & (F.col("returned_quantity") == F.col("Quantity")))
    # Plain comparison: NULL <= x is NULL in SQL, which is exactly the behaviour we want. No coalesce.
    .withColumn("at_reorder_point", F.col("StockOnHand") <= F.col("ReorderPoint"))
)

assert retail_analytics.count() == unified.count(), "dimension join changed the row count (duplicate dim keys?)"
write_delta(retail_analytics, "retail_analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC 📸 `grading-artifacts/pipeline/gold_schema.png` and `gold_sample.png`

# COMMAND ----------

ra = spark.table(f"{GOLD}.retail_analytics")
ra.printSchema()

DERIVED = ["invoice_date", "invoice_year", "invoice_month", "invoice_month_name", "invoice_dayofweek",
           "invoice_hour", "day_type", "revenue", "gross_revenue", "is_return", "has_partial_return",
           "fully_returned", "at_reorder_point"]
display(ra.select("InvoiceNo", "StockCode", "Quantity", "returned_quantity", "net_quantity", "UnitPrice", *DERIVED)
          .filter("is_return").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity checks on derived columns

# COMMAND ----------

display(ra.groupBy("at_reorder_point").count())  # expect TRUE / FALSE / NULL buckets
display(ra.groupBy("invoice_dayofweek", "day_type").count().orderBy("invoice_dayofweek"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `store_performance` — store × month
# MAGIC `return_rate` = share of sales lines with a return. `regional_median_revenue` = median of
# MAGIC `monthly_revenue` across the stores of the same region in the same month; `vs_regional_median_pct`
# MAGIC tells how far a store sits above/below its regional peers (outlier detection).
# MAGIC The median is computed in a separate aggregate and joined back (rather than a window function)
# MAGIC because it reads more clearly and the median table is tiny (5 regions × 24 months).

# COMMAND ----------

store_month = (
    ra.groupBy("StoreID", "StoreName", "Region", "StoreType", "invoice_year", "invoice_month")
    .agg(
        F.round(F.sum("revenue"), 2).alias("monthly_revenue"),
        F.count("*").alias("lines"),
        F.sum(F.col("is_return").cast("int")).alias("return_lines"),
    )
    .withColumn("return_rate", F.round(F.col("return_lines") / F.col("lines"), 4))
)

regional_median = (
    store_month.groupBy("Region", "invoice_year", "invoice_month")
    .agg(F.round(F.median("monthly_revenue"), 2).alias("regional_median_revenue"))
)

store_performance = (
    store_month.join(regional_median, ["Region", "invoice_year", "invoice_month"])
    .withColumn(
        "vs_regional_median_pct",
        F.round((F.col("monthly_revenue") - F.col("regional_median_revenue")) / F.col("regional_median_revenue") * 100, 2),
    )
    .select("StoreID", "StoreName", "Region", "StoreType", "invoice_year", "invoice_month",
            "monthly_revenue", "lines", "return_lines", "return_rate",
            "regional_median_revenue", "vs_regional_median_pct")
)
write_delta(store_performance, "store_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `category_trends` — category × week with 4-week rolling averages
# MAGIC `week_start` = `DATE_TRUNC('week', …)` → the Monday of the week. Rolling window = current week +
# MAGIC 3 preceding weeks, per category. Rows with NULL Category (non-merchandise codes) are excluded —
# MAGIC they are not a category.
# MAGIC
# MAGIC We use `rowsBetween(-3, 0)` and *assert* that no category has a missing week; otherwise a
# MAGIC row-based window would silently span more than 4 calendar weeks.

# COMMAND ----------

weekly = (
    ra.filter(F.col("Category").isNotNull())
    .withColumn("week_start", F.to_date(F.date_trunc("week", "invoice_date")))
    .groupBy("Category", "week_start")
    .agg(
        F.round(F.sum("revenue"), 2).alias("weekly_revenue"),
        F.sum("net_quantity").alias("weekly_units"),
    )
)

w4 = Window.partitionBy("Category").orderBy("week_start").rowsBetween(-3, 0)
category_trends = (
    weekly
    .withColumn("rolling_4wk_avg_revenue", F.round(F.avg("weekly_revenue").over(w4), 2))
    .withColumn("rolling_4wk_avg_units", F.round(F.avg("weekly_units").over(w4), 2))
)

# Gap check: every category should have the same, contiguous set of weeks.
gaps = (
    category_trends.groupBy("Category")
    .agg(F.count("*").alias("n"), F.min("week_start").alias("lo"), F.max("week_start").alias("hi"))
    .withColumn("expected", (F.datediff("hi", "lo") / 7 + 1).cast("int"))
    .filter("n != expected")
)
assert gaps.count() == 0, "some category has missing weeks — switch the window to rangeBetween"
write_delta(category_trends, "category_trends")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row counts
# MAGIC 📸 `grading-artifacts/pipeline/gold_row_counts.png`

# COMMAND ----------

display(spark.createDataFrame(
    [(t, spark.table(f"{GOLD}.{t}").count()) for t in ["retail_analytics", "store_performance", "category_trends"]],
    "table STRING, row_count LONG",
))
