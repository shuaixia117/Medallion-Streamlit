# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Analysis Queries (Q1–Q9) + App Exports
# MAGIC
# MAGIC Part 1 runs the nine reference queries from the brief (Section 6.1) against Gold.
# MAGIC Part 2 exports the CSVs that the Streamlit app reads (`app/data/`).
# MAGIC
# MAGIC **Why export instead of querying live?** Serverless compute / SQL warehouses stop when idle and a
# MAGIC live connection needs a token that must never be committed. Small pre-aggregated CSVs make the
# MAGIC app fast, free to host and reliable.

# COMMAND ----------

import os

spark.conf.set("spark.sql.session.timeZone", "UTC")

EXPORT = os.environ.get("RETAIL_EXPORT", "/Volumes/workspace/retail/raw/app_exports")
os.makedirs(EXPORT, exist_ok=True)

QUERIES = {
    "Q1_channel_revenue": """
        SELECT Channel, COUNT(*) AS lines, SUM(net_quantity) AS units,
               ROUND(SUM(revenue),2) AS revenue,
               ROUND(SUM(revenue)/SUM(SUM(revenue)) OVER()*100,1) AS pct
        FROM retail_gold.retail_analytics GROUP BY Channel ORDER BY revenue DESC""",
    "Q2_monthly_trend": """
        SELECT invoice_year, invoice_month, invoice_month_name,
               ROUND(SUM(revenue),2) AS monthly_revenue
        FROM retail_gold.retail_analytics GROUP BY 1,2,3 ORDER BY 1,2""",
    "Q3_hourly_demand": """
        SELECT invoice_hour, Channel, COUNT(*) AS transactions, ROUND(SUM(revenue),2) AS revenue
        FROM retail_gold.retail_analytics GROUP BY 1,2 ORDER BY 1,2""",
    "Q4_top_categories": """
        SELECT Category, ROUND(SUM(revenue),2) AS revenue, SUM(net_quantity) AS units
        FROM retail_gold.retail_analytics WHERE Category IS NOT NULL
        GROUP BY 1 ORDER BY revenue DESC LIMIT 10""",
    "Q5_store_performance": """
        SELECT Region, StoreType, ROUND(SUM(monthly_revenue),2) AS revenue
        FROM retail_gold.store_performance GROUP BY 1,2 ORDER BY 1,2""",
    "Q6_return_rates": """
        SELECT Category, Channel, ROUND(SUM(CAST(is_return AS INT))/COUNT(*),4) AS return_rate
        FROM retail_gold.retail_analytics WHERE Category IS NOT NULL
        GROUP BY 1,2 ORDER BY return_rate DESC""",
    "Q7_anomaly_weekly": """
        SELECT DATE_TRUNC('week',week_start) AS wk, Category, ROUND(SUM(weekly_revenue),2) AS revenue
        FROM retail_gold.category_trends WHERE Category='Gifts & Seasonal'
        GROUP BY 1,2 ORDER BY 1""",
    "Q8_reorder_alerts": """
        SELECT StockCode, Description, Category, ReorderPoint, SUM(net_quantity) AS net_sold
        FROM retail_gold.retail_analytics WHERE at_reorder_point=TRUE
        GROUP BY 1,2,3,4 ORDER BY net_sold ASC LIMIT 20""",
    "Q9_yoy_comparison": """
        SELECT a.invoice_month, a.invoice_month_name, ROUND(a.r,2) AS rev_2024, ROUND(b.r,2) AS rev_2025,
               ROUND((b.r-a.r)/a.r*100,1) AS yoy_pct
        FROM (SELECT invoice_month, invoice_month_name, SUM(revenue) r
              FROM retail_gold.retail_analytics WHERE invoice_year=2024 GROUP BY 1,2) a
        JOIN (SELECT invoice_month, SUM(revenue) r
              FROM retail_gold.retail_analytics WHERE invoice_year=2025 GROUP BY 1) b
          ON a.invoice_month=b.invoice_month
        ORDER BY 1""",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 · Q1–Q9
# MAGIC 📸 One screenshot per query → `grading-artifacts/queries/<name>.png`.
# MAGIC (In Databricks you can also paste each SQL into its own `%sql` cell for nicer screenshots.)

# COMMAND ----------

results = {}
for name, sql in QUERIES.items():
    print(f"\n### {name}")
    results[name] = spark.sql(sql)
    display(results[name])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 · App exports
# MAGIC Rule of thumb from the brief: export at a grain **slightly finer than the chart**, keeping
# MAGIC `invoice_year`, `Channel` (and `Category` where relevant) as columns so the app can filter and
# MAGIC re-aggregate in pandas. Never export the ~1M-row fact table.
# MAGIC
# MAGIC Note the export of *counts* (`lines`, `return_lines`) instead of a pre-computed `return_rate`:
# MAGIC a rate cannot be re-aggregated by averaging (that would weight a tiny segment the same as a
# MAGIC huge one) — `SUM(return_lines) / SUM(lines)` after filtering is always correct.
# MAGIC
# MAGIC | File | Grain | Charts |
# MAGIC |---|---|---|
# MAGIC | `channel_month.csv` | year × month × channel | 1, 2, KPIs, YoY |
# MAGIC | `hourly_channel.csv` | year × hour × channel | 3 |
# MAGIC | `category_channel.csv` | year × category × channel | 4, 6 |
# MAGIC | `store_month.csv` | store × year × month | 5 |
# MAGIC | `category_weekly.csv` | category × week | 7 |
# MAGIC | `reorder_alerts.csv` | SKU at reorder point × year × channel | Q8 table |

# COMMAND ----------

EXPORTS = {
    "channel_month": """
        SELECT invoice_year, invoice_month, invoice_month_name, Channel,
               COUNT(*) AS lines, SUM(CAST(is_return AS INT)) AS return_lines,
               SUM(net_quantity) AS units, ROUND(SUM(revenue),2) AS revenue,
               ROUND(SUM(gross_revenue),2) AS gross_revenue
        FROM retail_gold.retail_analytics GROUP BY 1,2,3,4 ORDER BY 1,2,4""",
    "hourly_channel": """
        SELECT invoice_year, invoice_hour, Channel,
               COUNT(*) AS transactions, ROUND(SUM(revenue),2) AS revenue
        FROM retail_gold.retail_analytics GROUP BY 1,2,3 ORDER BY 1,2,3""",
    "category_channel": """
        SELECT invoice_year, Category, Channel,
               COUNT(*) AS lines, SUM(CAST(is_return AS INT)) AS return_lines,
               SUM(net_quantity) AS units, ROUND(SUM(revenue),2) AS revenue
        FROM retail_gold.retail_analytics WHERE Category IS NOT NULL
        GROUP BY 1,2,3 ORDER BY 1,2,3""",
    "store_month": """
        SELECT StoreID, StoreName, Region, StoreType, invoice_year, invoice_month,
               monthly_revenue, lines, return_lines, return_rate,
               regional_median_revenue, vs_regional_median_pct
        FROM retail_gold.store_performance ORDER BY StoreID, invoice_year, invoice_month""",
    "category_weekly": """
        SELECT Category, week_start, weekly_revenue, weekly_units,
               rolling_4wk_avg_revenue, rolling_4wk_avg_units
        FROM retail_gold.category_trends ORDER BY Category, week_start""",
    "reorder_alerts": """
        SELECT StockCode, Description, Category, ReorderPoint, StockOnHand, invoice_year, Channel,
               SUM(net_quantity) AS net_sold, ROUND(SUM(revenue),2) AS revenue
        FROM retail_gold.retail_analytics WHERE at_reorder_point = TRUE
        GROUP BY 1,2,3,4,5,6,7 ORDER BY 1,6,7""",
}

for name, sql in EXPORTS.items():
    pdf = spark.sql(sql).toPandas()
    path = f"{EXPORT}/{name}.csv"
    pdf.to_csv(path, index=False)
    print(f"{name:<18} {len(pdf):>6,} rows → {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconciliation — exports must add back up to Gold
# MAGIC If a filter or join in an export drops rows, totals in the app silently drift from Q1.

# COMMAND ----------

import pandas as pd

gold_total = spark.sql("SELECT ROUND(SUM(revenue),2) FROM retail_gold.retail_analytics").first()[0]
cm_total = round(pd.read_csv(f"{EXPORT}/channel_month.csv")["revenue"].sum(), 2)
hc_total = round(pd.read_csv(f"{EXPORT}/hourly_channel.csv")["revenue"].sum(), 2)
print(f"Gold revenue {gold_total:,.2f} | channel_month {cm_total:,.2f} | hourly_channel {hc_total:,.2f}")
assert abs(gold_total - cm_total) < 1 and abs(gold_total - hc_total) < 1, "exports do not reconcile to Gold"
