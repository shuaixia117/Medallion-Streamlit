"""Run the Databricks notebooks locally with PySpark + Delta Lake.

The notebooks in ../notebooks are Databricks "source format" .py files. They only rely on two
Databricks globals: `spark` and `display`. This runner provides both, points the notebooks at a
local copy of the raw data, and executes them in order in a single Spark session.

Usage (from the repo root):
    python local/run_pipeline.py --src /path/to/final_project_retail            # all notebooks
    python local/run_pipeline.py --src /path/to/final_project_retail 01 02      # only some
    python local/run_pipeline.py --src ... --full-refresh                       # rebuild Bronze

Requires: pip install -r local/requirements.txt  (and Java 17+)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS = sorted((ROOT / "notebooks").glob("[0-9][0-9]_*.py"))
WAREHOUSE = ROOT / "local" / "_warehouse"
METASTORE = ROOT / "local" / "_metastore"


def build_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[*]")
        .appName("retail-medallion-local")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(WAREHOUSE))
        # persistent Derby metastore so tables survive between runs (like a real catalog)
        .config("spark.hadoop.javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={METASTORE};create=true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def make_display():
    from pyspark.sql import DataFrame

    def display(obj, n: int = 40):
        if isinstance(obj, DataFrame):
            obj.show(n, truncate=False)
        else:
            print(obj)

    return display


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="path to the unzipped final_project_retail folder")
    ap.add_argument("--export", default=str(ROOT / "app" / "data"), help="where 04 writes the app CSVs")
    ap.add_argument("--full-refresh", action="store_true", help="drop and re-ingest Bronze")
    ap.add_argument("only", nargs="*", help="notebook prefixes to run, e.g. 01 02")
    args = ap.parse_args()

    os.environ["RETAIL_SRC"] = str(Path(args.src).resolve())
    os.environ["RETAIL_EXPORT"] = str(Path(args.export).resolve())
    os.environ["RETAIL_FULL_REFRESH"] = "true" if args.full_refresh else "false"
    os.chdir(ROOT / "local")  # keep derby.log etc. out of the repo root

    spark = build_spark()
    display = make_display()

    for nb in NOTEBOOKS:
        if args.only and not any(nb.name.startswith(p) for p in args.only):
            continue
        print(f"\n{'=' * 100}\n▶ {nb.name}\n{'=' * 100}")
        t0 = time.time()
        code = compile(nb.read_text(encoding="utf-8"), str(nb), "exec")
        exec(code, {"spark": spark, "display": display, "__name__": "__main__"})
        print(f"✔ {nb.name} finished in {time.time() - t0:.1f}s")

    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
