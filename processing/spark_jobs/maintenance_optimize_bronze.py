"""
Spark Maintenance: off-peak OPTIMIZE + VACUUM for CDC Bronze tables.

Big-company pattern: the streaming CDC job (streaming_cdc_to_bronze.py) keeps
`optimizeWrite` ON (right-size files at write) but `autoCompact` OFF (no compaction
in the hot path). Compaction is instead done here, once a day, off-peak.

Scope: the 4 CDC Bronze tables (pos/online transactions + details).
  - OPTIMIZE: compaction only (bin-pack). No ZORDER — Bronze keeps the raw Debezium
    `payload` JSON, there is no useful clustering column at this layer.
  - VACUUM: physically delete tombstones OLDER than the 7-day safety window
    (today's OPTIMIZE tombstones are kept ~7 days so time-travel still works;
    they get reclaimed by a later run once past the window — this is intentional).

Triggered by Airflow daily, off-peak (after Gold). Receives date as sys.argv[1] (YYYY-MM-DD).
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from processing.spark_jobs.delta_utils import get_spark_session, get_s3_path

spark = get_spark_session("Maintenance-OptimizeBronze")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

CDC_TABLES = [
    "pos_transactions",
    "online_transactions",
    "pos_transaction_details",
    "online_transaction_details",
]
VACUUM_RETAIN_HOURS = 168   


def optimize_table(table: str) -> None:
    path = get_s3_path("bronze", table)
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"[optimize] skip {table} — not a Delta table yet")
        return
    print(f"[optimize] OPTIMIZE bronze.{table} WHERE report_date = {run_date}")
    spark.sql(f"OPTIMIZE delta.`{path}` WHERE report_date = {run_date}")
    print(f"[vacuum] VACUUM bronze.{table} RETAIN {VACUUM_RETAIN_HOURS} HOURS")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {VACUUM_RETAIN_HOURS} HOURS")


def run() -> None:
    for table in CDC_TABLES:
        optimize_table(table)
    print("[maintenance] OPTIMIZE + VACUUM complete for all CDC Bronze tables.")


ymd = sys.argv[1]
ym  = ymd[0:7]
today = datetime.now().strftime("%Y-%m-%d")

print("PARAM >>>", ymd)
print("PARAM >>>", ym)
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

run_date = ymd.replace("-", "")


run()
spark.stop()
