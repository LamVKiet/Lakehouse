"""
One-shot: UNSET delta.autoOptimize.* on CDC Bronze tables so streaming append
writes in parallel (no hot-path compaction). Small-file compaction is handled
off-peak by dag_maintenance_optimize. Run inside the airflow container:
  docker exec airflow bash -c "cd /app && python3 scripts/unset_optimize_cdc.py"
"""
import sys, os
sys.path.insert(0, "/app")
from processing.spark_jobs.delta_utils import get_spark_session, get_s3_path
from delta.tables import DeltaTable

CDC_TABLES = ["pos_transactions", "online_transactions",
              "pos_transaction_details", "online_transaction_details"]

spark = get_spark_session("unset-optimize-cdc")
spark.sparkContext.setLogLevel("ERROR")
for t in CDC_TABLES:
    path = get_s3_path("bronze", t)
    if not DeltaTable.isDeltaTable(spark, path):
        print(f"{t:30s} -> not a Delta table yet, skip")
        continue
    spark.sql(f"ALTER TABLE delta.`{path}` UNSET TBLPROPERTIES IF EXISTS "
              "('delta.autoOptimize.optimizeWrite', 'delta.autoOptimize.autoCompact')")
    props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`").collect()}
    ow = props.get("delta.autoOptimize.optimizeWrite", "<unset>")
    ac = props.get("delta.autoOptimize.autoCompact", "<unset>")
    print(f"{t:30s} -> optimizeWrite={ow}, autoCompact={ac}")
spark.stop()
