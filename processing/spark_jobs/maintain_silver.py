"""
Spark Job: Weekly OPTIMIZE + VACUUM for all silver tables.
Schedule: Sundays 04:00 (Asia/Ho_Chi_Minh) via dag_silver_maintenance.

OPTIMIZE compacts small files and applies ZORDER for skipping.
VACUUM removes files no longer referenced (default 168h = 7 days retention).
"""

import os
import sys
from datetime import datetime
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from processing.spark_jobs.delta_utils import get_spark_session, get_s3_path

### spark session
spark = get_spark_session("Silver-Maintenance")
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
# VACUUM safety check disabled to allow custom retention
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")


### Section 1: functions
TABLES_ZORDER = {
    "events":       ("user_id", "event_id"),
    "transactions": ("customer_id", "branch_id"),
    "customers":    ("customer_id",),
    "products":     ("product_id",),
    "branches":     ("branch_id",),
    # nou: small lookup table — skip ZORDER, only VACUUM
}
RETAIN_HOURS = 168


def optimize_and_vacuum(table: str):
    path = get_s3_path("silver", table)
    print(f"[maintain_silver] >>> {table} ({path})")

    if table in TABLES_ZORDER:
        zcols = ", ".join(TABLES_ZORDER[table])
        print(f"  OPTIMIZE ZORDER BY ({zcols})")
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({zcols})")
    else:
        print("  OPTIMIZE (no ZORDER)")
        spark.sql(f"OPTIMIZE delta.`{path}`")

    print(f"  VACUUM RETAIN {RETAIN_HOURS} HOURS")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {RETAIN_HOURS} HOURS")


### Section 2: params
today = datetime.now().strftime("%Y-%m-%d")
print("PARAM >>>", today)
print("PARAM >>>", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

ALL_TABLES = ["events", "nou", "transactions", "customers", "products", "branches"]


### Section 3: run
for tbl in ALL_TABLES:
    try:
        optimize_and_vacuum(tbl)
    except Exception as e:
        print(f"[maintain_silver] {tbl} FAILED: {e}")

### Section 4: stop
spark.stop()
