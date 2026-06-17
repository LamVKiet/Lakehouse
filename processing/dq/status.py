"""
Phase 6c — Compact DATA-fail summary sentinel on S3.

When the gate fails, the job writes a SMALL JSON (reason breakdown + a few sample keys) to
dq/status/<run_date>.json. Two uses: (1) the Airflow alert reads it and renders straight into
email — no Jupyter digging; (2) a DATA-fail sentinel independent of where the driver runs
(cluster mode: stdout/exit-code don't reach Airflow, but S3 does).

WARNING: counts + sample keys only, do NOT dump full rows/PII (it goes into the mail).
Does NOT import pydeequ here -> the module is testable standalone, like reconcile.py.
"""
from __future__ import annotations

import json

from pyspark.sql import DataFrame
import pyspark.sql.functions as f


def summarize_failures(staged: DataFrame, bad: DataFrame, *, phase: str, run_date: str,
                       failed: list[str] | None, grain_col: str, sample_n: int = 5) -> dict:
    """Build a summary from the staged batch + the bad-rows df (failure_reason = CSV of rule names).
    Split the CSV -> explode -> count per reason; take a few grain keys as samples."""
    reason_rows = (
        bad.select(f.explode(f.split("failure_reason", ",")).alias("r"))
        .where("r <> ''").groupBy("r").count()
        .orderBy(f.col("count").desc()).collect()
    )
    reason_counts = {r["r"]: r["count"] for r in reason_rows}
    samples = [row[grain_col] for row in bad.select(grain_col).limit(sample_n).collect()]
    return {
        "phase": phase,
        "status": "Error",
        "run_date": run_date,
        "total_rows": staged.count(),
        "total_quarantined": bad.count(),
        "failed_constraints": failed or [],
        "reason_counts": reason_counts,
        "samples": samples,
    }


def write_summary(spark, path: str, summary: dict) -> None:
    """Write a single JSON object (not a part-dir) via the Hadoop FS — same creds/S3A as the job."""
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hconf)
    out = fs.create(jvm.org.apache.hadoop.fs.Path(path), True)
    try:
        out.write(bytearray(json.dumps(summary, ensure_ascii=False, indent=2), "utf-8"))
    finally:
        out.close()
