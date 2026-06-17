"""
Phase 0 — Setup & config (AWS S3 + Delta, aligned with the project's delta_utils).

A single place that declares the lake paths (staging/prod/quarantine/evidence/metrics) and the
tag convention. The SparkSession is NOT built here — always use delta_utils.get_spark_session
(project rule: do not reimplement the session inline).

On-prem / Docker note: the Deequ jar is baked into /opt/spark/jars (Dockerfile.spark), so on the
cluster there is NO need for spark.jars.packages (which fetches from Maven at runtime — on-prem
executors are usually blocked from the internet). When running in a LOCAL venv, set DEEQU_LOCAL_JAR
to the path of deequ-2.0.7-spark-3.5.jar -> it is loaded via spark.jars.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

# --- MANDATORY: set BEFORE importing pydeequ -------------------------------
# PyDeequ reads this to pick the jar coordinate matching your Spark (3.5 in this project).
os.environ.setdefault("SPARK_VERSION", "3.5")

import pydeequ  # noqa: E402,F401  (import after setting SPARK_VERSION)
from pyspark.sql import SparkSession  # noqa: E402

from processing.spark_jobs.delta_utils import S3_BUCKET, get_spark_session  # noqa: E402

# When running locally: point to the offline-downloaded deequ jar; leave empty on the cluster
# (the jar is already on the classpath).
DEEQU_LOCAL_JAR = os.environ.get("DEEQU_LOCAL_JAR", "").strip()


def _s3(*parts: str) -> str:
    base = f"s3a://{S3_BUCKET}"
    return "/".join([base, *[p.strip("/") for p in parts]]) + "/"


@dataclass(frozen=True)
class Paths:
    """Lake layout. prod = the table's real path; staging/quarantine/evidence/metrics live under dq/."""
    table: str
    layer: str  # bronze | silver | gold

    @property
    def staging(self) -> str:
        return _s3("dq", "staging", self.layer, self.table)

    @property
    def prod(self) -> str:
        return _s3(self.layer, self.table)

    @property
    def quarantine(self) -> str:
        return _s3("dq", "quarantine", self.layer, self.table)

    @property
    def quarantine_reconcile(self) -> str:
        # cross-table reconcile offenders — separate from the row-level audit quarantine
        return _s3("dq", "quarantine_reconcile", self.layer, self.table)

    @property
    def evidence(self) -> str:
        # audit evidence (checkResults) — kept for the auditor
        return _s3("dq", "evidence", self.layer, self.table)

    @property
    def metrics(self) -> str:
        # FileSystemMetricsRepository = ONE JSON file per table (avoid a shared file -> last-write-wins)
        return _s3("dq", "metrics").rstrip("/") + f"/{self.layer}_{self.table}.json"

    @property
    def status(self) -> str:
        # per-run compact failure summary JSON prefix -> job appends "<run_date>.json"
        return _s3("dq", "status", self.layer, self.table)


def make_tags(paths: "Paths", run_date: str, *, pipeline: str, env: str = "prod") -> dict:
    """FIXED tag convention — the dimensions you filter history by later (Phase 4/5). A missing
    dimension means you can't slice by it afterwards."""
    return {
        "table": paths.table,
        "layer": paths.layer,
        "run_date": run_date,
        "pipeline": pipeline,
        "env": env,
    }


def build_spark(app_name: str = "deequ-dq") -> SparkSession:
    """SparkSession via the project's shared factory (Delta + S3A). Loads the Deequ jar when local."""
    return get_spark_session(app_name, extra_jars=DEEQU_LOCAL_JAR or None)


def shutdown_spark(spark: SparkSession) -> None:
    """Cleanup — PyDeequ holds a JVM callback server, remember to stop it at the end of the job."""
    try:
        spark.sparkContext._gateway.shutdown_callback_server()
    except Exception:
        pass
    spark.stop()
