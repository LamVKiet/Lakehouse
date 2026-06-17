"""
processing.dq — DQ trust backbone (Deequ + WAP + Delta + Airflow on AWS S3).

Phase map:
  config.py     Phase 0  — Paths/tags + SparkSession via delta_utils (S3 + Delta)
  analyzers.py  Phase 1  — Analyzers + AnalysisRunner (measure, single-pass, narrow/wide)
  checks.py     Phase 2  — Check/VerificationSuite per table (hard/soft gate)
  profiling.py  Phase 3  — Profiling + Suggestion (discovery at onboarding)
  repository.py Phase 4  — MetricsRepository on S3 (history -> trend)
  anomaly.py    Phase 5  — AnomalyDetection (statistical, reads history)
  wap.py        Phase 6  — WAP engine + Delta constraint (preventive + detective)
  dq_wap_dag.py Phase 7  — Example Airflow DAG (write->audit->publish, quarantine, alert)
"""
from .config import Paths, build_spark, make_tags, shutdown_spark
from .exceptions import DataQualityError, DataQualityWarning
from .wap import WAPGate, AuditOutcome, apply_delta_constraints

__all__ = [
    "Paths",
    "build_spark",
    "make_tags",
    "shutdown_spark",
    "DataQualityError",
    "DataQualityWarning",
    "WAPGate",
    "AuditOutcome",
    "apply_delta_constraints",
]
