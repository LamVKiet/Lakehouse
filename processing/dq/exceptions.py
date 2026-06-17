"""
Exceptions — split the two failure classes so Airflow retries correctly (Phase 7).

  DataQualityError  -> DATA fail: the data violates a rule. Retry is POINTLESS.
                       Map to AirflowFailException (fail now, skip retry).
  Any other error (OOM, S3 auth, jar)  -> INFRA fail: let a normal Exception
                       propagate so the task's `retries=N` will retry.
"""
from __future__ import annotations


class DataQualityError(Exception):
    """Gate Error: block publish, push the batch to quarantine. NO retry."""

    def __init__(self, status: str, failed: list[str], evidence_path: str):
        self.status = status
        self.failed = failed
        self.evidence_path = evidence_path
        super().__init__(
            f"DQ gate FAILED (status={status}). "
            f"Failed constraints: {failed}. Evidence: {evidence_path}"
        )


class DataQualityWarning(Warning):
    """Soft signal: does NOT block publish, only a low-level alert."""
