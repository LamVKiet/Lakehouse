"""
Phase 4 — MetricsRepository on object storage (history -> trend; the MANDATORY base for anomaly).

FileSystemMetricsRepository writes JSON to s3a://. WARNING: it collects every result into ONE
file -> use ONE file per table (config.Paths.metrics); don't let all tables share one
(bloat + last-write-wins under concurrent writes).

Tags (Phase 0 make_tags) are the ONLY dimension to query back by — keep the convention fixed.
"""
from __future__ import annotations

from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pyspark.sql import DataFrame, SparkSession


def get_repository(spark: SparkSession, metrics_path: str) -> FileSystemMetricsRepository:
    """Repository pointing straight at one table's metric file on object storage."""
    return FileSystemMetricsRepository(spark, metrics_path)


def make_key(spark: SparkSession, tags: dict) -> ResultKey:
    """Identity of a single run = timestamp + tags."""
    return ResultKey(spark, ResultKey.current_milli_time(), tags)


def load_history(
    spark: SparkSession,
    repo: FileSystemMetricsRepository,
    *,
    table: str,
    layer: str,
    after_ms: int | None = None,
) -> DataFrame:
    """
    Read back the metric time series -> source for the trend dashboard & anomaly baseline.
    Returns a DataFrame: entity | instance | name | value | <tags...>
    """
    loader = repo.load().withTagValues({"table": table, "layer": layer})
    if after_ms is not None:
        loader = loader.after(after_ms)
    return loader.getSuccessMetricsAsDataFrame(spark)
