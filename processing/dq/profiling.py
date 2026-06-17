"""
Phase 3 — Profiling + Suggestion (discovery, run AT onboarding / on schema change).

NOT part of the WAP runtime — this is a SEPARATE notebook/job run by hand while designing the
suite for an unfamiliar source. The suggestion output is a DRAFT for review, not an authority:
it learns rules FROM the data you give it -> garbage in, garbage rule; and it has no knowledge of
business semantics (reconciliation must be written by hand).
"""
from __future__ import annotations

from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import DEFAULT, ConstraintSuggestionRunner
from pyspark.sql import DataFrame, SparkSession


def profile_table(spark: SparkSession, df: DataFrame, cols: list[str] | None = None) -> dict:
    """
    Survey every column: completeness, approxDistinct, dataType (inferred from CONTENT), histogram.
    WARNING: this is Deequ's heaviest whole-column read. Limit columns + run on a single partition/sample.
    """
    if cols:
        df = df.select(*cols)
    result = ColumnProfilerRunner(spark).onData(df).run()

    out: dict = {}
    for name, p in result.profiles.items():
        entry = {
            "completeness": p.completeness,
            "approx_distinct": p.approximateNumDistinctValues,
            "data_type": p.dataType,
            "type_counts": getattr(p, "typeCounts", None),
            "histogram": getattr(p, "histogram", None),
        }
        if hasattr(p, "mean"):  # only present on numeric columns
            entry.update(
                minimum=p.minimum, maximum=p.maximum, mean=p.mean, std_dev=p.stdDev
            )
        out[name] = entry
    return out


def suggest_constraints(spark: SparkSession, df: DataFrame) -> list[dict]:
    """
    Generate candidate constraints + copy-pasteable PyDeequ code + the currently measured value.
    WARNING: profile over a KNOWN-GOOD window, not a random batch (else you freeze a bug into the standard).
    Correct flow: profile -> suggest -> DE/analyst review & hand-edit -> commit into checks.py.
    """
    suggestions = (
        ConstraintSuggestionRunner(spark)
        .onData(df)
        .addConstraintRule(DEFAULT())
        .run()
    )
    rows = []
    for s in suggestions["constraint_suggestions"]:
        rows.append(
            {
                "column": s["column_name"],
                "description": s["description"],
                "current_value": s.get("current_value"),
                "code": s["code_for_constraint"],  # e.g. '.isComplete("id")'
            }
        )
    return rows
