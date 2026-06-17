"""
Phase 1 — Analyzers + AnalysisRunner (the MEASURE layer).

An analyzer only produces a NUMBER (metric), it does not judge pass/fail. AnalysisRunner folds
many analyzers into a single pass: narrow ones share one .agg() (1 job, 0 shuffle); wide ones
(exact Uniqueness/Distinctness/Entropy) cost 1 shuffle per KEY SET.

Cost rule: cost = the number of DISTINCT analyzers, not the number of constraints; narrow is
nearly free when folded together, wide is what you must count and approximate.
"""
from __future__ import annotations

from pydeequ.analyzers import (
    AnalysisRunner,
    AnalyzerContext,
    ApproxCountDistinct,
    Completeness,
    Compliance,
    Distinctness,
    Maximum,
    Mean,
    Minimum,
    PatternMatch,
    Size,
    StandardDeviation,
    Sum,
    Uniqueness,
)
from pyspark.sql import DataFrame, SparkSession


def narrow_analyzers(numeric_cols: list[str], key_cols: list[str]) -> list:
    """
    NARROW group — I/O-bound, all folded into one pass. Adding more is ~free.
    (Completeness / Compliance / PatternMatch / Approx* / Mean-Min-Max-Sum-StdDev)
    """
    analyzers: list = [Size()]
    for c in numeric_cols:
        analyzers += [
            Completeness(c),
            Sum(c),
            Mean(c),
            Minimum(c),
            Maximum(c),
            StandardDeviation(c),
            Compliance(f"{c}_non_negative", f"{c} >= 0"),
        ]
    for k in key_cols:
        analyzers.append(ApproxCountDistinct(k))  # HLL: estimated distinct, NO shuffle
    return analyzers


def wide_analyzers(exact_key_cols: list[list[str]]) -> list:
    """
    WIDE group — shuffle-bound, use SPARINGLY and only for tier-1/gold tables.
    Each distinct KEY SET = its own shuffle. Group checks on the same key to share the shuffle.
    """
    out: list = []
    for key in exact_key_cols:
        out += [Uniqueness(key), Distinctness(key)]
    return out


def run_analysis(spark: SparkSession, df: DataFrame, analyzers: list) -> DataFrame:
    """Run one pass for the analyzer list, returning metrics as a DataFrame (entity|instance|name|value)."""
    ctx = AnalysisRunner(spark).onData(df)
    for a in analyzers:
        ctx = ctx.addAnalyzer(a)
    result = ctx.run()
    return AnalyzerContext.successMetricsAsDataFrame(spark, result)
