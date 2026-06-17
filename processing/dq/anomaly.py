"""
Phase 5 — AnomalyDetection (unknown-unknowns).

Pure STATISTICS (rolling mean/stddev, rate-of-change, Holt-Winters), NOT an ML model
-> explainable to an auditor. Compares today's metric against its OWN history in the repository
(so it must run after Phase 4).

WARNING: anomaly = Warning, rarely a hard-block (a real holiday dip is CORRECT). Cold start is
noisy -> accumulate a baseline over a few weeks. Backfill skews the baseline -> tag it separately to exclude.
"""
from __future__ import annotations

from pydeequ.analyzers import Completeness, Mean, Size, Sum
from pydeequ.anomaly_detection import (
    OnlineNormalStrategy,
    RelativeRateOfChangeStrategy,
)
from pydeequ.verification import VerificationSuite


def add_anomaly_checks(suite: VerificationSuite, *, mean_col: str | None = None,
                       sum_col: str | None = None, completeness_col: str | None = None) -> VerificationSuite:
    """
    Attach anomaly checks to the suite (the suite must already .useRepository(repo) -> compare vs history).
      - Size -> RelativeRateOfChange: volume varies with scale, a % is more robust than absolute (ALWAYS on).
      - Mean -> OnlineNormal: catches subtle drift (a money-unit change) via the mean±k·stddev band.
      - Sum  -> RelativeRateOfChange: total amount (revenue) drop/spike by %.
      - Completeness -> OnlineNormal: null-rate drift (only on when a column is passed).
    addAnomalyCheck(strategy, analyzer): strategy = 'what counts as anomalous', analyzer = 'what to measure'.
    Anomaly = CheckLevel.Warning (Deequ default) -> NOT a hard-block, only surface + alert.
    """
    # Volume: keep >=70% and grow <=150% vs baseline to be OK
    suite = suite.addAnomalyCheck(
        RelativeRateOfChangeStrategy(maxRateDecrease=0.7, maxRateIncrease=1.5),
        Size(),
    )
    if completeness_col:
        suite = suite.addAnomalyCheck(
            OnlineNormalStrategy(lowerDeviationFactor=3.0, upperDeviationFactor=3.0),
            Completeness(completeness_col),
        )
    if mean_col:
        suite = suite.addAnomalyCheck(
            OnlineNormalStrategy(lowerDeviationFactor=3.0, upperDeviationFactor=3.0),
            Mean(mean_col),
        )
    if sum_col:
        suite = suite.addAnomalyCheck(
            RelativeRateOfChangeStrategy(maxRateDecrease=0.5, maxRateIncrease=2.0),
            Sum(sum_col),
        )
    return suite
