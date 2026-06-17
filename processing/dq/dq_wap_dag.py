"""
Phase 7 — Airflow orchestration (reference example).

DAG:  read_transform >> write_staging >> audit >> publish
                                           |
                                           └──> alert  (trigger_rule='one_failed')

- audit raises DataQualityError (DATA fail) -> map to AirflowFailException: fail NOW, skip retry.
- publish downstream all_success -> if audit raises, publish AUTO-skips (no extra code needed).
- alert one_failed -> read XCom status + evidence, fire Slack/PagerDuty with the broken constraints.
- INFRA fail (OOM/auth/jar) -> a normal Exception -> the task's retries=2 will retry.
- Every step is idempotent by run_date -> re-running after the upstream fix is clean.

Warning: does NOT raise, does NOT quarantine -> publish + a light alert.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from processing.dq.anomaly import add_anomaly_checks
from processing.dq.checks import gold_hard_check, gold_soft_check
from processing.dq.config import Paths, build_spark, shutdown_spark
from processing.dq.exceptions import DataQualityError
from processing.dq.wap import WAPGate

PIPELINE = "gold_transactions"
PATHS = Paths(table="transactions", layer="gold")


def _gate(run_date: str) -> tuple:
    spark = build_spark(f"{PIPELINE}-{run_date}")
    return spark, WAPGate(spark, PATHS, run_date, pipeline=PIPELINE)


# --- TASKS -------------------------------------------------------------------
def read_transform(ds: str, **_):
    spark, gate = _gate(ds)
    try:
        # ... your source read + transform -> df_gold (already has run_date = ds)
        df_gold = spark.read.format("delta").load(PATHS.staging)  # placeholder
        gate.write_staging(df_gold)
    finally:
        shutdown_spark(spark)


def audit(ds: str, ti=None, **_):
    spark, gate = _gate(ds)
    try:
        outcome = gate.audit(
            checks=[gold_hard_check(spark), gold_soft_check(spark)],
            with_anomaly=lambda s: add_anomaly_checks(s, mean_col="amount"),
        )
        # XCom for alert/downstream
        ti.xcom_push(key="dq_status", value=outcome.status)
        ti.xcom_push(key="evidence", value=outcome.evidence_path)
    except DataQualityError as e:
        if ti is not None:
            ti.xcom_push(key="dq_status", value=e.status)
            ti.xcom_push(key="evidence", value=e.evidence_path)
            ti.xcom_push(key="failed", value=e.failed)
        # DATA fail -> fail now, NO retry
        raise AirflowFailException(str(e))
    finally:
        shutdown_spark(spark)


def publish(ds: str, **_):
    spark, gate = _gate(ds)
    try:
        gate.publish_replace_where()  # switch to publish_merge([...]) if you need upsert
    finally:
        shutdown_spark(spark)


def alert(ti=None, **_):
    status = ti.xcom_pull(key="dq_status", task_ids="audit")
    failed = ti.xcom_pull(key="failed", task_ids="audit")
    evidence = ti.xcom_pull(key="evidence", task_ids="audit")
    # ... send Slack/PagerDuty
    print(f"[DQ ALERT] status={status} failed={failed} evidence={evidence}")


# --- DAG ---------------------------------------------------------------------
with DAG(
    dag_id="gold_transactions_wap",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["dq", "wap", "gold"],
) as dag:
    t_read = PythonOperator(task_id="read_transform", python_callable=read_transform)
    t_audit = PythonOperator(task_id="audit", python_callable=audit)
    t_publish = PythonOperator(
        task_id="publish",
        python_callable=publish,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # audit fail -> skip publish
    )
    t_alert = PythonOperator(
        task_id="alert",
        python_callable=alert,
        trigger_rule=TriggerRule.ONE_FAILED,   # only runs when a task fails
    )

    t_read >> t_audit >> t_publish
    t_audit >> t_alert
