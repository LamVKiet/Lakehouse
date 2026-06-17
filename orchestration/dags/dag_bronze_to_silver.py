"""
Airflow DAG: Bronze -> Silver (behavior events + future SQL-sourced data).
Schedule: 02:30 UTC+7 daily (runs after bronze_mysql_daily completes).
"""

import json
import os
import subprocess
import boto3
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Production alerting — on-call inbox for silver.transactions WAP failures (needs [smtp] in airflow.cfg).
ALERT_EMAIL = ["lamkiet345678@gmail.com"]
S3_BUCKET = os.getenv("S3_BUCKET", "")
# Sentinel JSON written by the job on a DATA fail; the alert reads + renders it into the mail (no Spark/driver log needed).
STATUS_KEY = "dq/status/silver/transactions/{run_date}.json"
# Archive the sent mail content (audit trail) — re-readable when needed.
ALERT_ARCHIVE_KEY = "dq/alerts/silver/transactions/{run_date}.html"

SPARK_HOME   = os.getenv("SPARK_HOME", "/opt/spark")
SPARK_MASTER = "spark://spark-master:7077"
JOB_BASE     = "/app/processing/spark_jobs"

LOCAL_DS = '{{ (execution_date + macros.timedelta(hours=7)).strftime("%Y-%m-%d") }}'

# DQ gate exit code emitted by batch_silver_transactions_cdc.py (phase=audit) on a
# data-quality failure -> map to AirflowFailException (no retry). Keep in sync with the job.
EXIT_DQ_FAIL = 42

SPARK_SUBMIT = (
    f"{SPARK_HOME}/bin/spark-submit --master {SPARK_MASTER} --deploy-mode client "
    "--driver-memory 512m --executor-memory 512m --executor-cores 1 "
    "--total-executor-cores 2 --conf spark.executor.memoryOverhead=256m"
)


def create_spark_task(dag, python_file, extra_args="", task_id=None, **task_kwargs):
    task_id = task_id or python_file.replace(".py", "")
    job_path = os.path.join(JOB_BASE, python_file)
    return BashOperator(
        task_id=task_id,
        bash_command=f"{SPARK_SUBMIT} {job_path} {LOCAL_DS} {extra_args} ",
        dag=dag,
        **task_kwargs,
    )


def _local_ds(context) -> str:
    dt = context.get("logical_date") or context["execution_date"]
    return (dt + timedelta(hours=7)).strftime("%Y-%m-%d")


def _run_dq_phase(phase, context):
    """Run a transactions DQ gate phase via spark-submit and split the failure mode:
      - DATA fail  -> AirflowFailException (no retry); keyed on exit 42 OR the DQ_GATE_FAILED
        marker, since spark-submit may normalize the driver exit code to 1.
      - INFRA fail -> RuntimeError (retries apply)."""
    job = os.path.join(JOB_BASE, "batch_silver_transactions_cdc.py")
    res = subprocess.run(f"{SPARK_SUBMIT} {job} {_local_ds(context)} {phase}",
                         shell=True, capture_output=True, text=True)
    print(res.stdout)
    print(res.stderr)
    if res.returncode == EXIT_DQ_FAIL or "DQ_GATE_FAILED" in res.stdout:
        raise AirflowFailException(f"silver.transactions DQ {phase} FAILED -> batch quarantined, no retry.")
    if res.returncode != 0:
        raise RuntimeError(f"silver.transactions {phase} infra failure (rc={res.returncode}) -> will retry.")


def run_dq_audit(**context):
    _run_dq_phase("audit", context)


def run_dq_reconcile(**context):
    """Cross-table reconcile (header<->detail control total + customer_id/branch_id FK). Same
    no-retry-on-data-fail semantics as audit (exit 42 / DQ_GATE_FAILED)."""
    _run_dq_phase("reconcile", context)


def _read_status_summary(run_date):
    """Read the JSON sentinel the job writes on a DATA fail (boto3, no Spark needed). None on an INFRA
    fail (the job never wrote a summary) -> the mail is still sent, just without the breakdown."""
    try:
        obj = boto3.client("s3").get_object(Bucket=S3_BUCKET, Key=STATUS_KEY.format(run_date=run_date))
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[DQ ALERT] no status summary for {run_date} ({e}) — likely INFRA fail.")
        return None


def _render_email(ds, dag_id, failed_tasks, summary):
    head = (f"<h3>[DQ ALERT] silver.transactions WAP failed for {ds}</h3>"
            f"<p><b>DAG:</b> {dag_id}<br><b>Run date:</b> {ds}<br>"
            f"<b>Failed task(s):</b> {', '.join(failed_tasks) or 'unknown'}</p>")
    if not summary:
        return head + ("<p><i>No DATA summary found — likely an INFRA failure "
                       "(OOM / S3 / Bronze missing). Check the Airflow task log + Spark History.</i></p>")
    rows = "".join(f"<tr><td>{r}</td><td align='right'>{c}</td></tr>"
                   for r, c in summary.get("reason_counts", {}).items())
    samples = ", ".join(summary.get("samples", [])) or "-"
    return head + (
        f"<p><b>Phase:</b> {summary.get('phase')} &nbsp; "
        f"<b>Quarantined:</b> {summary.get('total_quarantined')} / {summary.get('total_rows')} rows</p>"
        f"<table border='1' cellpadding='4' cellspacing='0'>"
        f"<tr><th align='left'>failure_reason</th><th>count</th></tr>{rows}</table>"
        f"<p><b>Sample trans_id:</b> {samples}</p>"
        f"<p>Full rows: <code>dq/quarantine/silver/transactions(_reconcile)/run_date={summary.get('run_date')}</code> "
        f"· evidence: <code>dq/evidence/silver/transactions</code></p>")


def _archive_email(run_date, subject, html):
    """Archive the rendered mail content to S3 (audit trail) — best-effort."""
    try:
        body = f"<!-- subject: {subject} -->\n{html}"
        boto3.client("s3").put_object(Bucket=S3_BUCKET, Key=ALERT_ARCHIVE_KEY.format(run_date=run_date),
                                      Body=body.encode("utf-8"), ContentType="text/html")
        print(f"[DQ ALERT] mail content archived -> {ALERT_ARCHIVE_KEY.format(run_date=run_date)}")
    except Exception as e:
        print(f"[DQ ALERT] archive failed ({e}) — alert still sent/logged.")


def alert_dq_failure(**context):
    """Fires when any silver.transactions WAP stage fails (one_failed). Reads the job's S3 status
    summary, renders a failure breakdown into the email + log, archives the mail body to S3, then
    sends. Email send is best-effort — a misconfigured [smtp] must not turn the alert task itself
    red and mask the real upstream failure."""
    ds = _local_ds(context)
    run_date = ds.replace("-", "")
    ti = context.get("task_instance")
    dag_id = context["dag"].dag_id
    failed = [t.task_id for t in (ti.get_dagrun().get_task_instances() if ti else [])
              if t.state == "failed"]
    summary = _read_status_summary(run_date)
    subject = f"[Airflow][{dag_id}] silver.transactions FAILED for {ds}"
    html = _render_email(ds, dag_id, failed, summary)
    print(f"[DQ ALERT] silver.transactions WAP failed for {ds} failed_tasks={failed} "
          f"summary={summary.get('reason_counts') if summary else 'none'}")
    _archive_email(run_date, subject, html)
    try:
        send_email(to=ALERT_EMAIL, subject=subject, html_content=html)
        print(f"[DQ ALERT] failure email sent to {ALERT_EMAIL}.")
    except Exception as e:
        print(f"[DQ ALERT] email send failed ({e}) — check airflow.cfg [smtp]. Alert still logged.")


############################## DAG CONFIG
dag = DAG(
    dag_id="bronze_to_silver_daily",
    tags=["silver", "batch", "daily"],
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    schedule_interval="30 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
)

start  = DummyOperator(task_id="START", dag=dag)
# silver_events       = create_spark_task(dag, "batch_silver_events.py")
# silver_customers    = create_spark_task(dag, "batch_silver_customers.py")
# silver_category     = create_spark_task(dag, "batch_silver_category.py")
# silver_products     = create_spark_task(dag, "batch_silver_products.py")
# silver_branches     = create_spark_task(dag, "batch_silver_branches.py")
# silver_nou                       = create_spark_task(dag, "batch_silver_nou.py")
# silver.transactions — WAP gate: write(staging) -> audit(Deequ) -> reconcile(cross-table) -> publish(MERGE)
txn_write     = create_spark_task(dag, "batch_silver_transactions_cdc.py", "write",
                                  task_id="dq_write_transactions")
txn_audit     = PythonOperator(task_id="dq_audit_transactions", python_callable=run_dq_audit, dag=dag)
txn_reconcile = PythonOperator(task_id="dq_reconcile_transactions", python_callable=run_dq_reconcile, dag=dag)
txn_publish   = create_spark_task(dag, "batch_silver_transactions_cdc.py", "publish",
                                  task_id="dq_publish_transactions")
txn_alert     = PythonOperator(task_id="dq_alert_transactions", python_callable=alert_dq_failure,
                               trigger_rule=TriggerRule.ONE_FAILED, dag=dag)
# silver_transaction_details       = create_spark_task(dag, "batch_silver_transaction_details.py")
# silver_customer_activity_monthly = create_spark_task(dag, "batch_silver_customer_activity_monthly.py")
end    = DummyOperator(task_id="END", dag=dag)

# Parallel: unified events job + dim tables; sequential: category -> products (JOIN)
# silver_transaction_details denormalizes silver.products (category_id + unit_price) -> products must run first
# start >> silver_events >> end
# start >> silver_customers >> end
# start >> silver_category >> silver_products >> end
# silver_products >> silver_transaction_details >> end
# start >> silver_branches >> end
# start >> silver_nou >> end
# WAP chain: bad batch never reaches prod. audit fail -> publish auto-skip (all_success) + alert.
# alert (one_failed, leaf) fires on a failure of ANY WAP stage; not wired to END on purpose —
# it is skipped on success, so linking it would skip END's all_success on every good run.
start >> txn_write >> txn_audit >> txn_reconcile >> txn_publish >> end
[txn_write, txn_audit, txn_reconcile, txn_publish] >> txn_alert
# start >> silver_customer_activity_monthly >> end
