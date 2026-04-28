"""
Airflow DAG: Backfill Silver (NOU + Transactions) — manual trigger only.
Run once after initial Bronze data is loaded.
"""

import os
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

SPARK_HOME   = os.getenv("SPARK_HOME", "/opt/spark")
SPARK_MASTER = "spark://spark-master:7077"
JOB_BASE     = "/app/processing/spark_jobs"


def create_spark_task(dag, python_file):
    task_id = python_file.replace(".py", "")
    job_path = os.path.join(JOB_BASE, python_file)
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"{SPARK_HOME}/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            "--driver-memory 1g "
            "--executor-memory 1g "
            "--executor-cores 2 "
            "--total-executor-cores 4 "
            "--conf spark.executor.memoryOverhead=512m "
            f"{job_path} "
        ),
        dag=dag,
    )


############################## DAG CONFIG
dag = DAG(
    dag_id="backfill_silver",
    tags=["silver", "backfill", "manual"],
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=60),
    },
    schedule_interval=None,   # manual trigger only
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(task_id="START", dag=dag)
nou_backfill          = create_spark_task(dag, "batch_silver_nou_backfill.py")
transactions_backfill = create_spark_task(dag, "batch_silver_transactions_backfill.py")
end   = DummyOperator(task_id="END", dag=dag)

start >> nou_backfill >> transactions_backfill >> end
