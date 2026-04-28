"""
Airflow DAG: Weekly maintenance for Silver Delta tables.
Schedule: Sundays 04:00 (Asia/Ho_Chi_Minh) — runs OPTIMIZE + VACUUM on all silver tables.
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
            "--executor-cores 1 "
            "--total-executor-cores 2 "
            "--conf spark.executor.memoryOverhead=512m "
            f"{job_path}"
        ),
        dag=dag,
    )


############################## DAG CONFIG
dag = DAG(
    dag_id="silver_maintenance_weekly",
    tags=["silver", "maintenance", "weekly"],
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=2),
    },
    schedule_interval="0 4 * * 0",  # Sundays 04:00 UTC+7
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(task_id="START", dag=dag)
maintain_silver = create_spark_task(dag, "maintain_silver.py")
end   = DummyOperator(task_id="END", dag=dag)

start >> maintain_silver >> end
