"""
Airflow DAG: Silver -> Gold (4 aggregation tables).
Schedule: 03:00 UTC+7 daily (runs after bronze_to_silver_daily completes).
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

LOCAL_DS = '{{ (execution_date + macros.timedelta(hours=7)).strftime("%Y-%m-%d") }}'


def create_spark_task(dag, python_file):
    task_id = python_file.replace(".py", "")
    job_path = os.path.join(JOB_BASE, python_file)
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"{SPARK_HOME}/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            "--driver-memory 512m "
            "--executor-memory 512m "
            "--executor-cores 1 "
            "--total-executor-cores 2 "
            "--conf spark.executor.memoryOverhead=256m "
            f"{job_path} {LOCAL_DS} "
        ),
        dag=dag,
    )


############################## DAG CONFIG
dag = DAG(
    dag_id="silver_to_gold_daily",
    tags=["gold", "batch", "daily"],
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
)

start = DummyOperator(task_id="START", dag=dag)
gold_events_discovery = create_spark_task(dag, "batch_gold_events_discovery.py")
gold_events_cart      = create_spark_task(dag, "batch_gold_events_cart.py")
gold_events_checkout  = create_spark_task(dag, "batch_gold_events_checkout.py")
gold_transactions     = create_spark_task(dag, "batch_gold_transactions.py")
end   = DummyOperator(task_id="END", dag=dag)

start >> gold_events_discovery >> end
start >> gold_events_cart      >> end
start >> gold_events_checkout  >> end
start >> gold_transactions     >> end
