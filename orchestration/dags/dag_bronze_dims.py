"""
Airflow DAG: MySQL -> Bronze (dimension tables).
Schedule: 02:00 UTC+7 daily. Dims are independent — loaded in parallel.
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
            "--executor-memory 1536m "
            "--executor-cores 2 "
            "--total-executor-cores 2 "
            "--conf spark.executor.memoryOverhead=256m "
            f"{job_path} {LOCAL_DS} "
        ),
        dag=dag,
    )


############################## DAG CONFIG
dag = DAG(
    dag_id="bronze_dims_daily",
    tags=["bronze", "mysql", "dim", "daily"],
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    concurrency=2,
)

start = DummyOperator(task_id="START", dag=dag)

customers = create_spark_task(dag, "batch_bronze_customers.py")
products  = create_spark_task(dag, "batch_bronze_products.py")
branches  = create_spark_task(dag, "batch_bronze_branches.py")
category  = create_spark_task(dag, "batch_bronze_category.py")

end = DummyOperator(task_id="END", dag=dag)

# All dimensions are independent — load in parallel (2 at a time on the 4-core worker).
start >> [customers, products, branches, category] >> end
