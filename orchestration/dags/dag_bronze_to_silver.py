"""
Airflow DAG: Bronze -> Silver (behavior events + future SQL-sourced data).
Schedule: 02:30 UTC+7 daily (runs after bronze_mysql_daily completes).
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
silver_events_discovery = create_spark_task(dag, "batch_silver_events_discovery.py")
silver_events_cart      = create_spark_task(dag, "batch_silver_events_cart.py")
silver_events_checkout  = create_spark_task(dag, "batch_silver_events_checkout.py")
silver_customers    = create_spark_task(dag, "batch_silver_customers.py")
silver_category     = create_spark_task(dag, "batch_silver_category.py")
silver_products     = create_spark_task(dag, "batch_silver_products.py")
silver_branches     = create_spark_task(dag, "batch_silver_branches.py")
silver_nou          = create_spark_task(dag, "batch_silver_nou.py")
silver_transactions = create_spark_task(dag, "batch_silver_transactions.py")
end    = DummyOperator(task_id="END", dag=dag)

# Parallel: 3 events buckets + dim tables; sequential: category -> products (JOIN), nou -> transactions
start >> silver_events_discovery >> end
start >> silver_events_cart >> end
start >> silver_events_checkout >> end
start >> silver_customers >> end
start >> silver_category >> silver_products >> end
start >> silver_branches >> end
start >> silver_nou >> silver_transactions >> end
