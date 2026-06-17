"""
Airflow DAG: Debezium CDC -> Bronze (transaction tables only).
Schedule: @hourly. Runs streaming_cdc_to_bronze.py with trigger(availableNow=True)
— drains all CDC events accumulated since the last run, then stops.
Dimensions + Silver + Gold stay on the daily T-1 batch path.
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


def create_spark_task(dag, python_file, topic_name):
    # Tên task trên Airflow (bỏ tiền tố dbz.apparel_retail để tên ngắn gọn)
    short_topic = topic_name.split(".")[-1]
    task_id = f"cdc_{short_topic}"
    job_path = os.path.join(JOB_BASE, python_file)
    
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"{SPARK_HOME}/bin/spark-submit "
            f"--master {SPARK_MASTER} "
            "--deploy-mode client "
            "--driver-memory 512m "
            "--executor-memory 3g "
            "--executor-cores 4 "
            "--total-executor-cores 4 "
            "--conf spark.executor.memoryOverhead=256m "
            "--conf spark.driver.host=airflow "
            f"{job_path} {topic_name}"   # Truyền topic_name vào làm tham số (sys.argv[1])
        ),
        dag=dag,
    )


############################## DAG CONFIG
dag = DAG(
    dag_id="cdc_bronze_hourly",
    tags=["bronze", "cdc", "hourly"],
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=15),
    },
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
)

CDC_TOPICS = [
    "dbz.apparel_retail.pos_transactions",
    "dbz.apparel_retail.online_transactions",
    "dbz.apparel_retail.pos_transaction_details",
    "dbz.apparel_retail.online_transaction_details",
]

start = DummyOperator(task_id="START", dag=dag)
end = DummyOperator(task_id="END", dag=dag)

for topic in CDC_TOPICS:
    spark_task = create_spark_task(dag, "streaming_cdc_to_bronze.py", topic)
    start >> spark_task >> end
