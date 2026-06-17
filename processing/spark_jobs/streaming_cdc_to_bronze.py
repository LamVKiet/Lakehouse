"""
Spark Structured Streaming: Debezium CDC (MySQL) -> S3/Delta Lake Bronze.

Scope: TRANSACTION tables only (pos/online headers + details). Dimensions
(customers/category/products/branches) stay on the T-1 batch path; Silver/Gold
also remain T-1 batch. This job runs an hourly micro-batch (1h trigger).

One streaming query per CDC topic -> each MySQL table lands into its OWN Bronze
Delta table (1:1, NOT merged — pos and online stay separate; centralization
happens later at Silver). Each query owns its own checkpoint folder, so
checkpoints are organized per table under CHECKPOINT_BASE.

Option A parsing (keep raw, defer flattening to Silver):
  - `after` kept as raw JSON string (after_payload) — NOT flattened to typed cols
  - CDC metadata: op (c/u/d/r), ts_ms, source_ts_ms
  - Kafka metadata: _kafka_topic, _kafka_partition, _kafka_offset, _kafka_timestamp, _ingested_at
  - Append-only. Bronze is immutable + keeps full history, so `before` is dropped
    (every new state is captured via `after`; op='d' marks deletes). Dedup -> Silver.
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql.functions import col, current_timestamp, date_format
from processing.spark_jobs.delta_utils import get_spark_session, get_s3_path, register_glue_table, write_delta_append, enable_auto_optimize

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19094,kafka-3:19096")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/checkpoints/cdc")

CDC_TOPICS = [
    "dbz.apparel_retail.pos_transactions",
    "dbz.apparel_retail.online_transactions",
    "dbz.apparel_retail.pos_transaction_details",
    "dbz.apparel_retail.online_transaction_details",
]


def parse_cdc(batch_df):
    """Keep Debezium envelope as raw JSON payload + Kafka metadata."""
    return (
        batch_df.select(
            col("value").cast("string").alias("payload"),
            col("topic").alias("_kafka_topic"),
            col("partition").cast("short").alias("_kafka_partition"),
            col("offset").cast("long").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("report_date", date_format(col("_kafka_timestamp"), "yyyyMMdd").cast("int"))
    )


def wait_for_kafka(max_retries=60, delay=5):
    import socket
    host, port = KAFKA_SERVERS.split(",")[0].rsplit(":", 1)
    port = int(port)
    for i in range(max_retries):
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"Kafka broker {host}:{port} is reachable")
                return
        except (ConnectionRefusedError, socket.timeout, OSError):
            print(f"Waiting for Kafka... ({i + 1}/{max_retries})")
            time.sleep(delay)
    raise ConnectionError("Kafka not available after retries")


def main():
    print("=" * 60)
    print("SPARK STREAMING: Debezium CDC -> S3/Delta Bronze")
    print("=" * 60)

    wait_for_kafka()

    spark = get_spark_session("ApparelRetail-CDCToBronze")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    spark.conf.set("spark.sql.shuffle.partitions", "8")


    kafka_options = {
        "kafka.bootstrap.servers": KAFKA_SERVERS,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "5000000",   # bound micro-batch size while draining backlog
        "minPartitions": "16",              # Force 16 Spark tasks per Kafka partition to prevent idle cores
    }

    topic = sys.argv[1] if len(sys.argv) > 1 else "dbz.apparel_retail.online_transactions"
    table = topic.split(".")[-1]
    
    print(f"Running in SINGLE TOPIC mode for: {topic}")
    
    stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )
    
    bronze_stream = parse_cdc(stream)

    path = get_s3_path("bronze", table)

    query = (
        bronze_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table}")
        .partitionBy("report_date")
        .trigger(availableNow=True)
        .start(path)
    )

    print(f"Stream started: {topic} -> bronze/{table} (checkpoint: {CHECKPOINT_BASE}/{table})")
    query.awaitTermination()

    print(f"\nCDC batch complete for topic {topic}.\n")
    print(f"Registering table {table} to Glue Catalog...")

if __name__ == "__main__":
    main()
