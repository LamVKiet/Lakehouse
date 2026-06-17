"""
Spark Structured Streaming: Kafka behavior_events -> S3/Delta Lake Bronze layer.

Reads the behavior_events topic and writes raw events to a Delta table on S3,
partitioned by report_date (derived from the Kafka record timestamp).

Key design decisions:
  - whole event JSON kept as a raw string (payload) — Bronze is immutable, never flattened
  - Kafka metadata (_kafka_topic, _kafka_partition, _kafka_offset, _kafka_timestamp)
    preserved for targeted reprocessing if needed
  - foreachBatch with 30-second trigger for micro-batch control
  - Delta Optimized Write + Auto Compact (table properties) auto-bin-pack files at full
    write parallelism — no manual repartition that would throttle high-throughput batches
  - maxOffsetsPerTrigger bounds batch size so a post-downtime backlog can't OOM the first batch
  - Checkpoint on a named Docker volume for crash recovery
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql.functions import col, current_timestamp, date_format
from processing.spark_jobs.delta_utils import get_spark_session, get_s3_path, register_glue_table, write_delta_append, enable_auto_optimize

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19094,kafka-3:19096")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/checkpoints/bronze")


def parse_to_bronze(batch_df):
    """Parse Kafka message value and keep as raw JSON payload."""
    return (
        batch_df
        .select(
            col("value").cast("string").alias("payload"),
            col("topic").alias("_kafka_topic"),
            col("partition").cast("int").alias("_kafka_partition"),
            col("offset").cast("long").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
        )
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("report_date", date_format(col("_kafka_timestamp"), "yyyyMMdd"))
    )


def make_write_batch(spark, topic_name):
    """Return a foreachBatch handler for the given topic."""
    def write_batch(batch_df, epoch_id):
        if batch_df.isEmpty():
            return
        bronze_df = parse_to_bronze(batch_df)
        path = get_s3_path("bronze", topic_name)
        write_delta_append(bronze_df, path, partition_by="report_date")
        if epoch_id == 0:
            enable_auto_optimize(spark, path)
            register_glue_table(spark, "bronze", topic_name, path)
        print(f"[Batch {epoch_id}] Wrote partition to bronze/{topic_name}")
    return write_batch


def wait_for_kafka(max_retries=60, delay=5):
    """Wait for Kafka brokers to be reachable before starting streams."""
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
    print("SPARK STREAMING: Kafka -> S3/Delta Bronze")
    print("=" * 60)

    wait_for_kafka()

    spark = get_spark_session("MovieTicketing-StreamToBronze")
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")

    kafka_options = {
        "kafka.bootstrap.servers": KAFKA_SERVERS,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "1400000",
        "minPartitions": "16",
    }

    from delta.tables import DeltaTable
    queries = []
    for topic in ["behavior_events"]:
        path = get_s3_path("bronze", topic)
        if DeltaTable.isDeltaTable(spark, path):
            enable_auto_optimize(spark, path)
        stream = (
            spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .option("subscribe", topic)
            .load()
        )
        query = (
            stream.writeStream
            .foreachBatch(make_write_batch(spark, topic))
            .outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{topic}")
            .trigger(processingTime="30 seconds")
            .start()
        )
        queries.append(query)
        print(f"Stream started: {topic} -> bronze/{topic}")

    print("\nStreaming is running. Press Ctrl+C to stop.\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
