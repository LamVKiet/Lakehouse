"""
Spark Structured Streaming: Kafka -> S3/Delta Lake Bronze layer.

Reads from both behavior_events and transaction_events topics in parallel.
Writes raw events to Delta tables on S3, partitioned by log_date.

Key design decisions:
  - metadata is kept as a raw JSON string (raw_metadata) — Bronze is immutable
  - Kafka metadata (_kafka_topic, _kafka_partition, _kafka_offset) preserved
    for targeted reprocessing if needed
  - foreachBatch with 30-second trigger for micro-batch control
  - Checkpoint on a named Docker volume for crash recovery
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from pyspark.sql.functions import col, current_timestamp, from_json, to_json
from processing.schemas.event_schema import EVENT_SCHEMA
from processing.spark_jobs.delta_utils import get_spark_session, get_s3_path, register_glue_table, write_delta_append

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19094")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/checkpoints/bronze")


def parse_to_bronze(batch_df):
    """Parse Kafka message value (JSON) and enrich with Kafka metadata."""
    return (
        batch_df
        .select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
            col("topic").alias("_kafka_topic"),
            col("partition").cast("int").alias("_kafka_partition"),
            col("offset").cast("long").alias("_kafka_offset"),
        )
        .select(
            col("data.event_uuid"), col("data.event_id"), col("data.event_type"),
            col("data.timestamp").alias("event_ts"), col("data.log_date").cast("date").alias("log_date"),
            col("data.created_at"), col("data.session_id"), col("data.user_id"), col("data.device_os"), col("data.app_version"),
            to_json(col("data.metadata")).alias("raw_metadata"),  # Keep as raw JSON — Bronze is immutable source of truth
            col("_kafka_topic"), col("_kafka_partition"), col("_kafka_offset"),
            current_timestamp().alias("_ingested_at"),
        )
    )


def make_write_batch(spark, topic_name):
    """Return a foreachBatch handler for the given topic."""
    def write_batch(batch_df, epoch_id):
        if batch_df.isEmpty():
            return
        bronze_df = parse_to_bronze(batch_df)
        path = get_s3_path("bronze", topic_name)
        write_delta_append(bronze_df, path, partition_by="log_date")
        if epoch_id == 0:
            register_glue_table(spark, "bronze", topic_name, path)
        print(f"[Batch {epoch_id}] Wrote {bronze_df.count()} rows to bronze/{topic_name}")
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

    kafka_options = {
        "kafka.bootstrap.servers": KAFKA_SERVERS,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }

    queries = []
    for topic in ["behavior_events"]:
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
