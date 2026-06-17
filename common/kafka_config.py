"""
Shared Kafka connection configuration.
Used by producer, consumers, and setup_topics to avoid duplicating broker config.
"""

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9094",
)

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
}

# Clickstream producer (aiokafka, async). acks=1 is enough for high-volume
# behavior events — leader ack only. NO idempotence (it requires acks=all);
# retries may create duplicates, deduped by event_uuid at Silver. lz4 + 50ms
# linger batch to reduce broker/network load.
CLICKSTREAM_PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": 1,
    "linger_ms": 50,
    "compression_type": "lz4",
    "request_timeout_ms": 30000,
}


def get_consumer_config(group_id: str) -> dict:
    """Return a Kafka consumer config dict for the given consumer group."""
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }
