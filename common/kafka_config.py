"""
Shared Kafka connection configuration.
Used by producer, consumers, and setup_topics to avoid duplicating broker config.
"""

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9094,localhost:9096",
)

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
}


def get_consumer_config(group_id: str) -> dict:
    """Return a Kafka consumer config dict for the given consumer group."""
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }
