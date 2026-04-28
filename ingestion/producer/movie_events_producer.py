"""
Kafka producer: fetches clickstream events from the data-simulator API
and publishes them to the behavior_events Kafka topic.

All events go to behavior_events (full user journey tracking).
Transaction data is sourced from MySQL via batch ETL, not Kafka.
"""

import json
import os
import time
import random

import requests
from confluent_kafka import Producer
from common.kafka_config import PRODUCER_CONFIG

SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://data-simulator:8000")
producer = Producer(PRODUCER_CONFIG)


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def wait_for_simulator(max_retries=30, delay=3):
    """Block until the data-simulator service is healthy."""
    for i in range(max_retries):
        try:
            resp = requests.get(f"{SIMULATOR_URL}/health", timeout=5)
            if resp.status_code == 200:
                print("Simulator is ready")
                return
        except Exception:
            pass
        print(f"Waiting for simulator... ({i + 1}/{max_retries})")
        time.sleep(delay)
    raise ConnectionError("Simulator not available after retries")


def main():
    wait_for_simulator()
    print("Producer started. Press Ctrl+C to stop.")
    message_count = 0
    try:
        while True:
            resp = requests.get(f"{SIMULATOR_URL}/events/single", timeout=5)
            event = resp.json()
            value = json.dumps(event).encode("utf-8")
            key = event["user_id"].encode("utf-8")

            # All events go to behavior_events (full clickstream funnel)
            producer.produce(topic="behavior_events", key=key, value=value, callback=delivery_report)

            message_count += 1
            if message_count % 100 == 0:
                producer.flush()
                print(f"Sent {message_count} events")

            time.sleep(random.uniform(0.01, 0.2))
    except KeyboardInterrupt:
        print("Stopping producer")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
