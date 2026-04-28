"""
Behavior events consumer: reads from the behavior_events topic.

Runs two consumer groups in parallel threads:
  - datalake-sync:   batches events for S3/HDFS storage (simulated)
  - ai-recommender:  real-time push notification on movie views
"""

import json
import threading
from confluent_kafka import Consumer
from common.kafka_config import get_consumer_config


class Colors:
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    RESET = "\033[0m"


def datalake_sync():
    """Thread 1: Simulate batching events to S3/HDFS (every 20 events)."""
    consumer = Consumer(get_consumer_config("datalake-sync"))
    consumer.subscribe(["behavior_events"])
    print(f"{Colors.BLUE}[Data Lake] Started syncing raw data to S3...{Colors.RESET}")
    count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            count += 1
            if count % 20 == 0:
                print(f"{Colors.CYAN}[Data Lake] Synced {count} events to S3 bucket{Colors.RESET}")
    except Exception as e:
        print(f"Error in Data Lake Sync: {e}")
    finally:
        consumer.close()


def ai_recommender():
    """Thread 2: Real-time movie recommendation based on user browsing."""
    consumer = Consumer(get_consumer_config("ai-recommender"))
    consumer.subscribe(["behavior_events"])
    print(f"{Colors.YELLOW}[AI Engine] Recommender System online...{Colors.RESET}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value().decode("utf-8"))
            event = data.get("event_type")
            user = data.get("user_id")
            if event == "view_movie_details":
                movie_name = data.get("metadata", {}).get("movie_name", "Unknown")
                print(f"{Colors.YELLOW}[AI Engine] User {user} viewing '{movie_name}' -> Push: '20% off popcorn combo!'{Colors.RESET}")
    except Exception as e:
        print(f"Error in AI Recommender: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    t1 = threading.Thread(target=datalake_sync, daemon=True)
    t2 = threading.Thread(target=ai_recommender, daemon=True)
    t1.start()
    t2.start()
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("Stopping all Behavior Consumers...")
