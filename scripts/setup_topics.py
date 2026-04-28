"""
Create Kafka topics for the movie ticketing pipeline.
Run once on first cluster startup.

Topics:
  - behavior_events:    10 partitions, replication=2 (high volume, full funnel)
  - transaction_events:  3 partitions, replication=2 (low volume, max durability)
"""

from confluent_kafka.admin import AdminClient, NewTopic

from common.kafka_config import KAFKA_BOOTSTRAP_SERVERS


def create_topics():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    topics = [
        # Low volume, high priority — max replication for failover
        NewTopic("transaction_events", num_partitions=3, replication_factor=2),
        # High volume (~95% traffic) — more partitions for throughput
        NewTopic("behavior_events", num_partitions=10, replication_factor=2),
    ]

    fs = admin_client.create_topics(topics)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "TopicExistsError" in str(e) or "already exists" in str(e):
                print(f"Topic '{topic}' already exists.")
            else:
                print(f"Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    create_topics()
