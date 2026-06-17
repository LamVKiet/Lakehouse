"""
Register the Debezium MySQL CDC connector on Kafka Connect.
Run once after the stack is up:  python scripts/register_debezium.py

Captures only the 4 TRANSACTION tables (dimensions stay on the T-1 batch path).
Debezium emits one topic per table: dbz.apparel_retail.<table>.
Producer overrides (acks=all, idempotence, lz4, linger) tune durability + throughput.
"""
import json, os, urllib.request, urllib.error

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19094,kafka-3:19096")
DB = os.getenv("MYSQL_DB", "apparel_retail")
TABLES = ["pos_transactions", "pos_transaction_details",
          "online_transactions", "online_transaction_details"]

CONNECTOR = {
    "name": "mysql-apparel-retail",
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "tasks.max": "1",
        "database.hostname": os.getenv("MYSQL_HOST", "mysql"),
        "database.port": "3306",
        "database.user": os.getenv("MYSQL_USER", "app_user"),
        "database.password": os.getenv("MYSQL_PASSWORD", "app_pass"),
        "database.server.id": "184054",
        "topic.prefix": "dbz",
        "database.include.list": DB,
        "table.include.list": ",".join(f"{DB}.{t}" for t in TABLES),
        "schema.history.internal.kafka.bootstrap.servers": BOOTSTRAP,
        "schema.history.internal.kafka.topic": f"schema-history.{DB}",
        "topic.creation.default.replication.factor": "3",
        "topic.creation.default.partitions": "3",
        "snapshot.mode": "initial",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
        # Producer tuning (durability + throughput) — Debezium is the Kafka producer here
        "producer.override.acks": "all",
        "producer.override.enable.idempotence": "true",
        "producer.override.max.in.flight.requests.per.connection": "5",
        "producer.override.compression.type": "lz4",
        "producer.override.linger.ms": "50",
        "producer.override.batch.size": "32768",
    },
}


def register():
    data = json.dumps(CONNECTOR).encode()
    req = urllib.request.Request(
        f"{CONNECT_URL}/connectors", data=data, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            print(f"Connector '{CONNECTOR['name']}' registered: HTTP {resp.status}")
    except urllib.error.HTTPError as e:
        if e.code == 409:
            print(f"Connector '{CONNECTOR['name']}' already exists.")
        else:
            print(f"Failed: HTTP {e.code} - {e.read().decode()}")


if __name__ == "__main__":
    register()
