"""
Transaction events consumer: reads from the transaction_events topic.

Runs two consumer groups in parallel threads:
  - notification-service:  SMS/Email on ticket booking or payment failure
  - dwh-dashboard:         real-time revenue aggregation for management
"""

import json
import threading
from confluent_kafka import Consumer
from common.kafka_config import get_consumer_config


class Colors:
    HEADER = "\033[95m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


TICKET_PRICE = 150_000  # VND per ticket


def notification_service():
    """Thread 1: Send SMS/Email based on transaction outcomes."""
    consumer = Consumer(get_consumer_config("notification-service"))
    consumer.subscribe(["transaction_events"])
    print(f"{Colors.HEADER}[Notification] Started waiting for checkout events...{Colors.RESET}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value().decode("utf-8"))
            event = data.get("event_type")
            user = data.get("user_id")
            meta = data.get("metadata", {})
            movie_name = meta.get("movie_name", "Unknown")
            if event == "ticket_booked" and meta.get("is_success") == 1:
                seats = meta.get("seat_numbers", [])
                print(f"{Colors.GREEN}[SMS] Sent to {user}: Tickets for {movie_name} confirmed! Seats: {seats}{Colors.RESET}")
            elif event == "payment_failed":
                reason = meta.get("error_message", "Unknown error")
                print(f"{Colors.RED}[Email] Sent to {user}: Payment failed for {movie_name}. Reason: {reason}{Colors.RESET}")
    except Exception as e:
        print(f"Error in Notification: {e}")
    finally:
        consumer.close()


def dwh_dashboard():
    """Thread 2: Aggregate revenue in real-time for management dashboard."""
    consumer = Consumer(get_consumer_config("dwh-dashboard"))
    consumer.subscribe(["transaction_events"])
    print(f"{Colors.HEADER}[Dashboard] Started tracking revenue...{Colors.RESET}")
    revenue = 0
    tickets_sold = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            data = json.loads(msg.value().decode("utf-8"))
            if data.get("event_type") == "ticket_booked" and data.get("metadata", {}).get("is_success") == 1:
                seats = len(data.get("metadata", {}).get("seat_numbers", []))
                tickets_sold += seats
                revenue += seats * TICKET_PRICE
                print(f"{Colors.BOLD}[Dashboard] Revenue: {revenue:,} VND | Tickets: {tickets_sold}{Colors.RESET}")
    except Exception as e:
        print(f"Error in DWH Dashboard: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    t1 = threading.Thread(target=notification_service, daemon=True)
    t2 = threading.Thread(target=dwh_dashboard, daemon=True)
    t1.start()
    t2.start()
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("Stopping all Transaction Consumers...")
