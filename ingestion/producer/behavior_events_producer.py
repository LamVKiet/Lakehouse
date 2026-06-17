"""
Async Kafka producer: fetches clickstream events from the data-simulator API
and publishes them to the behavior_events topic (full user journey tracking).

Reliability model (tuned for high-volume clickstream):
  - aiokafka with acks=1 (leader ack only) + lz4 + 50ms linger. NO idempotence.
  - Each send has a 1s timeout (asyncio.wait_for): if Kafka is slow/down, fail fast
    instead of blocking — the event is written to a local SQLite DLQ (not dropped),
    and the loop keeps going (equivalent to an inbound API returning 202).
  - A background retry loop drains the DLQ back into Kafka once it recovers.
Transaction data is sourced from MySQL (CDC / batch), not Kafka.
"""

import asyncio
import json
import os

import aiohttp
from aiokafka import AIOKafkaProducer
from common.kafka_config import CLICKSTREAM_PRODUCER_CONFIG
from ingestion.producer import dlq

SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://data-simulator:8000")
TOPIC = "behavior_events"
SEND_TIMEOUT = 1.0   # fail fast -> DLQ instead of blocking the loop

BYPASS_SIMULATOR = os.getenv("BYPASS_SIMULATOR", "False").lower() == "true"
DISABLE_DLQ = os.getenv("DISABLE_DLQ", "False").lower() == "true"
THROUGHPUT_LIMIT = int(os.getenv("THROUGHPUT_LIMIT", "-1"))
FALLBACK_PATH = os.getenv("FALLBACK_PATH", "/app/fallback/clickstream_fallback.jsonl")

if BYPASS_SIMULATOR:
    from ingestion.simulator.event_generator import generate_event
    print("Producer is running in HIGH-THROUGHPUT DIRECT MODE (Bypassing HTTP Simulator)")
else:
    generate_event = None


class TokenBucket:
    """Asynchronous Token Bucket rate limiter for throughput throttling."""
    def __init__(self, rate):
        self.rate = rate
        self.capacity = max(rate, 100)
        self.tokens = float(self.capacity)
        self.last_time = asyncio.get_event_loop().time()

    async def consume(self):
        if self.rate <= 0:
            return
        while True:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_time = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            # Sleep exactly the duration needed to accumulate 1 token
            sleep_time = (1.0 - self.tokens) / self.rate
            await asyncio.sleep(max(sleep_time, 0.001))


def write_fallback(event):
    """Write event to a lightweight local append-only JSONL file."""
    try:
        os.makedirs(os.path.dirname(FALLBACK_PATH), exist_ok=True)
        with open(FALLBACK_PATH, "a") as f:
            f.write(json.dumps(event) + "\n")
    except Exception as e:
        print(f"Failed to write to fallback: {e}")


FAILED = 0

def on_send_error(exc, event, key):
    """Handle background send error asynchronously."""
    global FAILED
    FAILED += 1
    if DISABLE_DLQ:
        write_fallback(event)
    else:
        dlq.enqueue(TOPIC, key, json.dumps(event).encode())


def _errback(event, key):
    """Done-callback factory: only fires on a REAL delivery failure. Binds event/key per call
    (a bare lambda would late-bind to the next loop iteration's values)."""
    def _cb(fut):
        exc = fut.exception()
        if exc is not None:
            on_send_error(exc, event, key)
    return _cb


async def send_event(producer, key, value, event):
    """Send one event; on any Kafka error/timeout, buffer to local DLQ or Fallback."""
    if DISABLE_DLQ:
        try:
            future = await producer.send(TOPIC, value=value, key=key)
            future.add_done_callback(_errback(event, key))
            return True
        except Exception as exc:
            on_send_error(exc, event, key)
            return False
    else:
        try:
            await asyncio.wait_for(producer.send_and_wait(TOPIC, value=value, key=key), timeout=SEND_TIMEOUT)
            return True
        except Exception as exc:
            on_send_error(exc, event, key)
            return False


async def produce_loop(producer, session):
    sent = 0
    bucket = TokenBucket(THROUGHPUT_LIMIT) if THROUGHPUT_LIMIT > 0 else None
    
    while True:
        if bucket:
            await bucket.consume()

        event = None
        if BYPASS_SIMULATOR:
            event = generate_event()
        else:
            try:
                async with session.get(f"{SIMULATOR_URL}/events/single", timeout=aiohttp.ClientTimeout(total=5)) as r:
                    event = await r.json()
            except Exception:
                await asyncio.sleep(1)
                continue
                
        key = event["user_id"].encode()
        value = json.dumps(event).encode()
        await send_event(producer, key, value, event)
        sent += 1

        # Backpressure: periodically flush so the fire-and-forget loop can't overrun the internal
        # buffer — overflow was causing ~50% send timeouts (events lost to fallback) under high load.
        if sent % 5000 == 0:
            await producer.flush()
            print(f"Attempted {sent}, delivered {sent - FAILED}, failed {FAILED}")

        if not BYPASS_SIMULATOR:
            await asyncio.sleep(0.05)


async def retry_dlq_loop(producer, interval=30):
    """Periodically resend DLQ-buffered events; stop the round as soon as Kafka fails again."""
    while True:
        await asyncio.sleep(interval)
        rows = dlq.fetch_pending(limit=500)
        if not rows:
            continue
        resent = 0
        for row_id, topic, k, v in rows:
            try:
                await asyncio.wait_for(producer.send_and_wait(topic, value=v, key=k), timeout=SEND_TIMEOUT)
                dlq.delete(row_id)
                resent += 1
            except Exception:
                break   # Kafka still down — retry next round
        if resent:
            print(f"DLQ retry: resent {resent}, remaining {dlq.count()}")


async def wait_for_simulator(session, max_retries=30, delay=3):
    for i in range(max_retries):
        try:
            async with session.get(f"{SIMULATOR_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    print("Simulator is ready")
                    return
        except Exception:
            pass
        print(f"Waiting for simulator... ({i + 1}/{max_retries})")
        await asyncio.sleep(delay)
    raise ConnectionError("Simulator not available after retries")


async def main():
    if not DISABLE_DLQ:
        dlq.init()
        
    producer = AIOKafkaProducer(**CLICKSTREAM_PRODUCER_CONFIG)
    await producer.start()
    
    async with aiohttp.ClientSession() as session:
        if not BYPASS_SIMULATOR:
            await wait_for_simulator(session)
        print("Producer started.")
        try:
            if DISABLE_DLQ:
                await produce_loop(producer, session)
            else:
                await asyncio.gather(produce_loop(producer, session), retry_dlq_loop(producer))
        finally:
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
