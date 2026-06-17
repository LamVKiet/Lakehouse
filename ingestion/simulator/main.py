"""
FastAPI data-simulator: generates fake apparel retail clickstream events via HTTP.

Endpoints:
    GET  /health           — health check
    GET  /events/single    — one random event
    GET  /events/batch     — N random events
    POST /events/scenario  — controlled event generation (specific type, error rate)
"""

import random

from fastapi import FastAPI, Query
from pydantic import BaseModel

from ingestion.simulator.event_generator import (
    generate_event,
    EVENT_TYPE_TO_ID,
    COUPON_ERRORS,
    PAYMENT_ERRORS,
)

app = FastAPI(
    title="Apparel Retail Clickstream Simulator",
    description="Generates fake user behavior events for an omnichannel fashion retail platform",
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/events/single")
def single_event():
    """Return one random event."""
    return generate_event()


@app.get("/events/batch")
def batch_events(count: int = Query(default=10, ge=1, le=1000)):
    """Return a list of N random events."""
    return [generate_event() for _ in range(count)]


class ScenarioRequest(BaseModel):
    event_type: str
    count: int = 10
    error_rate: float = 0.0


@app.post("/events/scenario")
def scenario_events(req: ScenarioRequest):
    """
    Generate events of a specific type with optional error injection.
    Useful for testing specific funnel steps or simulating failure spikes.
    """
    events = []
    errors = COUPON_ERRORS + PAYMENT_ERRORS
    for _ in range(req.count):
        event = generate_event()
        if req.event_type in EVENT_TYPE_TO_ID:
            event["event_id"] = EVENT_TYPE_TO_ID[req.event_type]
            event["event_type"] = req.event_type
        if random.random() < req.error_rate:
            event["metadata"]["is_success"] = 0
            event["metadata"]["error_message"] = random.choice(errors)
        events.append(event)
    return events
