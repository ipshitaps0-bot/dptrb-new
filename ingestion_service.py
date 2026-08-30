"""
Turnstile ingestion microservice.

Responsibilities:
  - Accept raw turnstile tap payloads (vendor-agnostic).
  - Normalize them into the canonical RidershipEvent schema.
  - Attach a deterministic idempotency key to guard against
    duplicate counting during network-level retries.
  - Publish to the partitioned Kafka topic `ridership.events.raw`,
    keyed by station_id to preserve per-station ordering while
    allowing parallel consumption across partitions.
"""

import hashlib
import json
import logging
import os
import random
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] ingestion-service: %(message)s",
)
logger = logging.getLogger("ingestion-service")

TOPIC_RIDERSHIP_RAW = "ridership.events.raw"


@dataclass(frozen=True)
class RidershipEvent:
    station_id: str
    turnstile_id: str
    direction: str  # "IN" or "OUT"
    passenger_count: int
    timestamp: str
    sequence_id: str

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode("utf-8")


def build_idempotency_key(station_id: str, turnstile_id: str, raw_timestamp: str, sequence: int) -> str:
    """Deterministic composite key preventing duplicate ingestion
    of the same physical tap event during producer-side retries."""
    digest_input = f"{station_id}:{turnstile_id}:{raw_timestamp}:{sequence}".encode("utf-8")
    return hashlib.sha256(digest_input).hexdigest()[:24]


def normalize_tap(raw_payload: dict, sequence: int) -> RidershipEvent:
    """Coerce a vendor-specific raw turnstile payload into the
    canonical schema consumed by the routing engine."""
    station_id = raw_payload["station_id"]
    turnstile_id = raw_payload["turnstile_id"]
    direction = raw_payload["direction"].upper()
    if direction not in ("IN", "OUT"):
        raise ValueError(f"unrecognized direction value: {direction!r}")

    raw_timestamp = raw_payload.get("timestamp") or datetime.now(timezone.utc).isoformat()
    passenger_count = int(raw_payload.get("passenger_count", 1))

    return RidershipEvent(
        station_id=station_id,
        turnstile_id=turnstile_id,
        direction=direction,
        passenger_count=passenger_count,
        timestamp=raw_timestamp,
        sequence_id=build_idempotency_key(station_id, turnstile_id, raw_timestamp, sequence),
    )


class TurnstileProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 10,
            "retry.backoff.ms": 200,
            "linger.ms": 20,
            "batch.size": 65536,
            "compression.type": "lz4",
        })
        self._running = True

    def _delivery_callback(self, err, msg):
        if err is not None:
            logger.error("delivery failed for station=%s: %s", msg.key(), err)
        else:
            logger.debug(
                "delivered to %s [partition %d] at offset %d",
                msg.topic(), msg.partition(), msg.offset(),
            )

    def publish(self, event: RidershipEvent) -> None:
        self._producer.produce(
            topic=TOPIC_RIDERSHIP_RAW,
            key=event.station_id.encode("utf-8"),
            value=event.to_json(),
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("%d messages still in-flight after flush timeout", remaining)

    def shutdown(self, *_args) -> None:
        logger.info("shutdown signal received, draining producer buffer")
        self._running = False


def run(bootstrap_servers: str, station_ids: list[str], events_per_second: float) -> None:
    """Entrypoint loop. In a production deployment this loop is
    replaced by an edge-device webhook receiver or a message-bus
    subscriber bridging vendor turnstile hardware; the normalization
    and publication path downstream is identical."""
    producer = TurnstileProducer(bootstrap_servers)

    signal.signal(signal.SIGTERM, producer.shutdown)
    signal.signal(signal.SIGINT, producer.shutdown)

    sequence = 0
    interval = 1.0 / events_per_second if events_per_second > 0 else 1.0

    logger.info(
        "ingestion service started: brokers=%s stations=%d rate=%.2f events/sec",
        bootstrap_servers, len(station_ids), events_per_second,
    )

    while producer._running:
        station_id = "STN-001"
        raw_payload = {
            "station_id": station_id,
            "turnstile_id": f"{station_id}-TS-{random.randint(1, 6):02d}",
            "direction": "IN",
            "passenger_count": 1,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            event = normalize_tap(raw_payload, sequence)
            producer.publish(event)
            sequence += 1
        except (KeyError, ValueError) as exc:
            logger.error("rejected malformed turnstile payload: %s", exc)

        time.sleep(interval)

    producer.flush()
    logger.info("ingestion service stopped cleanly")


if __name__ == "__main__":
    bootstrap = os.environ.get("KAFKA_BROKERS")
    if not bootstrap:
        logger.error("KAFKA_BROKERS environment variable is required")
        sys.exit(1)

    stations_env = os.environ.get("STATION_IDS", "STN-001,STN-002,STN-003,STN-004")
    stations = [s.strip() for s in stations_env.split(",") if s.strip()]

    rate = float(os.environ.get("EVENTS_PER_SECOND", "5"))

    run(bootstrap, stations, rate)
