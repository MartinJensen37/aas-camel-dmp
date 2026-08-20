"""AAS event listener (Kafka only, ADR-010).

Follows the ``kg-bridge`` Kafka conventions (SPEC §11.1):
``KAFKA_BOOTSTRAP``, ``KAFKA_TOPIC_PATTERN`` (default
``(aas-events|submodel-events)``), ``KAFKA_CONSUMER_GROUP``,
``KAFKA_AUTO_OFFSET_RESET`` (default ``earliest``).

Subscribes to the topic regex, parses AAS/Submodel events and forwards only
AIMC submodel create/update/delete events to the handler.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError

from .parsers.aimc import AIMC_SEMANTIC_ID

logger = logging.getLogger(__name__)

#: Event types that signal an AIMC content change.
_CREATE_UPDATE = {"SM_CREATED", "SM_UPDATED", "SME_UPDATED", "SME_CREATED"}


@dataclass
class AimcEvent:
    event_type: str
    submodel_id: str
    payload: dict[str, Any] | None = None

    @property
    def deleted(self) -> bool:
        return self.event_type == "SM_DELETED"


def _extract_event(payload: dict) -> dict:
    """BaSyx may wrap the event under ``{"event": {...}}``; unwrap."""
    inner = payload.get("event")
    return inner if isinstance(inner, dict) else payload


def _submodel_semantic_id(submodel: Any) -> str | None:
    if not isinstance(submodel, dict):
        return None
    sem = submodel.get("semanticId")
    if isinstance(sem, dict):
        keys = sem.get("keys") or []
        if keys:
            return str(keys[0].get("value", ""))
        if "value" in sem:
            return str(sem["value"])
    if isinstance(sem, str):
        return sem
    return None


def is_aimc_event(payload: dict, topic: str) -> bool:
    """Discriminator: submodel semantic id == AIMC 2.0 (SPEC §11.1)."""
    if not topic.lower().endswith(("submodel-events", "/submodel-events", ".submodel-events")):
        return False
    event = _extract_event(payload)
    submodel = event.get("submodel")
    return _submodel_semantic_id(submodel) == AIMC_SEMANTIC_ID


def to_aimc_event(payload: dict, topic: str) -> AimcEvent | None:
    event = _extract_event(payload)
    event_type = str(event.get("type", ""))
    submodel_id = str(event.get("id", ""))
    submodel = event.get("submodel") if isinstance(event.get("submodel"), dict) else None
    return AimcEvent(event_type=event_type, submodel_id=submodel_id, payload=submodel)


class AimcEventListener:
    """Kafka consumer that calls ``handler(AimcEvent)`` for AIMC events.

    Offsets are committed per message (at-least-once); the orchestrator is
    idempotent so replayed events are harmless (SPEC §11.2/8).
    """

    def __init__(
        self,
        handler: Callable[[AimcEvent], None],
        *,
        bootstrap: str = "kafka:9092",
        topic_pattern: str = "(aas-events|submodel-events)",
        consumer_group: str = "dmp-management-node",
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._handler = handler
        self._topic_pattern = topic_pattern
        self._running = True
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": consumer_group,
                "auto.offset.reset": auto_offset_reset,
                "enable.auto.commit": False,
            }
        )

    def run_forever(self) -> None:
        subscription = self._topic_pattern
        if not subscription.startswith("^"):
            subscription = f"^{subscription}$"
        self._consumer.subscribe([subscription])
        logger.info("subscribed to Kafka topic regex: %s", subscription)

        try:
            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("kafka error: %s", msg.error())
                    continue
                if msg.value() is None:
                    self._consumer.commit(message=msg, asynchronous=False)
                    continue

                self._handle(msg.topic(), msg.value(), msg)
                self._consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            logger.info("keyboard interrupt; shutting down")
        finally:
            self._consumer.close()

    def stop(self) -> None:
        self._running = False

    def _handle(self, topic: str, raw: bytes, msg) -> None:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception:
            logger.warning("skipping non-JSON message on topic=%s", topic)
            return

        if not is_aimc_event(payload, topic):
            return

        event = to_aimc_event(payload, topic)
        if event is None or not event.submodel_id:
            logger.warning("AIMC event without submodel id on topic=%s", topic)
            return

        logger.info("AIMC event type=%s submodel=%s", event.event_type, event.submodel_id)
        try:
            self._handler(event)
        except Exception:
            logger.exception("handler failed for AIMC event %s", event.submodel_id)
