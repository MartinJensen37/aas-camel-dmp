"""AAS event listener (Kafka only, ADR-010).

Follows the ``kg-bridge`` Kafka conventions (SPEC §11.1):
``KAFKA_BOOTSTRAP``, ``KAFKA_TOPIC_PATTERN`` (default
``(aas-events|submodel-events)``), ``KAFKA_CONSUMER_GROUP``,
``KAFKA_AUTO_OFFSET_RESET`` (default ``earliest``).

Subscribes to the topic regex and forwards AIMC and AID submodel events.
The latter invalidate cross-AAS endpoint dependencies.

Delete semantics (T1.4): real BaSyx ``SM_DELETED`` events do not carry the
deleted submodel's body, so the usual semantic-id discriminator cannot fire.
Deletion is therefore recognized by event type plus a submodel id that matches
a managed asset's AIMC/AID id (checked by the orchestrator's asset lookup).
Commit-on-handler-failure: offsets are committed even when the handler raises;
the orchestrator is idempotent and failures are logged, so at-most-redeploy is
preferred over an endless poison-message loop.

Startup resilience: the topics are auto-created by BaSyx on first publish, so a
node that starts before the repository is up subscribes to topics that do not
exist yet. librdkafka then reports ``UNKNOWN_TOPIC_OR_PART`` **once** and stops
reporting anything at all — ``poll`` returns ``None`` forever, so the node looks
idle but has never subscribed. Anything librdkafka can handle itself (broker
bounce, partition reassignment) never surfaces as a poll error at all, so a
surfaced non-EOF error means the subscription itself is unusable. The consumer
is therefore rebuilt on the first such error, backing off so a genuinely absent
broker costs a bounded amount of work.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from confluent_kafka import Consumer, KafkaError

from .parsers.aimc import AIMC_SEMANTIC_ID
from .submodel_locator import AID_SEMANTIC_ID

logger = logging.getLogger(__name__)

#: Event types that signal removal of a previously managed submodel.
DELETE_EVENTS = frozenset({"SM_DELETED", "SME_DELETED"})

#: Poll errors tolerated before the consumer is rebuilt. One is enough: the
#: failing subscription stops reporting after the first error (see the module
#: docstring), so waiting for a second one would wait forever.
ERRORS_BEFORE_REBUILD = 1

#: Backoff bounds for rebuilding the consumer.
REBUILD_BACKOFF_MIN_S = 1.0
REBUILD_BACKOFF_MAX_S = 30.0


@dataclass
class AimcEvent:
    event_type: str
    submodel_id: str
    payload: dict[str, Any] | None = None
    #: The submodel semantic id carried by the event body, when present
    #: (AIMC vs AID discriminator for dependency invalidation, T3.2).
    semantic_id: str | None = None

    @property
    def deleted(self) -> bool:
        return self.event_type in DELETE_EVENTS


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


def _is_submodel_events_topic(topic: str) -> bool:
    """True for the ``submodel-events`` stream, tolerating namespace
    separators (``x/submodel-events``, ``x.submodel-events``, bare) so
    deployment-specific topic naming does not silently drop events."""
    name = topic.lower().rstrip("/").rsplit("/", 1)[-1]
    return name == "submodel-events" or name.endswith(".submodel-events")


def is_aimc_event(payload: dict, topic: str) -> bool:
    """Admit managed AIMC events and AID dependency changes (SPEC §11.1).

    Delete events carry no body; they pass only if the event type signals a
    deletion (the id is validated against managed assets downstream)."""
    if not _is_submodel_events_topic(topic):
        return False
    event = _extract_event(payload)
    submodel = event.get("submodel")
    semantic = _submodel_semantic_id(submodel)
    if semantic in (AIMC_SEMANTIC_ID, AID_SEMANTIC_ID):
        return True
    # SM_DELETED payloads have no submodel object — let the type decide; the
    # orchestrator drops ids it cannot attribute to a managed asset.
    return not isinstance(submodel, dict) and str(event.get("type", "")) in DELETE_EVENTS


def to_aimc_event(payload: dict, topic: str) -> AimcEvent | None:
    event = _extract_event(payload)
    event_type = str(event.get("type", ""))
    submodel_id = str(event.get("id", ""))
    submodel = event.get("submodel") if isinstance(event.get("submodel"), dict) else None
    return AimcEvent(event_type=event_type, submodel_id=submodel_id, payload=submodel,
                     semantic_id=_submodel_semantic_id(submodel))


def anchor_topic_pattern(pattern: str) -> str:
    """Anchors a Kafka topic regex for subscription, tolerating pre-anchored
    input (T1.4): strips existing ``^``/``$`` anchors, then wraps exactly once."""
    base = pattern.strip()
    if base.startswith("^"):
        base = base[1:]
    if base.endswith("$"):
        base = base[:-1]
    return f"^{base}$"


class AimcEventListener:
    """Kafka consumer that calls ``handler(AimcEvent)`` for AIMC/AID events.

    Offsets are committed per message; handler failures are logged and the
    offset still committed rather than retrying a poison message indefinitely.
    """

    def __init__(
        self,
        handler: Callable[[AimcEvent], None],
        *,
        bootstrap: str = "kafka:9092",
        topic_pattern: str = "(aas-events|submodel-events)",
        consumer_group: str = "dmp-management-node",
        auto_offset_reset: str = "earliest",
        errors_before_rebuild: int = ERRORS_BEFORE_REBUILD,
        backoff_min_s: float = REBUILD_BACKOFF_MIN_S,
        backoff_max_s: float = REBUILD_BACKOFF_MAX_S,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._handler = handler
        self._topic_pattern = topic_pattern
        self._consumer_config = {
            "bootstrap.servers": bootstrap,
            "group.id": consumer_group,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
        }
        self._errors_before_rebuild = max(1, errors_before_rebuild)
        self._backoff_min_s = backoff_min_s
        self._backoff_max_s = backoff_max_s
        self._sleep = sleep
        self._running = True
        self._consumer: Consumer = Consumer(self._consumer_config)

    def run_forever(self) -> None:
        """Consume until :meth:`stop`, rebuilding the consumer when the
        subscription stops being usable (see the module docstring)."""
        try:
            subscription = anchor_topic_pattern(self._topic_pattern)
            self._consumer.subscribe([subscription])
            logger.info("subscribed to Kafka topic regex: %s", subscription)
            consecutive_errors = 0
            backoff = self._backoff_min_s

            while self._running:
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    consecutive_errors += 1
                    logger.warning(
                        "kafka error (%d/%d): %s",
                        consecutive_errors, self._errors_before_rebuild, msg.error())
                    if consecutive_errors >= self._errors_before_rebuild:
                        if not self._rebuild(subscription, backoff):
                            break
                        consecutive_errors = 0
                        backoff = min(self._backoff_max_s, backoff * 2)
                    continue

                consecutive_errors = 0
                if msg.value() is None:
                    self._consumer.commit(message=msg, asynchronous=False)
                    continue

                self._handle(msg.topic(), msg.value(), msg)
                self._consumer.commit(message=msg, asynchronous=False)
        except KeyboardInterrupt:
            logger.info("keyboard interrupt; shutting down")
        finally:
            self._close()

    def _rebuild(self, subscription: str, backoff: float) -> bool:
        """Closes and recreates the consumer. Returns False if stopping."""
        logger.warning(
            "rebuilding Kafka consumer after %d consecutive errors "
            "(broker or topic not available yet); retrying in %.1fs",
            self._errors_before_rebuild, backoff)
        self._close()
        while self._running and backoff > 0:
            # Sleep in slices so stop() is honoured promptly during backoff.
            self._sleep(min(0.5, backoff))
            backoff -= 0.5
        if not self._running:
            return False
        self._consumer = Consumer(self._consumer_config)
        self._consumer.subscribe([subscription])
        logger.info("resubscribed to Kafka topic regex: %s", subscription)
        return True

    def _close(self) -> None:
        try:
            self._consumer.close()
        except Exception:  # pragma: no cover - close must never mask shutdown
            logger.debug("consumer close failed", exc_info=True)

    def stop(self) -> None:
        self._running = False

    @property
    def is_running(self) -> bool:
        """False once :meth:`stop` was called, for cooperative shutdown."""
        return self._running

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
            # commit happens upstream regardless: never wedge the consumer
            logger.exception("handler failed for AIMC event %s", event.submodel_id)
