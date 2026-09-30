"""Tests for the Kafka event listener + AIMC filtering (ADR-010)."""

from __future__ import annotations

import json

from management_node.kafka_listener import (
    AIMC_SEMANTIC_ID,
    is_aimc_event,
    to_aimc_event,
)
from management_node.submodel_locator import AID_SEMANTIC_ID


def _submodel_event(etype: str, submodel_id: str, sem: str | None) -> dict:
    payload: dict = {"type": etype, "id": submodel_id}
    if sem is not None:
        payload["submodel"] = {"semanticId": {"keys": [{"type": "Submodel", "value": sem}]}}
    return payload


def test_detects_aimc_submodel_event():
    payload = _submodel_event("SM_CREATED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    assert is_aimc_event(payload, "submodel-events") is True

    event = to_aimc_event(payload, "submodel-events")
    assert event.event_type == "SM_CREATED"
    assert event.submodel_id == "syntegon/AIMC"
    assert not event.deleted


def test_ignores_non_aimc_submodel_event():
    payload = _submodel_event("SM_CREATED", "other", "https://other/Submodel")
    assert is_aimc_event(payload, "submodel-events") is False


def test_aid_update_passes_filter_for_dependency_invalidation():
    payload = _submodel_event("SM_UPDATED", "device/submodels/AID", AID_SEMANTIC_ID)
    assert is_aimc_event(payload, "submodel-events") is True
    assert to_aimc_event(payload, "submodel-events").semantic_id == AID_SEMANTIC_ID


def test_ignores_aas_events_topic():
    payload = _submodel_event("SM_CREATED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    assert is_aimc_event(payload, "aas-events") is False


def test_detects_deleted_event():
    payload = _submodel_event("SM_DELETED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    event = to_aimc_event(payload, "submodel-events")
    assert event.deleted is True


def test_deleted_event_without_body_passes_filter():
    """Real SM_DELETED events carry no submodel object; the type alone lets
    them through and the orchestrator attributes the id (T1.4/T0.1 fallback)."""
    payload = {"type": "SM_DELETED", "id": "syntegon/AssetInterfacesMappingConfiguration"}
    assert is_aimc_event(payload, "submodel-events") is True
    assert is_aimc_event(payload, "aas-events") is False
    # non-delete events still need the semantic id
    assert is_aimc_event({"type": "SM_UPDATED", "id": "x"}, "submodel-events") is False


def test_delete_detection_covers_element_deletes():
    from management_node.kafka_listener import DELETE_EVENTS
    assert DELETE_EVENTS == frozenset({"SM_DELETED", "SME_DELETED"})


def test_topic_pattern_anchoring_tolerates_preanchored_input():
    from management_node.kafka_listener import anchor_topic_pattern
    assert anchor_topic_pattern("(aas-events|submodel-events)") == "^(aas-events|submodel-events)$"
    assert anchor_topic_pattern("^(aas-events|submodel-events)$") == "^(aas-events|submodel-events)$"
    assert anchor_topic_pattern("^foo$extra") == "^foo$extra$"


def test_handles_wrapped_event_payload():
    inner = _submodel_event("SM_UPDATED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    assert is_aimc_event({"event": inner}, "submodel-events") is True


def test_handles_plain_semantic_id_string():
    payload = {"type": "SM_UPDATED", "id": "syntegon/AIMC", "submodel": {"semanticId": AIMC_SEMANTIC_ID}}
    assert is_aimc_event(payload, "submodel-events") is True


# ── consumer recovery (cold-start race) ──────────────────────────────────────

class _FakeError:
    """Stands in for a confluent_kafka error handle."""

    def __init__(self, code: int, text: str) -> None:
        self._code = code
        self._text = text

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        return self._text


class _FakeMessage:
    def __init__(self, error=None, value=None, topic="submodel-events") -> None:
        self._error = error
        self._value = value
        self._topic = topic

    def error(self):
        return self._error

    def value(self):
        return self._value

    def topic(self):
        return self._topic


class _FakeConsumer:
    """Consumer double scripted with the messages each instance will return.

    Recreating the consumer is how the listener recovers, so the double tracks
    how many consumers were built and what the next one would yield.
    """

    def __init__(self, _config):
        self.closed = False
        self.subscriptions: list[str] = []
        self.committed: list[object] = []

    def subscribe(self, topics):
        self.subscriptions.extend(topics)

    def close(self):
        self.closed = True

    def commit(self, message=None, asynchronous=False):
        self.committed.append(message)


class _ScriptExhausted(Exception):
    """Raised by the fake consumer once its script runs out.

    Production ``poll`` returns ``None`` forever when there is nothing to read,
    so a test cannot simply wait for the loop to end; the fake ends the run
    explicitly instead.
    """


def _listener(monkeypatch, scripts, **kwargs):
    """Builds a listener whose consumers yield the given scripts in order.

    Each consumer instance is bound to the script at its own index, so a rebuild
    is observable as a second consumer appearing.
    """
    from management_node import kafka_listener as kl

    built: list[_FakeConsumer] = []

    def make_consumer(config):
        consumer = _FakeConsumer(config)
        index = len(built)
        seen = {"step": 0}

        def poll(_timeout):
            script = scripts[index] if index < len(scripts) else []
            step = seen["step"]
            seen["step"] = step + 1
            if step >= len(script):
                raise _ScriptExhausted
            return script[step]

        consumer.poll = poll
        built.append(consumer)
        return consumer

    monkeypatch.setattr(kl, "Consumer", make_consumer)

    slept: list[float] = []
    listener = kl.AimcEventListener(lambda _event: None, sleep=slept.append, **kwargs)
    return listener, built, slept


def _run(listener):
    """Runs the loop until the fake consumer runs out of script."""
    try:
        listener.run_forever()
    except _ScriptExhausted:
        pass


def test_rebuilds_on_the_first_error_even_if_polling_then_goes_silent(monkeypatch):
    """The real cold-start failure: UNKNOWN_TOPIC_OR_PART is reported once and
    poll then returns None forever. A rebuild on the *first* error is what
    recovers; waiting for a second error would wait forever."""
    unknown = _FakeMessage(error=_FakeError(3, "Subscribed topic not available"))
    event = _FakeMessage(
        value=json.dumps(_submodel_event("SM_CREATED", "syntegon/AIMC", AIMC_SEMANTIC_ID)).encode())

    # consumer #1: one error, then silence (its script ends -> test stops it).
    # consumer #2: the real event.
    listener, built, slept = _listener(
        monkeypatch, [[unknown], [event]], errors_before_rebuild=1)
    _run(listener)

    assert len(built) == 2, "consumer was not rebuilt after the error"
    assert built[0].closed, "the failed consumer was not closed"
    assert built[1].subscriptions, "the rebuilt consumer did not resubscribe"
    assert slept, "no backoff was applied before rebuilding"
    assert built[1].committed, "the event after the rebuild was not committed"


def test_default_tolerance_is_one_error(monkeypatch):
    """Anything librdkafka can handle itself never surfaces as a poll error
    (a full broker outage produces none), so one error is already conclusive."""
    from management_node.kafka_listener import ERRORS_BEFORE_REBUILD
    assert ERRORS_BEFORE_REBUILD == 1


def test_backoff_grows_and_is_capped(monkeypatch):
    unknown = _FakeMessage(error=_FakeError(3, "unknown topic"))
    # Three rebuilds, then the script runs out.
    listener, built, slept = _listener(
        monkeypatch, [[unknown]] * 3, errors_before_rebuild=1,
        backoff_min_s=1.0, backoff_max_s=2.0)
    _run(listener)

    # The wait is issued in 0.5 s slices so stop() stays responsive; the backoff
    # doubles per rebuild and then saturates at the cap.
    # 1.0 -> 2.0 -> 2.0 (capped) = 5.0 s in total.
    assert len(built) == 4, "expected three rebuilds"
    assert round(sum(slept), 3) == 5.0, sum(slept)
    assert max(slept) <= 0.5, "backoff must be slept in slices"


def test_stops_during_backoff_without_rebuilding(monkeypatch):
    unknown = _FakeMessage(error=_FakeError(3, "unknown topic"))
    listener, built, _slept = _listener(
        monkeypatch, [[unknown]] * 3, errors_before_rebuild=1)
    # Stop as soon as the first backoff begins.
    listener._sleep = lambda _s: listener.stop()
    _run(listener)

    assert len(built) == 1, "stop() during backoff must not rebuild"


def test_partition_eof_does_not_count_as_an_error(monkeypatch):
    from confluent_kafka import KafkaError
    eof = _FakeMessage(error=_FakeError(KafkaError._PARTITION_EOF, "eof"))
    listener, built, _slept = _listener(
        monkeypatch, [[eof]] * 3, errors_before_rebuild=1)
    _run(listener)

    assert len(built) == 1, "EOF must not trigger a rebuild"


def test_error_does_not_churn_when_tolerance_is_raised(monkeypatch):
    """The tolerance is still honoured, so a deployment that prefers to absorb
    some errors can raise it."""
    unknown = _FakeMessage(error=_FakeError(3, "unknown topic"))
    listener, built, _slept = _listener(
        monkeypatch, [[unknown], [unknown]], errors_before_rebuild=5)
    _run(listener)

    assert len(built) == 1, "no rebuild should happen below the tolerance"


def test_is_running_reflects_stop():
    from management_node.kafka_listener import AimcEventListener
    listener = AimcEventListener(lambda _e: None)
    assert listener.is_running is True
    listener.stop()
    assert listener.is_running is False
