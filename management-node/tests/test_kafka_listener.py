"""Tests for the Kafka event listener + AIMC filtering (ADR-010)."""

from __future__ import annotations

from management_node.kafka_listener import (
    AIMC_SEMANTIC_ID,
    is_aimc_event,
    to_aimc_event,
)


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


def test_ignores_aas_events_topic():
    payload = _submodel_event("SM_CREATED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    assert is_aimc_event(payload, "aas-events") is False


def test_detects_deleted_event():
    payload = _submodel_event("SM_DELETED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    event = to_aimc_event(payload, "submodel-events")
    assert event.deleted is True


def test_handles_wrapped_event_payload():
    inner = _submodel_event("SM_UPDATED", "syntegon/AIMC", AIMC_SEMANTIC_ID)
    assert is_aimc_event({"event": inner}, "submodel-events") is True


def test_handles_plain_semantic_id_string():
    payload = {"type": "SM_UPDATED", "id": "syntegon/AIMC", "submodel": {"semanticId": AIMC_SEMANTIC_ID}}
    assert is_aimc_event(payload, "submodel-events") is True
