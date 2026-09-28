"""Tests for the Kafka event listener + AIMC filtering (ADR-010)."""

from __future__ import annotations

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
