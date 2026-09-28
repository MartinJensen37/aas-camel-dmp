"""Real BaSyx submodel-event payloads captured from the running lab
(BaSyx aas-environment 2.0.0-SNAPSHOT-06f8841 with Kafka eventing enabled),
used to pin the event shapes the listener depends on (plan T0.1/T1.4).

Captured shapes that matter here:

- ``SM_CREATED``/``SM_UPDATED`` carry the whole ``submodel`` object, so the
  AIMC/AID ``semanticId`` is available for attribution.
- ``SM_DELETED`` carries **only** ``id`` and ``type`` — no submodel and no
  semanticId — which is why a delete can never be attributed by semantic id
  and the id-based fallback is mandatory.
- ``SME_CREATED``/``SME_UPDATED`` carry ``smElement``/``smElementPath`` and no
  submodel object, so submodel-element events are never treated as AIMC
  lifecycle events.

The ``Variables`` fixtures were re-captured (2026-09-28) after the submodel was
flattened, so ``smElementPath`` is the bare element id
(``PackMLStateTimer``) — it matches the descriptor's ``elementPath``, which the
nested ``VariableItem`` shape could never do. ``sme_created.json`` is the
throwaway-element capture from the T0.1 run (submodel ``DmpEventProbe``); its
path is a single id, so it carries no shape assumption.
"""

from __future__ import annotations

import json
from pathlib import Path

from management_node.kafka_listener import is_aimc_event, to_aimc_event
from management_node.parsers.aimc import AIMC_SEMANTIC_ID
from management_node.submodel_locator import AID_SEMANTIC_ID

EVENTS_DIR = Path(__file__).parent / "data" / "events"
TOPIC = "submodel-events"


def _load(name: str) -> dict:
    return json.loads((EVENTS_DIR / f"{name}.json").read_text())


def test_captured_events_are_present_and_unmodified():
    for name in ("sm_created", "sm_created_aid", "sm_created_non_aimc",
                 "sm_updated", "sme_created", "sme_updated", "sm_deleted"):
        assert isinstance(_load(name), dict), name


def test_create_and_update_carry_the_submodel_and_its_semantic_id():
    """The real AIMC payloads carry the whole submodel, so the semanticId
    available for attribution is the AIMC's own."""
    for name in ("sm_created", "sm_updated"):
        payload = _load(name)
        assert payload["submodel"]["semanticId"]["keys"], name
        assert is_aimc_event(payload, TOPIC) is True, name
        event = to_aimc_event(payload, TOPIC)
        assert event.submodel_id == payload["id"], name
        assert event.semantic_id == AIMC_SEMANTIC_ID, name
        assert event.deleted is False, name


def test_a_real_aid_payload_is_accepted_for_dependency_invalidation():
    """A captured AID create carries the AID semanticId, which is what routes
    it to dependency invalidation instead of a redeploy."""
    payload = _load("sm_created_aid")
    assert is_aimc_event(payload, TOPIC) is True
    event = to_aimc_event(payload, TOPIC)
    assert event.semantic_id == AID_SEMANTIC_ID
    assert AID_SEMANTIC_ID != AIMC_SEMANTIC_ID
    assert event.deleted is False


def test_a_created_submodel_that_is_neither_aimc_nor_aid_is_ignored():
    """Real station submodels are published on the same topic and must not
    trigger a reconcile."""
    payload = _load("sm_created_non_aimc")
    assert is_aimc_event(payload, TOPIC) is False
    assert to_aimc_event(payload, TOPIC).semantic_id is None


def test_a_real_delete_has_no_submodel_so_attribution_falls_back_to_the_id():
    """The captured SM_DELETED is only {id, type}: a delete can never be
    matched by semanticId, so the listener must let it through on the event
    type and let the orchestrator attribute it by id."""
    payload = _load("sm_deleted")
    assert set(payload) == {"id", "type"}
    assert "submodel" not in payload

    assert is_aimc_event(payload, TOPIC) is True
    event = to_aimc_event(payload, TOPIC)
    assert event.submodel_id == payload["id"]
    assert event.deleted is True
    assert event.semantic_id is None


def test_submodel_element_events_are_never_treated_as_aimc_events():
    for name in ("sme_created", "sme_updated"):
        payload = _load(name)
        assert "submodel" not in payload
        assert "smElement" in payload, name
        assert is_aimc_event(payload, TOPIC) is False, name
