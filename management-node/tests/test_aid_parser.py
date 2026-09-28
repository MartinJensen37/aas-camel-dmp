"""Tests for the AID parser (management_node.parsers.aid), which now returns the
shared ``aas_model.DmpAssetInterfacesDescription`` pydantic model (ADR-016).

Uses the real AID submodel extracted from
``AASDescriptions/Resource/configs/syntegonStoppering.json``.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.parsers import aid

DATA = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"


@pytest.fixture(scope="module")
def submodel() -> dict:
    return json.loads(DATA.read_text())


def _mqtt(model):
    return model.InterfaceTemplateForMQTT["interface_mqtt"]


def test_parses_interfaces(submodel):
    model = aid.parse_aid(submodel)
    assert model.InterfaceTemplateForMQTT["interface_mqtt"] is not None
    # DMP v3 (ADR-022): the caller-facing REST interface is retired entirely —
    # the AID models only the device's own communication
    assert not hasattr(model, "interface_rest")


def test_parses_endpoint_metadata(submodel):
    mqtt = _mqtt(aid.parse_aid(submodel))
    assert mqtt.EndpointMetadata.base.value == \
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
    assert mqtt.EndpointMetadata.contentType.value == "application/json"


def test_parses_properties_with_mqtt_forms(submodel):
    mqtt = _mqtt(aid.parse_aid(submodel))
    props = mqtt.InteractionMetadata.properties.property_name

    assert "StationState" in props
    prop = props["StationState"]
    assert prop.key.value == "StationState"

    form = prop.forms
    assert form.href.value == "/DATA/State"
    assert form.mqv_retain.value == "true"
    assert form.mqv_qos.value == "0"
    assert form.mqv_control_packet.value == "publish"


def test_parses_actions_with_sync_flag_and_response(submodel):
    mqtt = _mqtt(aid.parse_aid(submodel))
    acts = mqtt.InteractionMetadata.actions.property_name

    assert "Stoppering" in acts
    action = acts["Stoppering"]
    assert action.synchronous.value == "false"
    # the affordance's name is its idShort (the dict key) — no legacy key child

    form = action.forms
    assert form.href.value == "/CMD/Stoppering"
    assert form.mqv_control_packet.value == "subscribe"
    assert form.mqv_qos.value == "2"
    # response form is preserved
    assert form.response is not None
    assert form.response.href.value == "/DATA/Stoppering"
    assert form.response.mqv_control_packet.value == "publish"


def test_parses_halt_declared_response(submodel):
    """Every delegated skill's command affordance declares its reply topic —
    the correlated leg of the derived interaction."""
    mqtt = _mqtt(aid.parse_aid(submodel))
    acts = mqtt.InteractionMetadata.actions.property_name
    for name in ("Halt", "Occupy", "Release", "Stoppering"):
        form = acts[name].forms
        assert form.response is not None, f"{name} must declare its response form"
        assert form.response.href.value == f"/DATA/{name}"


def test_accepts_json_string(submodel):
    model = aid.parse_aid(json.dumps(submodel))
    assert model.InterfaceTemplateForMQTT["interface_mqtt"] is not None


def test_empty_input_produces_none():
    # Non-submodel input is not validated; the parser returns None (the
    # management node only parses real AAS submodels).
    assert aid.parse_aid({}) is None
