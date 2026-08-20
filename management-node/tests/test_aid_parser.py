"""Tests for the dedicated AID parser (management_node.parsers.aid).

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


def test_parses_interfaces(submodel):
    model = aid.parse_aid(submodel)
    assert set(model.interfaces) == {"interface_mqtt", "interface_rest"}


def test_parses_endpoint_metadata(submodel):
    model = aid.parse_aid(submodel)
    mqtt = model.interfaces["interface_mqtt"]
    assert mqtt.base == "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
    assert mqtt.content_type == "application/json"


def test_parses_properties_with_mqtt_forms(submodel):
    model = aid.parse_aid(submodel)
    mqtt = model.interfaces["interface_mqtt"]

    assert "StationState" in mqtt.properties
    prop = mqtt.properties["StationState"]
    assert prop.key == "StationState"

    form = prop.forms[0]
    assert form.href == "/DATA/State"
    assert form.mqtt is not None
    assert form.mqtt.retain is True
    assert form.mqtt.qos == 0
    assert form.mqtt.control_packet == "publish"
    assert prop.semantic_ids and "stationState.schema.json" in prop.semantic_ids[0]


def test_parses_actions_with_sync_flag_and_response(submodel):
    model = aid.parse_aid(submodel)
    mqtt = model.interfaces["interface_mqtt"]

    # actions are nested under a grouping wrapper (property_name)
    assert "Stoppering" in mqtt.actions
    action = mqtt.actions["Stoppering"]
    assert action.synchronous is False
    assert action.key == "Stoppering"

    form = action.forms[0]
    assert form.href == "/CMD/Stoppering"
    assert form.mqtt is not None
    assert form.mqtt.control_packet == "subscribe"
    assert form.mqtt.qos == 2
    # response form is preserved
    assert form.response is not None
    assert form.response.href == "/DATA/Stoppering"
    assert form.response.mqtt is not None
    assert form.response.mqtt.control_packet == "publish"


def test_parses_rest_interface_with_method(submodel):
    model = aid.parse_aid(submodel)
    rest = model.interfaces["interface_rest"]

    assert "Stoppering" in rest.actions
    form = rest.actions["Stoppering"].forms[0]
    assert form.method == "POST"  # htv_methodName
    assert form.op == "invokeAction"
    assert form.mqtt is None


def test_accepts_json_string(submodel):
    model = aid.parse_aid(json.dumps(submodel))
    assert "interface_mqtt" in model.interfaces


def test_empty_input_produces_empty_model():
    model = aid.parse_aid({})
    assert model.interfaces == {}
