"""Tests for fine-granularity resolution via the DECLARED data schema
(ADR-020 as revised: no authored marker).

The IDTA AID PropertyDefinition carries the WoT-2.0-ized JSON-Schema mirror —
``properties`` containers keyed by the exact payload keys, ``items`` for
arrays. Trailing reference keys past the data point walk THAT mirror and
derive the JSON Pointer; anything that does not exist in the schema fails
loudly. Covers the resolver walk and the descriptor emitting ``valuePath`` on
datasources and MQTT sinks.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.descriptor import (
    MQTT_CONSUMER_FILE,
    MQTT_SINK_FILE,
    compile_descriptor,
)
from management_node.parsers.aid import parse_aid
from management_node.resolver import resolve
from aas_model.json_schema_aid import datapoint_from_schema
from aas_model.submodel_templates.aid import MqttProperty

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface,
)

DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"
BASE = "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"

#: mirrors the REAL fixture affordance: StationState is an object whose
#: payload keys are exactly these (parsed mirror verified in the fixture AID)
STATION_SCHEMA = {
    "type": "object",
    "properties": {
        "TimeStamp": {"type": "string"},
        "State": {"type": "string"},
        "ProcessQueue": {"type": "array", "items": {"type": "string"}},
    },
}
WORKORDERS_SCHEMA = {
    "type": "object",
    "properties": {
        "WorkOrders": {"type": "array", "items": {"type": "string"}},
    },
}
SETPOINT_SCHEMA = {
    "type": "object",
    "properties": {"mode": {"type": "string"}},
}


def _schema_iface(props: dict[str, tuple[str, dict | None]]):
    """MQTT interface whose named affordances carry the declared data schema."""
    iface = mqtt_interface(BASE, properties={n: href for n, (href, _) in props.items()})
    for n, (href, schema) in props.items():
        if schema is not None:
            form = iface.InteractionMetadata.properties.property_name[n].forms
            iface.InteractionMetadata.properties.property_name[n] = datapoint_from_schema(
                schema, key=n, cls=MqttProperty, forms=form)
    return iface


def _ref(*path):
    return aimc_ref(*path)


def _aimc(source_path: list[str], *, source_id: str = "StationState",
          sink=None, transformation: str | None = None):
    return build_aimc(aimc_mapping(
        id_short="mapping_0",
        sources=[aimc_source(source_id, _ref(*source_path))],
        sinks=[sink] if sink is not None else [],
        transformation=transformation or "function aimc_main(sources) return { x = 1 } end",
    ))


def _sink(sink_id: str, path: list[str]):
    return aimc_sink(sink_id, _ref(*path))


def _vars_sink():
    return _sink("Out", ["{aas_id}/submodels/Variables", "Out"])


def _compile(resolution):
    return compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )


@pytest.fixture(scope="module")
def fixture_aid():
    return parse_aid(json.loads(DATA_AID.read_text()))


def _src_ref(*trailing):
    return ["a/submodels/AID", "interface_mqtt", "InteractionMetadata",
            "properties", "StationState", *trailing]


def test_coarse_reference_has_no_pointer(fixture_aid):
    result = resolve(_aimc(_src_ref()), fixture_aid)
    assert result.mappings[0].sources[0].value_path is None


def test_trailing_schema_key_derives_pointer(fixture_aid):
    """`State` exists in the StationState schema mirror → `$.State`."""
    result = resolve(_aimc(_src_ref("State")), fixture_aid)
    src = result.mappings[0].sources[0]
    assert src.data_point == "StationState"
    assert src.value_path == "$.State"


def test_container_step_is_transparent(fixture_aid):
    """`[..., StationState, properties, State]` walks the same mirror — the
    literal `properties` container step contributes no pointer token."""
    result = resolve(_aimc(_src_ref("properties", "State")), fixture_aid)
    assert result.mappings[0].sources[0].value_path == "$.State"


def test_non_schema_trailing_key_fails_loudly(fixture_aid):
    with pytest.raises(ValueError, match="not a key of the declared data schema"):
        resolve(_aimc(_src_ref("Unit")), fixture_aid)


def test_typo_trailing_key_fails_loudly(fixture_aid):
    with pytest.raises(ValueError, match="available"):
        resolve(_aimc(_src_ref("Stat")), fixture_aid)


def test_array_reference_stops_at_the_array():
    """`items` names the item SHAPE (one element, no per-index key): the
    pointer stops at the array key; keys past `items` fail loudly."""
    aid = build_aid(mqtt=_schema_iface({
        "WorkOrders": ("/DATA/WorkOrders", WORKORDERS_SCHEMA)}))
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata",
            "properties", "WorkOrders"]

    # payload {"WorkOrders": [...]}: coarse binds the whole payload…
    result = resolve(_aimc(path, source_id="WorkOrders"), aid)
    assert result.mappings[0].sources[0].value_path is None

    # …the named schema key pins the array…
    result = resolve(_aimc([*path, "WorkOrders"], source_id="WorkOrders"), aid)
    assert result.mappings[0].sources[0].value_path == "$.WorkOrders"

    # …and `items` stops the walk at the array (whole array delivered).
    result = resolve(_aimc([*path, "WorkOrders", "items"], source_id="WorkOrders"), aid)
    assert result.mappings[0].sources[0].value_path == "$.WorkOrders"

    with pytest.raises(ValueError, match="per-index"):
        resolve(_aimc([*path, "WorkOrders", "items", "id"], source_id="WorkOrders"), aid)


def test_descriptor_emits_schema_derived_pointer():
    aid = build_aid(mqtt=_schema_iface({
        "StationState": ("/DATA/State", STATION_SCHEMA)}))
    resolution = resolve(_aimc(_src_ref("State"), sink=_vars_sink()), aid)
    descriptor = _compile(resolution)

    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    entry = next(c for c in consumers if c["sourceId"] == "StationState")
    assert entry["valuePath"] == "$.State"
    assert entry["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/State"
    assert entry["clientId"] == f"dmp-{entry['uniqueId']}"


def _attach_schema(iface, props: dict[str, dict]):
    """Attach a declared data schema to an already-built interface's
    affordances, preserving each protocol's own property/form classes."""
    for name, schema in props.items():
        current = iface.InteractionMetadata.properties.property_name[name]
        iface.InteractionMetadata.properties.property_name[name] = datapoint_from_schema(
            schema, key=name, cls=type(current), forms=current.forms)
    return iface


NESTED_SCHEMA = {"type": "object", "properties": {"Reading": {"type": "number"}}}


def test_http_source_and_sink_keep_the_schema_pointer():
    from aas_model.testing import http_interface

    aid = build_aid(http=_attach_schema(
        http_interface("http://192.168.0.30:8080/api",
                       properties={"Pressure": "/pressure"}),
        {"Pressure": NESTED_SCHEMA}))
    path = ["a/submodels/AID", "interface_http", "InteractionMetadata",
            "properties", "Pressure", "Reading"]
    resolution = resolve(_aimc(path, source_id="Pressure",
                               sink=_sink("PressureOut", path)), aid)
    descriptor = _compile(resolution)

    source = json.loads(descriptor.files["httpconsumer.json"])[0]
    assert source["valuePath"] == "$.Reading"
    assert source["serverUrl"] == "http://192.168.0.30:8080/api/pressure"
    sink = json.loads(descriptor.files["httpdatasink.json"])[0]
    assert sink["sinkId"] == "PressureOut"
    assert sink["valuePath"] == "$.Reading"
    assert sink["path"] == "/api/pressure"


def test_opcua_source_and_sink_keep_the_schema_pointer():
    from aas_model.testing import opcua_interface

    aid = build_aid(opcua=_attach_schema(
        opcua_interface("opc.tcp://192.168.0.40:4840/server",
                        properties={"Level": "?id=ns=2;s=Level"}),
        {"Level": NESTED_SCHEMA}))
    path = ["a/submodels/AID", "interface_opcua", "InteractionMetadata",
            "properties", "Level", "Reading"]
    resolution = resolve(_aimc(path, source_id="Level",
                               sink=_sink("LevelOut", path)), aid)
    descriptor = _compile(resolution)

    source = json.loads(descriptor.files["opcuaconsumer.json"])[0]
    assert source["valuePath"] == "$.Reading"
    assert source["pathToService"] == "server"
    sink = json.loads(descriptor.files["opcuadatasink.json"])[0]
    assert sink["valuePath"] == "$.Reading"


def test_plc4x_source_and_sink_keep_the_schema_pointer():
    from aas_model.testing import modbus_interface

    aid = build_aid(modbus=_attach_schema(
        modbus_interface("tcp://192.168.0.50:502/service?unit=1",
                         properties={"Press": "holdingRegister:40001"}),
        {"Press": NESTED_SCHEMA}))
    path = ["a/submodels/AID", "interface_modbus", "InteractionMetadata",
            "properties", "Press", "Reading"]
    resolution = resolve(_aimc(path, source_id="Press",
                               sink=_sink("PressOut", path)), aid)
    descriptor = _compile(resolution)

    source = json.loads(descriptor.files["plc4xconsumer.json"])[0]
    assert source["valuePath"] == "$.Reading"
    assert source["options"] == "unit=1"
    sink = json.loads(descriptor.files["plc4xdatasink.json"])[0]
    assert sink["valuePath"] == "$.Reading"


def test_descriptor_coarse_source_omits_value_path():
    aid = build_aid(mqtt=_schema_iface({
        "StationState": ("/DATA/State", STATION_SCHEMA)}))
    resolution = resolve(_aimc(_src_ref(), sink=_vars_sink()), aid)
    descriptor = _compile(resolution)

    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    entry = next(c for c in consumers if c["sourceId"] == "StationState")
    assert "valuePath" not in entry


def test_sink_pointer_derived_from_schema():
    aid = build_aid(mqtt=_schema_iface({
        "SetPoint": ("/SET/SetPoint", SETPOINT_SCHEMA)}))
    src_path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                "properties", "SetPoint"]
    resolution = resolve(_aimc(src_path, source_id="SetPoint",
                               sink=_sink("sinkSet", [*src_path, "mode"])),
                         aid)
    descriptor = _compile(resolution)

    sinks = json.loads(descriptor.files[MQTT_SINK_FILE])
    s = next(x for x in sinks if x["sinkId"] == "sinkSet")
    assert s["valuePath"] == "$.mode"
    assert s["topic"].endswith("/SetPoint")
