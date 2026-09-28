"""Tests for per-source delivery classification (async/sync) and the routes
it produces (SPEC §6.1).

The classification is protocol-agnostic and derived from DECLARED facts only:
- event affordances / observe-op forms push;
- observable properties push;
- transport-declared push protocols (MQTT per its TD semantic id) push
  everything that is not an action;
- everything else is pulled (polled request/response).

Execution is a fixed runtime semantic (decision 2026-09-15): every mapping
compiles to ONE aggregation route; there is no authorable policy block.
"""

from __future__ import annotations

import json
import pytest

from management_node.descriptor import (
    HTTP_CONSUMER_FILE,
    MQTT_CONSUMER_FILE,
    OPCUA_CONSUMER_FILE,
    KAFKA_CONSUMER_FILE,
    ROUTES_FILE,
    compile_descriptor,
)
from management_node.resolver import resolve

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface, http_interface, opcua_interface,
)


def _source_ref(interface: str, data_point: str):
    return aimc_ref("a/submodels/AID", interface, "InteractionMetadata",
                    "properties", data_point)


def _model(source_ref, source_id: str = "src", default_polling_interval=None):
    return build_aimc(aimc_mapping(
        sources=[aimc_source(source_id, source_ref)],
        sinks=[aimc_sink("Out", aimc_ref("{aas_id}/submodels/Variables", "Out"))],
        default_polling_interval=default_polling_interval,
    ))


def _compile(resolution):
    return compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )


def test_mqtt_interface_source_is_async():
    aid_model = build_aid(mqtt=mqtt_interface("mqtt://b:1883/base", properties={"P": "/DATA/x"}))
    resolution = resolve(_model(_source_ref("interface_mqtt", "P")), aid_model)
    src = resolution.mappings[0].sources[0]
    assert src.delivery == "async"

    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    assert "executionPolicy" not in route
    assert MQTT_CONSUMER_FILE in descriptor.files


def test_http_interface_source_is_sync():
    aid_model = build_aid(http=http_interface("http://h:8080", properties={"P": None}))
    resolution = resolve(_model(_source_ref("interface_http", "P"),
                                default_polling_interval=3), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "sync"

    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    # sync-only mapping: the consumer still carries the declared MC-level
    # polling interval (plan Q8) — it now drives the poll cadence directly
    assert route["trigger"] == "execution-policy"
    assert "executionPolicy" not in route
    consumer = json.loads(descriptor.files[HTTP_CONSUMER_FILE])[0]
    assert consumer["pollingIntervalMs"] == 3000
    assert HTTP_CONSUMER_FILE in descriptor.files


def test_opcua_observable_source_is_async():
    aid_model = build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/opcua/server",
                                                properties={"Press": None},
                                                observable=True))
    resolution = resolve(_model(_source_ref("interface_opcua", "Press"), "Press"), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "async"

    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    assert OPCUA_CONSUMER_FILE in descriptor.files


def test_opcua_plain_read_source_is_sync():
    aid_model = build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/opcua/server",
                                                properties={"Press": None}))
    resolution = resolve(_model(_source_ref("interface_opcua", "Press"), "Press",
                                default_polling_interval=3), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "sync"
    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"


def test_observation_subprotocol_makes_source_async_without_op():
    aid_model = build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/opcua/server",
                                                properties={"Press": None},
                                                observable=True,
                                                subprotocol="opcua-monitored-item"))
    resolution = resolve(_model(_source_ref("interface_opcua", "Press"), "Press"), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "async"


def test_aas_element_source_is_sync():
    source_ref = aimc_ref("{aas_id}/submodels/Variables", "MachineState", "State")
    resolution = resolve(_model(source_ref), build_aid())
    assert resolution.mappings[0].sources[0].delivery == "sync"


def test_invalid_mapping_polling_interval_fails_compilation():
    aid_model = build_aid(http=http_interface("http://h:8080", properties={"P": None}))
    resolution = resolve(_model(_source_ref("interface_http", "P"),
                                default_polling_interval=0), aid_model)
    with pytest.raises(ValueError, match="must be positive"):
        _compile(resolution)


def test_aas_property_without_interval_requires_change_stream():
    from aas_pydantic import Key, ModelReference

    source_ref = ModelReference(key=(
        Key(type_="Submodel", value="a/submodels/Variables"),
        Key(type_="Property", value="State"),
    ))
    resolution = resolve(_model(source_ref, "State"), build_aid())
    with pytest.raises(ValueError, match="change stream"):
        _compile(resolution)


def test_aas_property_without_interval_emits_filtered_event_source():
    from aas_pydantic import Key, ModelReference

    source_ref = ModelReference(key=(
        Key(type_="Submodel", value="a/submodels/Variables"),
        Key(type_="Property", value="State"),
    ))
    resolution = resolve(_model(source_ref, "State"), build_aid())
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="a",
        aas_id_short="a",
        aas_event_broker=("kafka", 9092),
    )
    event_source = json.loads(descriptor.files[KAFKA_CONSUMER_FILE])[0]
    assert event_source["sourceKind"] == "aas-element-event"
    assert event_source["mode"] == "event"
    assert event_source["breakOnFirstError"] is True
    assert event_source["eventFilter"]["elementPath"] == "State"
    assert "pollingIntervalMs" not in event_source


def test_smc_source_emits_element_event_when_eventing_is_available():
    source_ref = aimc_ref("a/submodels/Runtime", "Runtime", "State")
    resolution = resolve(_model(source_ref, "State"), build_aid())
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="a",
        aas_id_short="a",
        aas_event_broker=("kafka", 9092),
    )
    event_source = json.loads(descriptor.files[KAFKA_CONSUMER_FILE])[0]
    assert event_source["sourceKind"] == "aas-element-event"
    assert event_source["eventFilter"]["modelType"] == "SubmodelElementCollection"
