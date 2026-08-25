"""Tests for per-source delivery classification (async/sync).

The classification is protocol-agnostic and derived from the AID (SPEC §6.1):
- MQTT / Kafka -> async (push)
- HTTP / Modbus / PLC / AAS-element -> sync (polled)
- OPC UA is dual-mode: observable + subscription subprotocol -> async,
  otherwise sync
The descriptor keys the route trigger (event vs timer) off ``delivery``, not the
protocol string.
"""

from __future__ import annotations

import json

from management_node.descriptor import (
    HTTP_CONSUMER_FILE,
    MQTT_CONSUMER_FILE,
    OPCUA_CONSUMER_FILE,
    ROUTES_FILE,
    TIMER_CONSUMER_FILE,
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


def _model(source_ref, source_id: str = "src"):
    return build_aimc(aimc_mapping(
        sources=[aimc_source(source_id, source_ref)],
        sinks=[aimc_sink("Out", aimc_ref("{aas_id}/submodels/Variables", "Out"))],
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
    assert route["trigger"] == "event"
    assert MQTT_CONSUMER_FILE in descriptor.files


def test_http_interface_source_is_sync():
    aid_model = build_aid(http=http_interface("http://h:8080", properties={"P": None}))
    resolution = resolve(_model(_source_ref("interface_http", "P")), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "sync"

    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "timer"
    assert TIMER_CONSUMER_FILE in descriptor.files
    assert HTTP_CONSUMER_FILE in descriptor.files


def test_opcua_observable_source_is_async():
    aid_model = build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/",
                                                properties={"Press": None},
                                                observable=True))
    resolution = resolve(_model(_source_ref("interface_opcua", "Press"), "Press"), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "async"

    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "event"
    assert OPCUA_CONSUMER_FILE in descriptor.files


def test_opcua_plain_read_source_is_sync():
    aid_model = build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/",
                                                properties={"Press": None}))
    resolution = resolve(_model(_source_ref("interface_opcua", "Press"), "Press"), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "sync"
    descriptor = _compile(resolution)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "timer"


def test_observation_subprotocol_makes_source_async_without_op():
    aid_model = build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/",
                                                properties={"Press": None},
                                                observable=True,
                                                subprotocol="opcua-monitored-item"))
    resolution = resolve(_model(_source_ref("interface_opcua", "Press"), "Press"), aid_model)
    assert resolution.mappings[0].sources[0].delivery == "async"


def test_aas_element_source_is_sync():
    source_ref = aimc_ref("{aas_id}/submodels/Variables", "MachineState", "State")
    resolution = resolve(_model(source_ref), build_aid())
    assert resolution.mappings[0].sources[0].delivery == "sync"
