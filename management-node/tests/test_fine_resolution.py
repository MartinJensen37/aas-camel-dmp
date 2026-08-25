"""Tests for coarse/fine reference resolution (ADR-014 / SPEC §6.5).

Covers fine-source detection (nested reference -> JSON Pointer value_path),
fine-sink detection, and the descriptor emitting ``valuePath`` on datasources
and MQTT data sinks.
"""

from __future__ import annotations

import json

from management_node.descriptor import (
    LUA_TRANSFORMERS_FILE,
    MQTT_CONSUMER_FILE,
    MQTT_SINK_FILE,
    ROUTES_FILE,
    compile_descriptor,
)
from management_node.resolver import resolve

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface,
)

BASE = "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"


def _aid(**properties):
    return build_aid(mqtt=mqtt_interface(BASE, properties=properties))


def _variables_sink_ref():
    return aimc_ref("{aas_id}/submodels/Variables", "Out")


def _aimc(source_path: list[str], *, source_id: str = "StationState",
          sink_ref=None, sink_id: str = "sinkSet", transformation: str | None = None,
          extra_sources=None):
    sources = [aimc_source(source_id, aimc_ref(*source_path))]
    if extra_sources:
        sources.extend(extra_sources)
    sinks = []
    if sink_ref is not None:
        sinks = [aimc_sink(sink_id, sink_ref)]
    return build_aimc(aimc_mapping(
        sources=sources, sinks=sinks,
        transformation=transformation or "function aimc_main(sources) return { x = 1 } end",
    ))


def _compile(resolution):
    return compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )


def test_coarse_source_has_no_value_path():
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "StationState"]
    result = resolve(_aimc(path), _aid(StationState="/DATA/State"))
    src = result.mappings[0].sources[0]
    assert src.data_point == "StationState"
    assert src.value_path is None


def test_fine_source_pins_nested_pointer():
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties",
            "StationState", "State"]
    result = resolve(_aimc(path), _aid(StationState="/DATA/State"))
    src = result.mappings[0].sources[0]
    assert src.data_point == "StationState"
    assert src.value_path == "$.State"


def test_fine_source_with_multiple_nested_keys():
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties",
            "WorkOrders", "0", "id"]
    result = resolve(_aimc(path), _aid(WorkOrders="/DATA/WorkOrders"))
    src = result.mappings[0].sources[0]
    assert src.data_point == "WorkOrders"
    assert src.value_path == "$.0.id"


def test_descriptor_emits_value_path_on_fine_source():
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties",
            "StationState", "State"]
    resolution = resolve(_aimc(path, sink_ref=_variables_sink_ref(), sink_id="Out"),
                         _aid(StationState="/DATA/State"))
    descriptor = _compile(resolution)

    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    entry = next(c for c in consumers if c["uniqueId"] == "StationState")
    assert entry["valuePath"] == "$.State"
    assert entry["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/State"


def test_coarse_source_omits_value_path():
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "StationState"]
    resolution = resolve(_aimc(path, sink_ref=_variables_sink_ref(), sink_id="Out"),
                         _aid(StationState="/DATA/State"))
    descriptor = _compile(resolution)

    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    entry = next(c for c in consumers if c["uniqueId"] == "StationState")
    assert "valuePath" not in entry


def test_mqtt_data_sink_emitted_with_sink_id():
    aid_model = _aid(Data="/DATA/In", SetPoint="/SET/SetPoint")
    src_path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "Data"]
    snk_path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "SetPoint"]
    resolution = resolve(_aimc(src_path, sink_ref=aimc_ref(*snk_path)), aid_model)

    descriptor = _compile(resolution)
    sinks = json.loads(descriptor.files[MQTT_SINK_FILE])
    s = next(x for x in sinks if x["sinkId"] == "sinkSet")
    assert s["topic"] == "NN/Nybrovej/InnoLab/Stoppering/SET/SetPoint"
    assert s["clientId"].startswith("dmp-")
    assert "valuePath" not in s, "coarse MQTT sink must not pin a pointer"

    routes = json.loads(descriptor.files[ROUTES_FILE])
    route = next(r for r in routes if r["trigger"] == "event")
    assert "syntegonStopperingSystem_sinkSet_sink" in route["datasinks"]


def test_fine_mqtt_sink_emits_value_path():
    aid_model = _aid(Data="/DATA/In", SetPoint="/SET/SetPoint")
    src_path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "Data"]
    snk_path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties",
                "SetPoint", "mode"]
    resolution = resolve(_aimc(src_path, sink_ref=aimc_ref(*snk_path)), aid_model)

    descriptor = _compile(resolution)
    sinks = json.loads(descriptor.files[MQTT_SINK_FILE])
    s = next(x for x in sinks if x["sinkId"] == "sinkSet")
    assert s["valuePath"] == "$.mode"
    assert s["topic"].endswith("/SetPoint")


def test_fan_in_compiles_all_sources_and_plural_datasources():
    aid_model = _aid(StationState="/DATA/State", StationMode="/DATA/Mode")
    srcA = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "StationState"]
    srcB = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "StationMode"]
    extra = [aimc_source("station_mode", aimc_ref(*srcB))]
    aimc_model = _aimc(
        srcA, source_id="station_state",
        sink_ref=_variables_sink_ref(), sink_id="Out",
        transformation="function aimc_main(sources) return { Out = sources.station_state + sources.station_mode } end",
        extra_sources=extra,
    )
    descriptor = _compile(resolve(aimc_model, aid_model))

    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    ids = {c["uniqueId"] for c in consumers}
    assert ids == {"station_state", "station_mode"}

    routes = json.loads(descriptor.files[ROUTES_FILE])
    route = routes[0]
    assert route["datasources"] == ["station_state", "station_mode"]
    assert "datasource" not in route, "fan-in must use the plural datasources key"

    xformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert xformers[0]["sourceKeys"] == "station_state,station_mode"


def test_single_source_keeps_legacy_singular_datasource():
    aid_model = _aid(StationState="/DATA/State")
    path = ["a/submodels/AID", "interface_mqtt", "InteractionMetadata", "properties", "StationState"]
    aimc_model = _aimc(path, sink_ref=_variables_sink_ref(), sink_id="Out")
    resolution = resolve(aimc_model, aid_model)
    descriptor = _compile(resolution)

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["datasource"] == "StationState"
    assert "datasources" not in route
