"""Tests for the uniform aggregation route (successor of the unified fan-in,
ADR-018→ADR-020): every mapping compiles to ONE shape — plural datasources
with per-source delivery roles, and no authorable policy block (decision
2026-09-15: execution is a fixed runtime semantic — evaluate on any source
update once ALL sources have delivered; sync sources are polled at their
declared IDTA interval).
"""

from __future__ import annotations

import json

from management_node.descriptor import (
    MQTT_CONSUMER_FILE,
    ROUTES_FILE,
    compile_descriptor,
)
from management_node.resolver import resolve

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface, http_interface,
)

MQTT_BASE = "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
HTTP_BASE = "http://192.168.0.30:8080"

BLOB = "function aimc_main(sources) return { Out = sources.StationState } end"


def _mqtt_ref(data_point):
    return aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                    "properties", data_point)


def _http_ref(data_point):
    return aimc_ref("a/submodels/AID", "interface_http", "InteractionMetadata",
                    "properties", data_point)


def _aid():
    return build_aid(
        mqtt=mqtt_interface(MQTT_BASE,
                            properties={"StationState": "/DATA/State",
                                        "StationMode": "/DATA/Mode"}),
        http=http_interface(HTTP_BASE, properties={"Pressure": None}),
    )


def _vars_sink():
    return aimc_sink("Out", aimc_ref("{aas_id}/submodels/Variables", "Out"))


def _compile(aimc_model):
    return compile_descriptor(
        resolve(build_aimc(aimc_model), _aid()),
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )


def test_mixed_sync_async_mapping_is_one_uniform_route():
    """1 MQTT (async) + 1 HTTP (sync) source: one uniform route, plural
    datasources both listed; no authorable policy block exists."""
    mc = aimc_mapping(
        id_short="KpiMix",
        sources=[
            aimc_source("StationState", _mqtt_ref("StationState")),
            aimc_source("Pressure", _http_ref("Pressure")),
        ],
        sinks=[_vars_sink()],
        transformation=BLOB,
    )
    descriptor = _compile(mc)

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    assert "executionPolicy" not in route
    assert sorted(route["datasources"]) == [
        "syntegonStopperingSystem_KpiMix_Pressure",
        "syntegonStopperingSystem_KpiMix_StationState",
    ]

    consumers = json.loads(descriptor.files["mqttconsumer.json"])
    by_raw = {c["sourceId"]: c for c in consumers}
    assert by_raw["StationState"]["delivery"] == "async"
    # per-mapping distinct clientIds: identical feeds can never collide
    assert all(c["clientId"].startswith("dmp-syntegonStopperingSystem_KpiMix")
               for c in consumers)
    # the HTTP source lands in httpconsumer.json with delivery=sync and the
    # MC-level default interval (the sync poll cadence drives evaluation)
    http = json.loads(descriptor.files["httpconsumer.json"])
    assert http[0]["sourceId"] == "Pressure"
    assert http[0]["delivery"] == "sync"

    # the timer-consumer file class is gone: sync cadence rides the consumer
    legacy_timer = descriptor.files.get("timerconsumer.json")
    assert legacy_timer is None


def test_all_async_mapping_is_one_uniform_route():
    mc = aimc_mapping(
        id_short="AsyncOnly",
        sources=[
            aimc_source("StationState", _mqtt_ref("StationState")),
            aimc_source("StationMode", _mqtt_ref("StationMode")),
        ],
        sinks=[_vars_sink()],
        transformation=BLOB,
    )
    descriptor = _compile(mc)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    assert "executionPolicy" not in route
    assert len(route["datasources"]) == 2


def test_multi_sync_mapping_polls_both_sources_in_one_route():
    """Two HTTP (sync) sources on one mapping: each poller runs at the
    declared MC-level interval and feeds the same cache."""
    mc = aimc_mapping(
        id_short="KpiSync2",
        sources=[
            aimc_source("P1", _http_ref("Pressure")),
            aimc_source("P2", _http_ref("Pressure")),
        ],
        sinks=[_vars_sink()],
        transformation=BLOB,
        default_polling_interval=5,
    )
    descriptor = _compile(mc)

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    assert route["datasources"] == [
        "syntegonStopperingSystem_KpiSync2_P1",
        "syntegonStopperingSystem_KpiSync2_P2",
    ]
    consumers = json.loads(descriptor.files["httpconsumer.json"])
    assert all(c["pollingIntervalMs"] == 5000 for c in consumers)


def test_two_sync_sources_and_one_async_source_keep_independent_roles():
    mc = aimc_mapping(
        id_short="ThreeWayKpi",
        sources=[
            aimc_source("StationState", _mqtt_ref("StationState")),
            aimc_source("P1", _http_ref("Pressure")),
            aimc_source("P2", _http_ref("Pressure")),
        ],
        sinks=[_vars_sink()],
        transformation=BLOB,
        default_polling_interval=3,
    )
    descriptor = _compile(mc)

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    assert len(route["datasources"]) == 3
    assert len(set(route["datasources"])) == 3
    mqtt = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    http = json.loads(descriptor.files["httpconsumer.json"])
    assert mqtt[0]["sourceId"] == "StationState"
    assert mqtt[0]["delivery"] == "async"
    assert {entry["sourceId"] for entry in http} == {"P1", "P2"}
    assert all(entry["delivery"] == "sync" and entry["pollingIntervalMs"] == 3000
               for entry in http)


def test_single_sync_source_is_uniform_too():
    mc = aimc_mapping(
        id_short="SoloSync",
        sources=[aimc_source("Pressure", _http_ref("Pressure"))],
        sinks=[_vars_sink()],
        transformation=BLOB,
        default_polling_interval=2,
    )
    descriptor = _compile(mc)
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "execution-policy"
    # legacy pollenrich special casing is gone; singular datasource key never
    assert route["datasources"] == ["syntegonStopperingSystem_SoloSync_Pressure"]
    assert "datasource" not in route and "triggerData" not in route
