"""Tests for use-case 2 (AAS->AAS) and the AIMC 2.0 template capabilities:

- R1: AAS-element sources -> aaspollingconsumer.json + timer route
- R2: AAS sinks to any submodel (not just the asset's Variables)
- R3: passthrough (identity) transformation when no Transformation blob
- R4: OPC UA / Modbus(PLC4X) / HTTP synchronous sources -> respective configs
"""

from __future__ import annotations

import base64
import json

from management_node.descriptor import (
    AAS_POLLING_CONSUMER_FILE,
    AAS_SERVER_FILE,
    HTTP_CONSUMER_FILE,
    LUA_TRANSFORMERS_FILE,
    OPCUA_CONSUMER_FILE,
    PLC4X_CONSUMER_FILE,
    ROUTES_FILE,
    TIMER_CONSUMER_FILE,
    compile_descriptor,
)
from management_node.resolver import resolve

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface, opcua_interface, modbus_interface, http_interface,
)


def _variables_source_ref():
    return aimc_ref("{aas_id}/submodels/Variables", "MachineState", "State")


def _kpi_sink_ref():
    return aimc_ref("{aas_id}/submodels/KPI", "Running")


def _variables_sink_ref():
    return aimc_ref("{aas_id}/submodels/Variables", "PackMLState")


def _mapping(*, source_ref, source_id: str = "state", sink_ref=None,
             sink_id: str = "Running", transformation: str | None = None,
             default_polling_interval: float | None = None):
    sinks = []
    if sink_ref is not None:
        sinks = [aimc_sink(sink_id, sink_ref)]
    return build_aimc(aimc_mapping(
        sources=[aimc_source(source_id, source_ref)],
        sinks=sinks,
        transformation=transformation,
        default_polling_interval=default_polling_interval,
    ))


def _compile(resolution, aas_id_short="syntegonStopperingSystem"):
    return compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short=aas_id_short,
    )


# --------------------------------------------------------------------------- #
# R1 + R2: AAS->AAS
# --------------------------------------------------------------------------- #
def test_aas_element_source_emits_polling_consumer_and_timer_route():
    resolution = resolve(_mapping(source_ref=_variables_source_ref(),
                                  sink_ref=_kpi_sink_ref(),
                                  default_polling_interval=2.5),
                         build_aid())
    descriptor = _compile(resolution)

    sources = json.loads(descriptor.files[AAS_POLLING_CONSUMER_FILE])
    assert len(sources) == 1
    src = sources[0]
    assert src["uniqueId"] == "state"
    assert src["submodelEndpoint"].startswith("http://aas-server:8081/submodels/")
    b64 = src["submodelEndpoint"].rsplit("/", 1)[-1]
    decoded = base64.urlsafe_b64decode(b64 + "=" * (-len(b64) % 4)).decode()
    assert "/submodels/Variables" in decoded
    assert src["idShortPath"] == "MachineState/State.variable"  # Variables apex leaf

    routes = json.loads(descriptor.files[ROUTES_FILE])
    route = routes[0]
    assert route["trigger"] == "timer"
    assert route["datasource"] == "state"
    assert route["triggerData"]["timerName"]

    timers = json.loads(descriptor.files[TIMER_CONSUMER_FILE])
    assert timers[0]["period"] == 2500

    sinks = json.loads(descriptor.files[AAS_SERVER_FILE])
    assert len(sinks) == 1
    assert sinks[0]["idShortPath"] == "Running"


def test_aas_sink_to_non_variables_submodel_omits_variable_suffix():
    resolution = resolve(_mapping(source_ref=_variables_source_ref(),
                                  sink_ref=_kpi_sink_ref()),
                         build_aid())
    descriptor = _compile(resolution)
    sinks = json.loads(descriptor.files[AAS_SERVER_FILE])
    assert sinks[0]["idShortPath"] == "Running"
    assert not sinks[0]["idShortPath"].endswith(".variable")


# --------------------------------------------------------------------------- #
# R3: transformation-less passthrough mapping.
# --------------------------------------------------------------------------- #
def _mqtt_aid():
    return build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
        properties={"StationState": "/DATA/State"}))


def test_passthrough_identity_transform_when_no_transformation():
    source_ref = aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                          "properties", "StationState")
    media = _mapping(source_ref=source_ref, source_id="StationState",
                     sink_ref=_variables_sink_ref(), sink_id="PackMLState",
                     transformation=None)
    descriptor = _compile(resolve(media, _mqtt_aid()))

    xformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert len(xformers) == 1
    t = xformers[0]
    assert t["queryLanguage"] == "LUA"
    assert t["uniqueId"].endswith("_passthrough")
    blob = descriptor.files[t["queryPath"]]
    assert "function aimc_main" in blob
    assert '["PackMLState"] = sources["StationState"]' in blob

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["transformers"] == [t["uniqueId"]]


def test_declared_transformation_still_used():
    media = _mapping(
        source_ref=aimc_ref("a/submodels/AID", "interface_mqtt",
                            "InteractionMetadata", "properties", "StationState"),
        source_id="StationState",
        sink_ref=_variables_sink_ref(), sink_id="PackMLState",
        transformation="function aimc_main(sources) return { PackMLState = sources.StationState.State } end")
    descriptor = _compile(resolve(media, _mqtt_aid()))
    xformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert "passthrough" not in xformers[0]["uniqueId"]
    assert "sources.StationState.State" in descriptor.files[xformers[0]["queryPath"]]


# --------------------------------------------------------------------------- #
# R4: OPC UA / Modbus / HTTP synchronous sources.
# --------------------------------------------------------------------------- #
def _opcua_aid():
    return build_aid(opcua=opcua_interface("opc.tcp://192.168.0.10:4840/",
                                           properties={"Pressure": None}))


def test_opcua_source_emits_opcuaconsumer():
    source_ref = aimc_ref("a/submodels/AID", "interface_opcua", "InteractionMetadata",
                          "properties", "Pressure")
    resolution = resolve(_mapping(source_ref=source_ref, source_id="Pressure",
                                  sink_ref=_kpi_sink_ref()), _opcua_aid())
    descriptor = _compile(resolution)

    opcua = json.loads(descriptor.files[OPCUA_CONSUMER_FILE])
    assert len(opcua) == 1
    src = opcua[0]
    assert src["uniqueId"] == "Pressure"
    assert src["serverUrl"] == "192.168.0.10"
    assert src["serverPort"] == 4840

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "timer"


def _modbus_aid():
    return build_aid(modbus=modbus_interface("tcp://192.168.0.20:502",
                                             properties={"Coil1": None}))


def test_modbus_source_emits_plc4xconsumer():
    source_ref = aimc_ref("a/submodels/AID", "interface_modbus", "InteractionMetadata",
                          "properties", "Coil1")
    resolution = resolve(_mapping(source_ref=source_ref, source_id="Coil1",
                                  sink_ref=_kpi_sink_ref()), _modbus_aid())
    descriptor = _compile(resolution)

    plc4x = json.loads(descriptor.files[PLC4X_CONSUMER_FILE])
    assert len(plc4x) == 1
    src = plc4x[0]
    assert src["driver"] == "modbus-tcp"
    assert src["serverUrl"] == "192.168.0.20"
    assert src["serverPort"] == 502
    assert src["tags"][0]["name"] == "Coil1"

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "timer"


def _http_aid():
    return build_aid(http=http_interface("http://192.168.0.30:8080",
                                         properties={"Pressure": None}))


def test_http_source_emits_httpconsumer():
    source_ref = aimc_ref("a/submodels/AID", "interface_http", "InteractionMetadata",
                          "properties", "Pressure")
    resolution = resolve(_mapping(source_ref=source_ref, source_id="Pressure",
                                  sink_ref=_kpi_sink_ref()), _http_aid())
    descriptor = _compile(resolution)

    http = json.loads(descriptor.files[HTTP_CONSUMER_FILE])
    assert len(http) == 1
    assert http[0]["uniqueId"] == "Pressure"

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["trigger"] == "timer"
