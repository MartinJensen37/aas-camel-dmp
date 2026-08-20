"""Tests for the DMP descriptor compiler (management_node.descriptor)."""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.descriptor import (
    AAS_SERVER_FILE,
    LUA_TRANSFORMERS_FILE,
    MQTT_CONSUMER_FILE,
    ROUTES_FILE,
    compile_descriptor,
)
from management_node.parsers import aid, aimc
from management_node.resolver import resolve

DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"


@pytest.fixture(scope="module")
def descriptor():
    aid_model = aid.parse_aid(json.loads(DATA_AID.read_text()))
    aimc_model = aimc.parse_aimc(json.loads(DATA_AIMC.read_text()))
    resolution = resolve(aimc_model, aid_model)
    return compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )


def test_emits_routes_json(descriptor):
    routes = json.loads(descriptor.files[ROUTES_FILE])
    assert len(routes) >= 1
    mqtt_route = next(r for r in routes if r["trigger"] == "event")
    assert mqtt_route["datasource"] == "StationState"
    assert mqtt_route["transformers"]
    assert mqtt_route["datasinks"]


def test_emits_mqtt_consumer_config(descriptor):
    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    entry = next(c for c in consumers if c["uniqueId"] == "StationState")
    assert entry["serverUrl"] == "192.168.0.104"
    assert entry["serverPort"] == 1883
    # AID base + href, without a leading slash (matches the station's topic)
    assert entry["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/State"


def test_emits_lua_transformers_and_blob_files(descriptor):
    transformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert transformers
    for t in transformers:
        assert t["queryLanguage"] == "LUA"
        assert t["queryPath"] in descriptor.files, "blob file must be materialized"
        blob = descriptor.files[t["queryPath"]]
        assert "function aimc_main" in blob


def test_emits_aas_sink_config(descriptor):
    import base64

    sinks = json.loads(descriptor.files[AAS_SERVER_FILE])
    assert sinks
    for s in sinks:
        assert s["type"] == "PROPERTY"
        assert s["submodelEndpoint"].startswith("http://aas-server:8081/submodels/")
        assert s["idShortPath"]
        assert s["api"] == "DotAasV3"
        # the endpoint id is the UTF8-BASE64-URL-encoded submodel id
        b64 = s["submodelEndpoint"].rsplit("/", 1)[-1]
        decoded = base64.urlsafe_b64decode(b64 + "=" * (-len(b64) % 4)).decode()
        assert "/submodels/" in decoded

    # the MQTT mapping writes its variables back to the asset's Variables submodel
    variable_sinks = [
        s for s in sinks
        if "/submodels/Variables" in base64.urlsafe_b64decode(
            s["submodelEndpoint"].rsplit("/", 1)[-1] + "=" * (-len(s["submodelEndpoint"].rsplit("/", 1)[-1]) % 4)
        ).decode()
    ]
    assert variable_sinks, "expected at least one sink on the Variables submodel"
    assert all(s["idShortPath"].endswith(".variable") for s in variable_sinks)


def test_write_to_directory(descriptor, tmp_path):
    descriptor.write_to(tmp_path)
    assert (tmp_path / ROUTES_FILE).exists()
    assert (tmp_path / LUA_TRANSFORMERS_FILE).exists()
    # blob files live under queries/
    assert list((tmp_path / "queries").glob("*.lua"))


def test_mqtt_broker_override(tmp_path):
    """A broker override replaces the AID base host/port in MQTT sources
    (simulated stations publish on the local broker, not the AID's physical one)."""
    aid_model = aid.parse_aid(json.loads(DATA_AID.read_text()))
    aimc_model = aimc.parse_aimc(json.loads(DATA_AIMC.read_text()))
    resolution = resolve(aimc_model, aid_model)
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
        mqtt_broker=("localhost", 1883),
    )
    consumers = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    entry = next(c for c in consumers if c["uniqueId"] == "StationState")
    assert entry["serverUrl"] == "localhost"
    assert entry["serverPort"] == 1883
    assert entry["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/State"


def _operations_models():
    """AID + AIMC with a bidirectional Halt mapping (as compiled by tests above)."""
    aid_model = aid.AidModel(
        id_short="AID",
        interfaces={
            "interface_rest": aid.AidInterface(
                name="interface_rest",
                base="http://dmp-syntegonstopperingsystemaas:8080",
                actions={
                    "Halt": aid.AidAction(
                        name="Halt",
                        synchronous=True,
                        forms=[aid.AidForm(href="/operations/syntegonStopperingSystemAAS/Halt", method="POST")],
                    ),
                },
            ),
            "interface_mqtt": aid.AidInterface(
                name="interface_mqtt",
                base="mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
                actions={
                    "Halt": aid.AidAction(
                        name="Halt",
                        forms=[aid.AidForm(href="/CMD/Halt", response=aid.AidForm(href="/DATA/Halt"))],
                    ),
                },
            ),
        },
    )
    ref = aimc.AimcReference(
        type="ModelReference",
        keys=[
            aimc.AimcReferenceKey(type="Submodel", value="a/submodels/AID"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="interface_rest"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="InteractionMetadata"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="actions"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="Halt"),
        ],
    )
    mqtt_ref = aimc.AimcReference(
        type="ModelReference",
        keys=[
            aimc.AimcReferenceKey(type="Submodel", value="a/submodels/AID"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="interface_mqtt"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="InteractionMetadata"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="actions"),
            aimc.AimcReferenceKey(type="SubmodelElementCollection", value="Halt"),
        ],
    )
    aimc_model = aimc.AimcModel(
        id_short="AIMC",
        mappings=[
            aimc.AimcMapping(
                id_short="HaltRequest",
                sources=[aimc.AimcSource(id_short="Halt", source_id="Halt", source=ref)],
                sinks=[aimc.AimcSink(id_short="Halt", sink_id="Halt", sink=mqtt_ref)],
                transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
            ),
            aimc.AimcMapping(
                id_short="Halt",
                sources=[aimc.AimcSource(id_short="Halt", source_id="Halt", source=mqtt_ref)],
                sinks=[aimc.AimcSink(id_short="Halt", sink_id="Halt", sink=ref)],
                transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
            ),
        ],
    )
    return resolve(aimc_model, aid_model)


def test_emits_operation_route_and_entities():
    """An operation is a route whose REST endpoint is a datasource and whose
    MQTT command is a datasink, with request/response Lua transformers."""
    from management_node.descriptor import (
        LUA_TRANSFORMERS_FILE, MQTT_CONSUMER_FILE, MQTT_SINK_FILE,
        REST_SOURCES_FILE, ROUTES_FILE,
    )

    resolution = _operations_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )

    # Operation route in routes.json.
    routes = json.loads(descriptor.files[ROUTES_FILE])
    op_routes = [r for r in routes if r["trigger"] == "operation"]
    assert len(op_routes) == 1
    op = op_routes[0]
    assert op["routeId"] == "syntegonStopperingSystem_Halt_operation"
    assert op["datasource"] == "syntegonStopperingSystem_Halt_rest"
    assert op["transformers"] == ["syntegonStopperingSystem_Halt_request_transformer"]
    assert op["datasinks"] == ["syntegonStopperingSystem_Halt_command_sink"]
    assert op["triggerData"]["responseDatasource"] == "syntegonStopperingSystem_Halt_response_source"
    assert op["triggerData"]["responseTransformer"] == "syntegonStopperingSystem_Halt_response_transformer"
    assert op["triggerData"]["timeoutSeconds"] == 30

    # REST datasource (the operation's exposed endpoint).
    rests = json.loads(descriptor.files[REST_SOURCES_FILE])
    rest = next(r for r in rests if r["uniqueId"] == op["datasource"])
    assert rest["restMethod"] == "POST"
    assert rest["restPath"] == "/operations/syntegonStopperingSystemAAS/Halt"
    assert rest["contentType"] == "application/json"

    # MQTT command datasink + response datasource.
    mqtt_sinks = json.loads(descriptor.files[MQTT_SINK_FILE])
    cmd = next(s for s in mqtt_sinks if s["uniqueId"] == op["datasinks"][0])
    assert cmd["serverUrl"] == "192.168.0.104"
    assert cmd["serverPort"] == 1883
    assert cmd["topic"] == "NN/Nybrovej/InnoLab/Stoppering/CMD/Halt"
    assert cmd["clientId"].startswith("dmp-")

    mqtt_sources = json.loads(descriptor.files[MQTT_CONSUMER_FILE])
    resp = next(s for s in mqtt_sources if s["uniqueId"] == op["triggerData"]["responseDatasource"])
    assert resp["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/Halt"

    # Both directional Lua transformers + materialized blobs.
    lua = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    request_t = next(t for t in lua if t["uniqueId"] == op["transformers"][0])
    response_t = next(t for t in lua if t["uniqueId"] == op["triggerData"]["responseTransformer"])
    assert request_t["sourceKeys"] == "Halt"
    assert response_t["sourceKeys"] == "Halt"
    assert "function aimc_main" in descriptor.files[request_t["queryPath"]]
    assert "function aimc_main" in descriptor.files[response_t["queryPath"]]


def test_operations_honour_mqtt_broker_override():
    from management_node.descriptor import (
        MQTT_CONSUMER_FILE, MQTT_SINK_FILE, REST_SOURCES_FILE, ROUTES_FILE,
    )

    resolution = _operations_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
        mqtt_broker=("localhost", 1883),
    )

    op = next(r for r in json.loads(descriptor.files[ROUTES_FILE]) if r["trigger"] == "operation")
    cmd = next(s for s in json.loads(descriptor.files[MQTT_SINK_FILE]) if s["uniqueId"] == op["datasinks"][0])
    resp = next(s for s in json.loads(descriptor.files[MQTT_CONSUMER_FILE]) if s["uniqueId"] == op["triggerData"]["responseDatasource"])
    assert cmd["serverUrl"] == "localhost"
    assert cmd["serverPort"] == 1883
    assert resp["serverUrl"] == "localhost"
    assert resp["serverPort"] == 1883
