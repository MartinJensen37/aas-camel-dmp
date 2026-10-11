"""Tests for the DMP descriptor compiler (management_node.descriptor)."""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.descriptor import (
    AAS_DATASINK_FILE,
    LUA_TRANSFORMER_FILE,
    MQTT_DATASOURCE_FILE,
    ROUTES_FILE,
    compile_descriptor,
)
from management_node.parsers import aid, aimc
from management_node.resolver import resolve

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface,
)

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
    # every route is either a uniform execution-policy aggregation or a
    # ProtocolBridge interaction (ADR-020) — legacy triggers are gone
    assert {r["trigger"] for r in routes} <= {"execution-policy", "bridge"}
    mqtt_route = next(
        r for r in routes
        if r["trigger"] == "execution-policy"
        and "syntegonStopperingSystem_mapping_0_StationState" in r["datasources"])
    # scoped entity id (ADR-017): {asset}_{mapping}_{SourceId}, plural ALWAYS
    assert mqtt_route["datasources"] == ["syntegonStopperingSystem_mapping_0_StationState"]
    assert "datasource" not in mqtt_route
    assert mqtt_route["transformers"]
    assert mqtt_route["datasinks"]
    route = next(
        r for r in routes
        if r["trigger"] == "execution-policy"
        and "syntegonStopperingSystem_mapping_0_StationState" in r["datasources"])
    # scoped entity id (ADR-017): {asset}_{mapping}_{SourceId}, plural ALWAYS
    assert mqtt_route["datasources"] == ["syntegonStopperingSystem_mapping_0_StationState"]
    assert "datasource" not in mqtt_route
    assert mqtt_route["transformers"]
    assert mqtt_route["datasinks"]
    # no authorable policy block (decision 2026-09-15)
    assert "executionPolicy" not in mqtt_route


def test_emits_mqtt_consumer_config(descriptor):
    consumers = json.loads(descriptor.files[MQTT_DATASOURCE_FILE])
    entry = next(c for c in consumers
                 if c["uniqueId"] == "syntegonStopperingSystem_mapping_0_StationState")
    # the raw AIMC SourceId is pinned for fan-in aggregation (ADR-017)
    assert entry["sourceId"] == "StationState"
    assert entry["serverUrl"] == "192.168.0.104"
    assert entry["serverPort"] == 1883
    # AID base + href, without a leading slash (matches the station's topic)
    assert entry["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/State"


def test_emits_lua_transformers_and_blob_files(descriptor):
    transformers = json.loads(descriptor.files[LUA_TRANSFORMER_FILE])
    assert transformers
    for t in transformers:
        assert not ({"queryLanguage", "inputType", "outputType"} & t.keys())
        assert t["queryPath"] in descriptor.files, "blob file must be materialized"
        blob = descriptor.files[t["queryPath"]]
        assert "function aimc_main" in blob


def test_emits_aas_sink_config(descriptor):
    import base64

    sinks = json.loads(descriptor.files[AAS_DATASINK_FILE])
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
    # A Variables entry is a plain Property addressed by its own idShort, so
    # the sink path is exactly the reference's element keys — no legacy
    # '.variable' compatibility suffix.
    assert all(not s["idShortPath"].endswith(".variable") for s in variable_sinks)
    assert all("." not in s["idShortPath"] for s in variable_sinks)


def test_response_sink_entities_are_scoped_per_interaction_binding():
    from management_node.descriptor import DescriptorCompiler, DescriptorContext
    from management_node.resolver import ResolvedMapping, ResolvedSink

    compiler = DescriptorCompiler(aas_base_url="http://aas-server:8081",
                                  aas_id_short="unit")
    context = DescriptorContext()
    mapping = ResolvedMapping(id_short="Stoppering")
    sink = ResolvedSink(sink_id="Audit", protocol="mqtt",
                        base="mqtt://broker:1883/NN/Device", topic="/AUDIT")

    primary = compiler._add_mqtt_datasink(
        context, mapping, sink, "Audit", entity_scope="unit_Stoppering")
    secondary = compiler._add_mqtt_datasink(
        context, mapping, sink, "Audit", entity_scope="unit_Stoppering_http")

    assert primary == "unit_Stoppering_Audit_sink"
    assert secondary == "unit_Stoppering_http_Audit_sink"
    assert primary != secondary
    assert [entry["uniqueId"] for entry in context.mqtt_sinks] == [primary, secondary]


def test_write_to_directory(descriptor, tmp_path):
    descriptor.write_to(tmp_path)
    assert (tmp_path / ROUTES_FILE).exists()
    assert (tmp_path / LUA_TRANSFORMER_FILE).exists()
    # blob files live under queries/
    assert list((tmp_path / "queries").glob("*.lua"))


def test_write_to_prunes_stale_files(descriptor, tmp_path):
    """Files from an earlier compilation that the current descriptor no longer
    contains are removed (T1.2 — stale {aas}_{mapping}.lua class of junk)."""
    stale = tmp_path / "queries" / "syntegonStopperingSystem_renamed_mapping.lua"
    stale.parent.mkdir(parents=True)
    stale.write_text("function aimc_main(sources) end\n")
    descriptor.write_to(tmp_path)
    assert not stale.exists()
    assert (tmp_path / ROUTES_FILE).exists()


def test_write_to_is_atomic_per_file(descriptor, tmp_path):
    import os
    descriptor.write_to(tmp_path)
    target = tmp_path / ROUTES_FILE
    before = target.read_text()
    # no temp-file leftovers after a successful write
    leftovers = [p.name for p in tmp_path.rglob(".*.tmp")]
    assert not leftovers
    descriptor.write_to(tmp_path)
    assert target.read_text() == before
    assert os.path.exists(target)


def test_write_to_rolls_back_the_whole_file_set_on_replace_failure(tmp_path, monkeypatch):
    import os
    from management_node.descriptor import DmpDescriptor

    root = tmp_path / "config"
    root.mkdir()
    (root / "routes.json").write_text("old routes")
    (root / "stale.json").write_text("old stale")
    descriptor = DmpDescriptor("aas", "unit", {
        "routes.json": "new routes", "queries/transform.lua": "new transform",
    })
    replace = os.replace
    failed = False

    def fail_once(src, dst):
        nonlocal failed
        if not failed and str(dst).endswith("transform.lua"):
            failed = True
            raise OSError("simulated disk failure")
        replace(src, dst)

    monkeypatch.setattr(os, "replace", fail_once)
    with pytest.raises(OSError, match="simulated disk failure"):
        descriptor.write_to(root)
    assert (root / "routes.json").read_text() == "old routes"
    assert (root / "stale.json").read_text() == "old stale"
    assert not (root / "queries" / "transform.lua").exists()
    assert not list(root.rglob(".*.tmp"))


def test_write_to_rejects_paths_outside_config_directory(tmp_path):
    from management_node.descriptor import DmpDescriptor

    descriptor = DmpDescriptor("aas", "unit", {"../outside.json": "no"})
    with pytest.raises(ValueError, match="escapes its config directory"):
        descriptor.write_to(tmp_path / "config")


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
    consumers = json.loads(descriptor.files[MQTT_DATASOURCE_FILE])
    entry = next(c for c in consumers
                 if c["uniqueId"] == "syntegonStopperingSystem_mapping_0_StationState")
    assert entry["serverUrl"] == "localhost"
    assert entry["serverPort"] == 1883
    assert entry["topic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/State"


def _operations_models():
    """AID + AIMC with a one-MC Halt operation mapping (DMP v3): Operation-
    element source → native command affordance, Transformation = request blob."""
    from aas_pydantic import Key, ModelReference

    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
        actions={"Halt": ("/CMD/Halt", "/DATA/Halt")}))
    mqtt_ref = aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                        "actions", "Halt")
    operation_ref = ModelReference(key=(
        Key(type_="Submodel", value="a/submodels/ControlComponentInstance"),
        Key(type_="SubmodelElementCollection", value="Skills"),
        Key(type_="SubmodelElementCollection", value="Halt"),
        Key(type_="Operation", value="operation"),
    ))
    request = aimc_mapping(
        id_short="Halt",
        sources=[aimc_source("Halt", operation_ref)],
        sinks=[aimc_sink("Halt", mqtt_ref)],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    )
    return resolve(build_aimc(request), aid_model)


def test_emits_operation_route_and_entities():
    """An interaction compiles to ONE bridge route: the DERIVED operation path
    rides in triggerData (the bridge emits the REST consumer from it — no
    REST datasource file), the declared command topic a datasink, with
    request/response Lua transformers and reply correlation (ADR-020
    ProtocolBridge, DMP v3 one-MC authoring)."""
    from management_node.descriptor import (
        LUA_TRANSFORMER_FILE, MQTT_DATASINK_FILE,
        ROUTES_FILE,
    )

    resolution = _operations_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystemAAS",
    )

    # Bridge route in routes.json — restPath/restMethod ride in triggerData,
    # no REST datasource entity exists.
    routes = json.loads(descriptor.files[ROUTES_FILE])
    op_routes = [r for r in routes if r["trigger"] == "bridge"]
    assert len(op_routes) == 1
    op = op_routes[0]
    assert op["routeId"] == "syntegonStopperingSystemAAS_Halt_interaction"
    assert op["datasources"] == []
    assert op["transformers"] == ["syntegonStopperingSystemAAS_Halt_request_transformer"]
    assert op["datasinks"] == ["syntegonStopperingSystemAAS_Halt_Halt_command_sink"]
    assert op["triggerData"]["commandSinkBindings"] == [{
        "sinkId": "Halt",
        "dataSinkId": "syntegonStopperingSystemAAS_Halt_Halt_command_sink",
        "valuePath": None,
    }]
    # the derived caller-facing operation path (DMP v3 convention)
    assert op["triggerData"]["restPath"] == "/operations/syntegonStopperingSystemAAS/Halt"
    assert op["triggerData"]["restMethod"] == "POST"
    assert op["triggerData"]["targetOptions"]["requestReply"] is True
    assert op["triggerData"]["targetOptions"]["responseTopic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/Halt"
    assert "responseDatasource" not in op["triggerData"]
    assert op["triggerData"]["responseTransformer"] == "syntegonStopperingSystemAAS_Halt_response_transformer"
    assert op["triggerData"]["targetOptions"]["timeoutSeconds"] == 30

    # no restsources.json is emitted at all
    from management_node.descriptor import DescriptorCompiler
    assert DescriptorCompiler.__name__ == "DescriptorCompiler"

    # MQTT command datasink; the InOut endpoint owns the reply subscription.
    mqtt_sinks = json.loads(descriptor.files[MQTT_DATASINK_FILE])
    cmd = next(s for s in mqtt_sinks if s["uniqueId"] == op["datasinks"][0])
    assert cmd["serverUrl"] == "192.168.0.104"
    assert cmd["serverPort"] == 1883
    assert cmd["topic"] == "NN/Nybrovej/InnoLab/Stoppering/CMD/Halt"
    assert cmd["clientId"].startswith("dmp-")

    # the command publish carries the DECLARED retain/qos verbatim
    # (mqv_retain=false, mqv_qos=2 authored on the affordance form)
    cmd = next(s for s in mqtt_sinks if s["uniqueId"] == op["datasinks"][0])
    assert cmd["qos"] == 2
    assert cmd["retain"] is False

    # Both directional Lua transformers + materialized blobs.
    lua = json.loads(descriptor.files[LUA_TRANSFORMER_FILE])
    request_t = next(t for t in lua if t["uniqueId"] == op["transformers"][0])
    response_t = next(t for t in lua if t["uniqueId"] == op["triggerData"]["responseTransformer"])
    assert request_t["sourceKeys"] == "Halt"
    assert response_t["sourceKeys"] == "Halt"
    assert "function aimc_main" in descriptor.files[request_t["queryPath"]]
    assert "function aimc_main" in descriptor.files[response_t["queryPath"]]


def test_duplicate_entity_ids_fail_loudly():
    """Two mappings with the same effective name produce colliding global
    entity ids; compilation must fail instead of silently misrouting
    (AIMC scopes ids per mapping, the DMP resolves them globally — ADR-017)."""
    from aas_model.testing import build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref

    ref = aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                   "properties", "StationState")
    sink_ref = aimc_ref("{aas_id}/submodels/Variables", "Out")
    blob = "function aimc_main(sources) return { Out = sources.StationState } end"
    aid_model = build_aid(mqtt=_mqtt_interface_with_station())
    mc = aimc_mapping(
        id_short="Dup", sources=[aimc_source("StationState", ref)],
        sinks=[aimc_sink("Out", sink_ref)], transformation=blob)
    resolution = resolve(build_aimc(mc, mc), aid_model)
    with pytest.raises(ValueError, match="duplicate DMP entity id"):
        compile_descriptor(
            resolution,
            aas_base_url="http://aas-server:8081",
            aas_id="syntegonStopperingSystemAAS",
            aas_id_short="syntegonStopperingSystem",
        )


def _mqtt_interface_with_station():
    from aas_model.testing import mqtt_interface
    return mqtt_interface("mqtt://192.168.0.104:1883/NN",
                          properties={"StationState": "/DATA/State"})


def test_operations_honour_mqtt_broker_override():
    from management_node.descriptor import (
        MQTT_DATASINK_FILE, ROUTES_FILE,
    )

    resolution = _operations_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystemAAS",
        mqtt_broker=("localhost", 1883),
    )

    op = next(r for r in json.loads(descriptor.files[ROUTES_FILE]) if r["trigger"] == "bridge")
    cmd = next(s for s in json.loads(descriptor.files[MQTT_DATASINK_FILE]) if s["uniqueId"] == op["datasinks"][0])
    assert cmd["serverUrl"] == "localhost"
    assert cmd["serverPort"] == 1883
    assert op["triggerData"]["targetOptions"]["responseTopic"] == "NN/Nybrovej/InnoLab/Stoppering/DATA/Halt"


def test_bacnet_source_and_sink_ride_plc4x():
    """BACnet has no dedicated component — it rides the plc4x builders with
    the bacnet-ip driver (ADR-022); both directions."""
    from aas_model.testing import bacnet_interface
    from management_node.descriptor import (
        PLC4X_DATASOURCE_FILE, PLC4X_DATASINK_FILE, ROUTES_FILE,
    )

    aid_model = build_aid(bacnet=bacnet_interface(
        "bacnet://192.168.0.70:47808",
        properties={"RoomTemp": "/analog-input,0"}))
    bacnet_ref = aimc_ref("a/submodels/AID", "interface_bacnet",
                          "InteractionMetadata", "properties", "RoomTemp")
    mc = aimc_mapping(
        id_short="RoomTemp",
        sources=[aimc_source("RoomTemp", bacnet_ref)],
        sinks=[aimc_sink("RoomTemp", bacnet_ref)],
        transformation="function aimc_main(sources) return { RoomTemp = sources.RoomTemp } end",
        default_polling_interval="5.0",
    )
    descriptor = compile_descriptor(
        resolve(build_aimc(mc), aid_model),
        aas_base_url="http://aas-server:8081",
        aas_id="a",
        aas_id_short="a",
    )

    consumers = json.loads(descriptor.files[PLC4X_DATASOURCE_FILE])
    assert consumers[0]["driver"] == "bacnet-ip"
    assert consumers[0]["serverUrl"] == "192.168.0.70"
    assert consumers[0]["tags"][0]["value"] == "/analog-input,0"
    sinks = json.loads(descriptor.files[PLC4X_DATASINK_FILE])
    assert sinks[0]["driver"] == "bacnet-ip"
    route = next(r for r in json.loads(descriptor.files[ROUTES_FILE])
                 if r["trigger"] == "execution-policy")
    assert route["datasources"] == [consumers[0]["uniqueId"]]
    assert sinks[0]["uniqueId"] in route["datasinks"]


def _http_source_url(href: str, base: str = "http://192.168.0.30:8080/api") -> str:
    from aas_model.testing import http_interface
    from management_node.descriptor import HTTP_DATASOURCE_FILE

    aid_model = build_aid(http=http_interface(base, properties={"Reading": href}))
    ref = aimc_ref("a/submodels/AID", "interface_http", "InteractionMetadata",
                   "properties", "Reading")
    descriptor = compile_descriptor(
        resolve(build_aimc(aimc_mapping(
            id_short="Reading",
            sources=[aimc_source("Reading", ref)],
            sinks=[aimc_sink("Out", aimc_ref("{aas_id}/submodels/KPI", "Out"))],
            transformation="function aimc_main(s) return { Out = s.Reading } end",
        )), aid_model),
        aas_base_url="http://aas-server:8081", aas_id="a", aas_id_short="a",
    )
    return json.loads(descriptor.files[HTTP_DATASOURCE_FILE])[0]["serverUrl"]


def test_http_source_joins_base_path_and_href_without_losing_a_separator():
    """A relative href must not concatenate straight onto the base path."""
    assert _http_source_url("/pressure") == "http://192.168.0.30:8080/api/pressure"
    assert _http_source_url("pressure") == "http://192.168.0.30:8080/api/pressure"
    assert _http_source_url("/pressure?unit=2") == \
        "http://192.168.0.30:8080/api/pressure?unit=2"
    assert _http_source_url("/") == "http://192.168.0.30:8080/api/"


def test_http_source_rejects_href_that_would_escape_the_declared_base():
    with pytest.raises(ValueError, match="must be a relative path"):
        _http_source_url("http://other-host/elsewhere")
    with pytest.raises(ValueError, match="must be a relative path"):
        _http_source_url("/pressure#frag")


def test_http_source_rejects_a_non_http_base():
    from aas_model.testing import http_interface
    from management_node.descriptor import HTTP_DATASOURCE_FILE, ROUTES_FILE

    aid_model = build_aid(http=http_interface("not-a-url",
                                              properties={"Reading": "/pressure"}))
    ref = aimc_ref("a/submodels/AID", "interface_http", "InteractionMetadata",
                   "properties", "Reading")
    descriptor = compile_descriptor(
        resolve(build_aimc(aimc_mapping(
            id_short="Reading",
            sources=[aimc_source("Reading", ref)],
            sinks=[aimc_sink("Out", aimc_ref("{aas_id}/submodels/KPI", "Out"))],
            transformation="function aimc_main(s) return { Out = s.Reading } end",
        )), aid_model),
        aas_base_url="http://aas-server:8081", aas_id="a", aas_id_short="a",
    )
    assert HTTP_DATASOURCE_FILE not in descriptor.files
    assert ROUTES_FILE not in descriptor.files


def test_compile_does_not_mutate_the_input_resolution():
    """A mapping rejected after its sources were compiled must not leave the
    caller's resolution objects rewritten (DescriptorContext rollback covers
    emitted artifacts only)."""
    from management_node.resolver import ResolvedMapping, ResolvedSink, ResolvedSource

    model = resolve(build_aimc(aimc_mapping(
        id_short="Mutating",
        sources=[aimc_source("State", aimc_ref("a/submodels/KPI", "State"))],
        sinks=[aimc_sink("Missing", aimc_ref("a/submodels/AID", "interface_http",
                                             "InteractionMetadata", "properties",
                                             "Missing"))],
        transformation="function aimc_main(s) return { x = 1 } end",
        default_polling_interval="2.0",
    )), build_aid())
    model.mappings = [ResolvedMapping(
        id_short="Mutating",
        sources=[ResolvedSource(source_id="State", protocol="aas", interface="",
                                polling_interval=None, aas_property=True,
                                aas_element_type="Property",
                                ref_path=["a/submodels/KPI", "State"])],
        sink_refs=[], sink_ids=["Missing"],
        resolved_sinks=[ResolvedSink(sink_id="Missing", protocol="nope")],
        transformation="function aimc_main(s) return { x = 1 } end",
        default_polling_interval=2.0,
    )]
    source = model.mappings[0].sources[0]
    assert (source.delivery, source.polling_interval) == ("sync", None)

    compile_descriptor(model, aas_base_url="http://aas-server:8081",
                       aas_id="a", aas_id_short="a")

    assert (source.delivery, source.polling_interval) == ("sync", None)


def _opcua_operation_models():
    """AID + AIMC with a one-MC Calibrate operation mapping whose command
    affordance is a UA Method (OPC-10101 §7.1.3: componentOf + §6.2 NodeId
    href) — the descriptor emits a milo-client METHOD CALL sink."""
    from aas_model.testing import opcua_interface
    from aas_pydantic import Key, ModelReference

    aid_model = build_aid(opcua=opcua_interface(
        "opc.tcp://192.168.0.90:4840",
        actions={"Calibrate": ("/?id=ns=2;s=RunCalibration", "POST",
                               "ns=1;s=CalibrationObject")}))
    opcua_ref = aimc_ref("a/submodels/AID", "interface_opcua", "InteractionMetadata",
                         "actions", "Calibrate")
    operation_ref = ModelReference(key=(
        Key(type_="Submodel", value="a/submodels/ControlComponentInstance"),
        Key(type_="SubmodelElementCollection", value="Skills"),
        Key(type_="SubmodelElementCollection", value="Calibrate"),
        Key(type_="Operation", value="operation"),
    ))
    request = aimc_mapping(
        id_short="Calibrate",
        sources=[aimc_source("Calibrate", operation_ref)],
        sinks=[aimc_sink("Calibrate", opcua_ref)],
        transformation="function aimc_main(sources) return { Calibrate = sources.Calibrate } end",
    )
    return resolve(build_aimc(request), aid_model)


def test_emits_opcua_method_call_route():
    """An OPC UA interaction compiles to a milo-client METHOD CALL sink:
    node = the owning Object (uav_componentOf), method = the Method NodeId —
    the UA Call service answers inline with its OutputArguments."""
    from management_node.descriptor import OPCUA_DATASINK_FILE, ROUTES_FILE

    resolution = _opcua_operation_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="aasCalibrationAAS",
        aas_id_short="aasCalibrationAAS",
    )

    op = next(r for r in json.loads(descriptor.files[ROUTES_FILE])
              if r["trigger"] == "bridge")
    assert "protocol" not in op["triggerData"]
    assert "responseDatasource" not in op["triggerData"]

    sinks = json.loads(descriptor.files[OPCUA_DATASINK_FILE])
    assert len(sinks) == 1
    sink = sinks[0]
    # node = the OWNING OBJECT; the Method NodeId rides the `method` option
    assert sink["nodeInformation"] == "ns=1;s=CalibrationObject"
    assert sink["configuration"]["method"] == "RAW(ns=2;s=RunCalibration)"
    assert sink["uniqueId"] == op["datasinks"][0]


def test_unresolved_owner_leaves_operation_path_macro_literal():
    """When no owning shell resolves, {aas_id_short} stays literal — camel-rest
    would treat it as a URI template matching ANY first segment, so the
    interaction is skipped (never deployed with a wildcard path)."""
    from management_node.descriptor import ROUTES_FILE

    resolution = _operations_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        # the orchestrator passes the literal macro when no owning shell is found
        aas_id="{aas_id}",
        aas_id_short="{aas_id_short}",
    )

    routes = json.loads(descriptor.files.get(ROUTES_FILE, "[]"))
    assert [r for r in routes if r["trigger"] == "bridge"] == []


def test_skipped_interactions_leave_no_command_sinks():
    """An unpaired MQTT command must not leave an orphaned runtime sink."""
    from management_node.descriptor import MQTT_DATASINK_FILE

    resolution = resolve(aimc.parse_aimc(json.loads(DATA_AIMC.read_text())),
                         aid.parse_aid(json.loads(DATA_AID.read_text())))
    descriptor = compile_descriptor(
        resolution, aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )
    assert MQTT_DATASINK_FILE not in descriptor.files


def test_one_unsupported_sink_rejects_entire_mapping():
    """No subset of the declared destinations may be deployed."""
    source_ref = aimc_ref("a/submodels/AID", "interface_mqtt",
                          "InteractionMetadata", "properties", "State")
    absent_ref = aimc_ref("a/submodels/AID", "interface_http",
                          "InteractionMetadata", "properties", "Missing")
    model = build_aimc(aimc_mapping(
        id_short="TwoSinks",
        sources=[aimc_source("State", source_ref)],
        sinks=[aimc_sink("Out", aimc_ref("{aas_id}/submodels/KPI", "Out")),
               aimc_sink("Missing", absent_ref)],
        transformation="function aimc_main(sources) return { Out = sources.State } end",
    ))
    resolution = resolve(model, build_aid(mqtt=mqtt_interface(
        "mqtt://broker:1883/line", properties={"State": "/state"})))
    descriptor = compile_descriptor(
        resolution, aas_base_url="http://aas-server:8081",
        aas_id="contractAAS", aas_id_short="contract",
    )
    assert ROUTES_FILE not in descriptor.files
    assert MQTT_DATASOURCE_FILE not in descriptor.files
    assert AAS_DATASINK_FILE not in descriptor.files


def _http_operation_models():
    """AID + AIMC with a one-MC Restart operation mapping whose command
    affordance lives on the HTTP interface (protocol-inherent reply)."""
    from aas_model.testing import http_interface
    from aas_pydantic import Key, ModelReference

    aid_model = build_aid(http=http_interface(
        "http://192.168.0.60:8080/plc/api",
        actions={"Restart": ("/actions/Restart?mode=quick", "POST")}))
    http_ref = aimc_ref("a/submodels/AID", "interface_http", "InteractionMetadata",
                        "actions", "Restart")
    operation_ref = ModelReference(key=(
        Key(type_="Submodel", value="a/submodels/ControlComponentInstance"),
        Key(type_="SubmodelElementCollection", value="Skills"),
        Key(type_="SubmodelElementCollection", value="Restart"),
        Key(type_="Operation", value="operation"),
    ))
    request = aimc_mapping(
        id_short="Restart",
        sources=[aimc_source("Restart", operation_ref)],
        sinks=[aimc_sink("Restart", http_ref)],
        transformation="function aimc_main(sources) return { Restart = sources.Restart } end",
    )
    return resolve(build_aimc(request), aid_model)


def test_emits_http_operation_route():
    """A rest-protocol interaction compiles to a bridge route whose command
    sink is a camel-http producer (httpdatasink.json) — the HTTP response is
    the reply, so no response datasource exists."""
    from management_node.descriptor import HTTP_DATASINK_FILE, ROUTES_FILE

    resolution = _http_operation_models()
    descriptor = compile_descriptor(
        resolution,
        aas_base_url="http://aas-server:8081",
        aas_id="aasRestartAAS",
        aas_id_short="aasRestartAAS",
    )

    routes = json.loads(descriptor.files[ROUTES_FILE])
    op = next(r for r in routes if r["trigger"] == "bridge")
    assert "protocol" not in op["triggerData"]
    assert op["triggerData"]["targetOptions"]["httpMethod"] == "POST"
    assert op["triggerData"]["restPath"] == "/operations/aasRestartAAS/Restart"
    assert op["triggerData"]["restMethod"] == "POST"
    assert "responseDatasource" not in op["triggerData"]

    sinks = json.loads(descriptor.files[HTTP_DATASINK_FILE])
    assert len(sinks) == 1
    assert sinks[0]["scheme"] == "http"
    assert sinks[0]["serverUrl"] == "192.168.0.60"
    assert sinks[0]["port"] == 8080
    assert sinks[0]["path"] == "/plc/api/actions/Restart"
    assert sinks[0]["query"] == "mode=quick"
    assert sinks[0]["uniqueId"] == op["datasinks"][0]


def test_mqtt_polling_interval_is_rejected_without_a_fresh_read_binding():
    """Do not compile a subscription queue as a periodic fresh Read."""
    from aas_pydantic.submodel_templates.asset_interfaces_mapping_configuration import (
        DefaultPollingInterval,
    )

    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
        properties={"StationState": "/DATA/State"}))
    src = aimc_source("StationState", aimc_ref(
        "{aas_id}/submodels/AID", "interface_mqtt",
        "InteractionMetadata", "properties", "StationState"))
    src.PollingInterval = DefaultPollingInterval(value="2.0")
    mc = aimc_mapping(
        sources=[src],
        sinks=[aimc_sink("PackMLStateTimer", aimc_ref(
            "{aas_id}/submodels/Variables", "PackMLStateTimer"))],
        transformation="function aimc_main(sources)\n"
                        "    return { PackMLStateTimer = sources.StationState.State }\n"
                        "end\n",
    )
    with pytest.raises(ValueError, match="MQTT property sources support Observe, not Read"):
        resolve(build_aimc(mc), aid_model)
