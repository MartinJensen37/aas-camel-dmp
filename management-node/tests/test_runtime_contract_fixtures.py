"""Python side of the management-to-runtime descriptor contract.

From management-node, run
``PYTHONPATH=../../aas-model/src:. python3 tests/test_runtime_contract_fixtures.py /tmp/kilo/phase0-contract``
to materialize fixtures for the Java GeneratedDescriptorContractTest. Both
tests use the same authored AID/AIMC models.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

from aas_model.testing import (
    aimc_mapping, aimc_ref, aimc_sink, aimc_source, build_aid, build_aimc,
    modbus_interface, mqtt_interface,
)
from aas_pydantic import Key, ModelReference

from management_node.descriptor import compile_descriptor
from management_node.parsers import aid, aimc
from management_node.resolver import resolve

from test_descriptor import DATA_AID, DATA_AIMC, _http_operation_models, _opcua_operation_models
from test_fine_resolution import SETPOINT_SCHEMA, _schema_iface
from test_usecase2 import _http_aid, _opcua_aid

BASE_URL = "http://aas-server:8081"
AAS_ID = "contractAAS"


def _aas_ref(element_type: str, path: str) -> ModelReference:
    return ModelReference(key=(
        Key(type_="Submodel", value="a/submodels/Runtime"),
        Key(type_=element_type, value=path),
    ))


def _mapping(source_id, source_ref, sinks, *, interval=None, name="Mapping"):
    return aimc_mapping(
        id_short=name,
        sources=[aimc_source(source_id, source_ref)],
        sinks=sinks,
        transformation="function aimc_main(sources) return { Out = sources[\"%s\"] } end" % source_id,
        default_polling_interval=interval,
    )


def _sink(name="Out"):
    return aimc_sink(name, aimc_ref("{aas_id}/submodels/KPI", name))


def contract_descriptors():
    """Compile distinct protocol/mode cases, with no generated JSON hand edits."""
    stoppering = resolve(
        aimc.parse_aimc(json.loads(DATA_AIMC.read_text())),
        aid.parse_aid(json.loads(DATA_AID.read_text())),
    )
    yield "stoppering", compile_descriptor(
        stoppering, aas_base_url=BASE_URL,
        aas_id="syntegonStopperingSystemAAS", aas_id_short="syntegonStopperingSystem",
    )
    for name, models in (
        ("opcua-method", _opcua_operation_models()),
        ("http-operation", _http_operation_models()),
    ):
        yield name, compile_descriptor(models, aas_base_url=BASE_URL,
                                       aas_id=AAS_ID, aas_id_short="contract")

    modbus_ref = aimc_ref("a/submodels/AID", "interface_modbus",
                          "InteractionMetadata", "properties", "Coil1")
    modbus = resolve(build_aimc(_mapping(
        "Coil1", modbus_ref, [aimc_sink("Out", modbus_ref)],
        interval=5, name="Modbus")),
        build_aid(modbus=modbus_interface("tcp://192.168.0.20:502/service?unit=2",
                                           properties={"Coil1": "/coil:1"})))
    yield "plc4x", compile_descriptor(modbus, aas_base_url=BASE_URL,
                                     aas_id=AAS_ID, aas_id_short="contract")

    for name, element_type in (("property", "Property"),
                               ("collection", "SubmodelElementCollection")):
        ref = _aas_ref(element_type, "State")
        for mode in ("event", "poll"):
            model = resolve(build_aimc(_mapping(
                "State", ref, [_sink()], interval=2.5 if mode == "poll" else None,
                name=f"{element_type}{mode}")), build_aid())
            options = {"aas_event_broker": ("kafka", 9092)} if mode == "event" else {}
            yield f"aas-{name}-{mode}", compile_descriptor(
                model, aas_base_url=BASE_URL, aas_id=AAS_ID,
                aas_id_short="contract", **options)

    opcua_ref = aimc_ref("a/submodels/AID", "interface_opcua",
                         "InteractionMetadata", "properties", "Pressure")
    http_ref = aimc_ref("a/submodels/AID", "interface_http",
                        "InteractionMetadata", "properties", "Pressure")
    for name, ref, aid_model in (("opcua-read", opcua_ref, _opcua_aid()),
                                 ("http-poll", http_ref, _http_aid())):
        model = resolve(build_aimc(_mapping(
            "Pressure", ref, [_sink()], interval=2.5, name=name)), aid_model)
        yield name, compile_descriptor(model, aas_base_url=BASE_URL,
                                       aas_id=AAS_ID, aas_id_short="contract")

    fine_ref = aimc_ref("a/submodels/AID", "interface_mqtt",
                        "InteractionMetadata", "properties", "SetPoint", "mode")
    fine = resolve(build_aimc(_mapping(
        "SetPoint", fine_ref, [aimc_sink("Out", fine_ref)], name="Fine")),
        build_aid(mqtt=_schema_iface({"SetPoint": ("/SET/SetPoint", SETPOINT_SCHEMA)})))
    yield "fine-path", compile_descriptor(fine, aas_base_url=BASE_URL,
                                          aas_id=AAS_ID, aas_id_short="contract")

    # schema-nested pointers on the NON-MQTT builders (Phase 4 T4.1/T4.2):
    # the runtime must read valuePath on http/opcua/plc4x sources and sinks,
    # not only on MQTT.
    from aas_model.testing import http_interface, opcua_interface
    from test_fine_resolution import NESTED_SCHEMA, _attach_schema

    http_nested = aimc_ref("a/submodels/AID", "interface_http",
                           "InteractionMetadata", "properties", "Pressure", "Reading")
    opcua_nested = aimc_ref("a/submodels/AID", "interface_opcua",
                            "InteractionMetadata", "properties", "Level", "Reading")
    nested = resolve(build_aimc(aimc_mapping(
        id_short="Nested",
        sources=[aimc_source("Pressure", http_nested),
                 aimc_source("Level", opcua_nested)],
        sinks=[aimc_sink("PressureOut", http_nested),
               aimc_sink("LevelOut", opcua_nested)],
        transformation=("function aimc_main(sources) return "
                        "{ PressureOut = sources.Pressure, LevelOut = sources.Level } end"),
    )), build_aid(
        http=_attach_schema(http_interface("http://192.168.0.30:8080/api",
                                           properties={"Pressure": "/pressure"}),
                            {"Pressure": NESTED_SCHEMA}),
        opcua=_attach_schema(opcua_interface("opc.tcp://192.168.0.40:4840/server",
                                             properties={"Level": "?id=ns=2;s=Level"}),
                             {"Level": NESTED_SCHEMA}),
    ))
    yield "fine-protocols", compile_descriptor(nested, aas_base_url=BASE_URL,
                                              aas_id=AAS_ID, aas_id_short="contract")

    mqtt = mqtt_interface("mqtt://192.168.0.104:1883/base",
                          properties={"State": "/state", "Mode": "/mode"})
    sources = [aimc_source(key, aimc_ref("a/submodels/AID", "interface_mqtt",
                                               "InteractionMetadata", "properties", key))
               for key in ("State", "Mode")]
    fan = aimc_mapping(id_short="Fan", sources=sources,
                       sinks=[_sink("Out"), _sink("Mirror")],
                       transformation="function aimc_main(sources) return { Out = sources.State, Mirror = sources.Mode } end")
    yield "fan-in-out", compile_descriptor(resolve(build_aimc(fan), build_aid(mqtt=mqtt)),
                                           aas_base_url=BASE_URL, aas_id=AAS_ID,
                                           aas_id_short="contract")


def test_contract_fixtures_are_generated(tmp_path):
    generated = list(contract_descriptors())
    assert len(generated) == 13
    for name, descriptor in generated:
        assert "routes.json" in descriptor.files, name
        assert json.loads(descriptor.files["routes.json"]), name
        descriptor.write_to(tmp_path / name)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python tests/test_runtime_contract_fixtures.py OUTPUT_DIRECTORY")
    destination = Path(sys.argv[1])
    for fixture_name, compiled in contract_descriptors():
        compiled.write_to(destination / fixture_name)
