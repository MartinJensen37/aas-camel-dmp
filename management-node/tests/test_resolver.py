"""Tests for reference resolution (management_node.resolver)."""

from __future__ import annotations

import json
import pathlib

import pytest

from aas_pydantic.submodel_templates.asset_interfaces_mapping_configuration import (
    DefaultPollingInterval,
)
from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface, opcua_interface, write_delegation_qualifier,
)
from management_node.parsers import aid, aimc
from management_node.resolver import resolve, reference_values

DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"


@pytest.fixture(scope="module")
def models():
    aid_model = aid.parse_aid(json.loads(DATA_AID.read_text()))
    aimc_model = aimc.parse_aimc(json.loads(DATA_AIMC.read_text()))
    return aid_model, aimc_model


def test_resolves_mqtt_sources_to_concrete_endpoints(models):
    aid_model, aimc_model = models
    result = resolve(aimc_model, aid_model)

    assert result.all_resolved, result.mappings
    mqtt = next(m for m in result.mappings if m.id_short == "mapping_0")

    source = mqtt.sources[0]
    assert source.protocol == "mqtt"
    assert source.interface == "interface_mqtt"
    assert source.base == "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
    assert source.topic == "/DATA/State"
    assert source.data_point == "StationState"
    # the DECLARED mqv_* qualifiers ride the resolved endpoint verbatim:
    # StationState is a retained last-known-value topic (UNS pattern)
    assert source.qos == 0
    assert source.retain is True


def test_keeps_sink_references_and_blob(models):
    aid_model, aimc_model = models
    result = resolve(aimc_model, aid_model)
    mqtt = next(m for m in result.mappings if m.id_short == "mapping_0")

    assert mqtt.transformation is not None and "function aimc_main" in mqtt.transformation
    assert len(mqtt.sink_ids) == 2
    assert reference_values(mqtt.sink_refs[0])[-1] == "PackMLState"


def test_unresolved_source_when_interface_missing():
    # an AIMC source pointing at an interface not present in the AID
    mc = aimc_mapping(
        sources=[aimc_source("s", aimc_ref("a/submodels/AID", "interface_kafka",
                                             "InteractionMetadata", "properties", "State"))],
        sinks=[],
    )
    aimc_model = build_aimc(mc)
    aid_model = build_aid()
    result = resolve(aimc_model, aid_model)
    assert not result.all_resolved


def test_interface_name_is_authored_not_a_protocol_prefix():
    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://broker:1883/line", properties={"State": "/state"}))
    aid_model.InterfaceTemplateForMQTT["PrimaryTelemetry"] = \
        aid_model.InterfaceTemplateForMQTT.pop("interface_mqtt")
    ref = aimc_ref("a/submodels/AID", "PrimaryTelemetry",
                   "InteractionMetadata", "properties", "State")
    result = resolve(build_aimc(aimc_mapping(
        sources=[aimc_source("State", ref)], sinks=[])), aid_model)
    assert result.mappings[0].sources[0].protocol == "mqtt"
    assert result.mappings[0].sources[0].topic == "/state"


def test_reference_group_cannot_select_same_named_property():
    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://broker:1883/line", properties={"State": "/state"}))
    ref = aimc_ref("a/submodels/AID", "interface_mqtt",
                   "InteractionMetadata", "actions", "State")
    result = resolve(build_aimc(aimc_mapping(
        sources=[aimc_source("State", ref)], sinks=[])), aid_model)
    assert result.mappings[0].sources[0].protocol == ""


def test_aas_property_named_interface_is_not_device_interface():
    from aas_pydantic import Key, ModelReference

    ref = ModelReference(key=(
        Key(type_="Submodel", value="urn:factory:telemetry"),
        Key(type_="Property", value="interface_temperature"),
    ))
    result = resolve(build_aimc(aimc_mapping(
        sources=[aimc_source("Temperature", ref)], sinks=[])), build_aid())
    source = result.mappings[0].sources[0]
    assert source.protocol == "aas"
    assert source.aas_property and source.aas_element_type == "Property"


def test_repository_element_type_must_match_reference_type():
    from aas_pydantic import Key, ModelReference

    ref = ModelReference(key=(
        Key(type_="Submodel", value="unit/submodels/Runtime"),
        Key(type_="SubmodelElementCollection", value="Temperature"),
    ))
    model = build_aimc(aimc_mapping(
        sources=[aimc_source("Temperature", ref)], sinks=[]))
    with pytest.raises(ValueError, match="repository stores Property"):
        resolve(model, build_aid(), element_type_for_reference=lambda _ref: "Property")


def test_duplicate_interface_name_across_templates_is_ambiguous():
    from aas_model.testing import AIDObservable, http_interface

    aid_model = build_aid(
        mqtt=mqtt_interface("mqtt://broker:1883/line", properties={"State": "/state"}),
        http=http_interface("http://device:8080/api", properties={"State": "/state"}),
    )
    aid_model.InterfaceTemplateForHTTP["interface_mqtt"] = \
        aid_model.InterfaceTemplateForHTTP.pop("interface_http")
    ref = aimc_ref("a/submodels/AID", "interface_mqtt",
                   "InteractionMetadata", "properties", "State")
    with pytest.raises(ValueError, match="ambiguous"):
        resolve(build_aimc(aimc_mapping(
            sources=[aimc_source("State", ref)], sinks=[])), aid_model)


def test_http_observable_without_event_consumer_is_rejected():
    from aas_model.testing import AIDObservable, http_interface

    ref = aimc_ref("a/submodels/AID", "interface_http",
                   "InteractionMetadata", "properties", "State")
    iface = http_interface("http://device:8080/api", properties={"State": "/state"})
    iface.InteractionMetadata.properties.property_name["State"].observable = \
        AIDObservable(value="true")
    aid_model = build_aid(http=iface)
    with pytest.raises(ValueError, match="no asynchronous event consumer"):
        resolve(build_aimc(aimc_mapping(
            sources=[aimc_source("State", ref)], sinks=[])), aid_model)


# --------------------------------------------------------------------------- #
# Operation delegation (DMP v3): the SOURCE reference targets an Operation
# SubmodelElement (last key type ``Operation``) — one mapping per operation,
# sink = native command affordance, REST surface derived by convention.
# --------------------------------------------------------------------------- #
def _halt_aid():
    # the reply topic is DECLARED via the action's response form (ADR-020):
    # no /CMD/->/DATA/ rewriting exists anywhere anymore
    return build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
        actions={"Halt": ("/CMD/Halt", "/DATA/Halt")}))


def _operation_ref(anchor: str = "{aas_id}/submodels/ControlComponentInstance"):
    from aas_pydantic import Key, ModelReference
    return ModelReference(key=(
        Key(type_="Submodel", value=anchor),
        Key(type_="SubmodelElementCollection", value="Skills"),
        Key(type_="SubmodelElementCollection", value="Halt"),
        Key(type_="Operation", value="operation"),
    ))


def _halt_aimc(anchor: str = "{aas_id}/submodels/ControlComponentInstance"):
    mqtt_ref = aimc_ref("{aas_id}/submodels/AID", "interface_mqtt",
                        "InteractionMetadata", "actions", "Halt")
    # one-MC operation authoring: Operation-element source → native command
    # sink, Transformation = request blob, no response half
    return build_aimc(aimc_mapping(
        id_short="Halt",
        sources=[aimc_source("Halt", _operation_ref(anchor))],
        sinks=[aimc_sink("Halt", mqtt_ref)],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    ))


def test_resolves_operation_mapping_to_derived_interaction():
    result = resolve(_halt_aimc(), _halt_aid())

    assert result.all_resolved, result.interactions[0].unresolved
    # the interaction mapping never enters mappings — it is not a data poll
    assert result.mappings == []
    interaction = result.interactions[0]
    assert interaction.id_short == "Halt"
    # REST surface derived by convention; the {aas_id_short} macro resolves
    # at descriptor compile time
    assert interaction.rest_path == "/operations/{aas_id_short}/Halt"
    assert interaction.rest_method == "POST"
    assert interaction.protocol == "mqtt"
    # the command leg rides the resolved native command affordance
    sink = interaction.command_sink
    assert sink.base == "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
    assert sink.topic == "/CMD/Halt"
    assert sink.response_topic == "DATA/Halt"
    assert "function aimc_main" in interaction.request_blob
    assert interaction.response_blob is None


def test_resolves_cross_aas_operation_source():
    """A cross-AAS Operation reference classifies the same way — the key types
    alone identify the delegation, no ownership scan."""
    local = _halt_aid()
    result = resolve(
        _halt_aimc("other/submodels/ControlComponentInstance"), local,
        aid_for_anchor=lambda anchor: local if anchor == "{aas_id}/submodels/AID" else None,
    )
    interaction = result.interactions[0]
    assert interaction.id_short == "Halt"
    assert interaction.command_sink.topic == "/CMD/Halt"


def test_interaction_without_declared_response_form_is_unresolved():
    # no forms.response authored: loud unresolved, never a topic rewrite
    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN",
        actions={"Halt": "/CMD/Halt"}))
    result = resolve(_halt_aimc(), aid_model)
    assert not result.all_resolved
    assert any("response form" in reason for reason in
               result.interactions[0].unresolved)


def test_interaction_without_transformation_blob_is_unresolved():
    aid_model = _halt_aid()
    mqtt_ref = aimc_ref("{aas_id}/submodels/AID", "interface_mqtt",
                        "InteractionMetadata", "actions", "Halt")
    mc = aimc_mapping(
        id_short="Halt",
        sources=[aimc_source("Halt", _operation_ref())],
        sinks=[aimc_sink("Halt", mqtt_ref)],
    )
    result = resolve(build_aimc(mc), aid_model)
    assert not result.all_resolved
    assert any("Transformation" in reason for reason in
               result.interactions[0].unresolved)


def test_mixed_operation_and_affordance_sources_are_unresolved():
    """An operation delegation maps exactly one operation invocation — a
    mapping mixing Operation-element and affordance sources fails loudly."""
    aid_model = _halt_aid()
    mqtt_ref = aimc_ref("{aas_id}/submodels/AID", "interface_mqtt",
                        "InteractionMetadata", "actions", "Halt")
    prop_ref = aimc_ref("{aas_id}/submodels/AID", "interface_mqtt",
                        "InteractionMetadata", "properties", "StationState")
    mc = aimc_mapping(
        id_short="Halt",
        sources=[aimc_source("Halt", _operation_ref()),
                 aimc_source("StationState", prop_ref)],
        sinks=[aimc_sink("Halt", mqtt_ref)],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    )
    result = resolve(build_aimc(mc), aid_model)
    assert not result.all_resolved
    assert any("mixes" in reason for reason in
               result.interactions[0].unresolved)


def test_non_command_sink_leaves_interaction_unresolved():
    """The sink must be a native command affordance: a property
    affordance (or an AAS property sink) is not a command target —
    the interaction stays unresolved instead of wiring a wrong endpoint."""
    aid_model = _halt_aid()
    # sink = the StationState PROPERTY affordance (not an action)
    prop_ref = aimc_ref("{aas_id}/submodels/AID", "interface_mqtt",
                        "InteractionMetadata", "properties", "StationState")
    mc = aimc_mapping(
        id_short="Halt",
        sources=[aimc_source("Halt", _operation_ref())],
        sinks=[aimc_sink("Halt", prop_ref)],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    )
    result = resolve(build_aimc(mc), aid_model)
    assert not result.all_resolved
    assert any("no native command affordance" in reason
               for reason in result.interactions[0].unresolved)
    assert result.interactions[0].command_sink is None


def test_resolves_opcua_method_action():
    """OPC-10101 §7.1.3: a UA Method maps to an action affordance whose form
    href carries the Method NodeId (§6.2 convention) and which MUST declare
    ``uav_componentOf`` — the owning Object for the UA Call service."""
    from aas_model.testing import opcua_interface
    aid_model = build_aid(opcua=opcua_interface(
        "opc.tcp://192.168.0.90:4840",
        actions={"Calibrate": ("/?id=ns=2;s=RunCalibration", "POST",
                               "ns=1;s=CalibrationObject")}))
    opcua_ref = aimc_ref("{aas_id}/submodels/AID", "interface_opcua",
                         "InteractionMetadata", "actions", "Calibrate")
    mc = aimc_mapping(
        id_short="Calibrate",
        sources=[aimc_source("Calibrate", _operation_ref())],
        sinks=[aimc_sink("Calibrate", opcua_ref)],
        transformation="function aimc_main(sources) return { Calibrate = sources.Calibrate } end",
    )
    result = resolve(build_aimc(mc), aid_model)
    assert result.all_resolved, result.interactions[0].unresolved
    interaction = result.interactions[0]
    assert interaction.protocol == "opcua"
    sink = interaction.command_sink
    # §6.2: the href's NodeId is the METHOD id; §7.1.3: componentOf is the
    # owning Object the Call service addresses
    assert sink.method_node == "ns=2;s=RunCalibration"
    assert sink.component_of == "ns=1;s=CalibrationObject"
    assert sink.node == "ns=1;s=CalibrationObject"


def test_resolves_http_action_interaction():
    """The command affordance may live on ANY protocol interface: an HTTP
    action affordance resolves the same way (the HTTP response is the
    protocol-inherent reply — no response form required)."""
    from aas_model.testing import http_interface
    aid_model = build_aid(http=http_interface(
        "http://192.168.0.60:8080/plc/api",
        actions={"Restart": ("/actions/Restart", "POST")}))
    http_ref = aimc_ref("{aas_id}/submodels/AID", "interface_http",
                        "InteractionMetadata", "actions", "Restart")
    mc = aimc_mapping(
        id_short="Restart",
        sources=[aimc_source("Restart", _operation_ref())],
        sinks=[aimc_sink("Restart", http_ref)],
        transformation="function aimc_main(sources) return { Restart = sources.Restart } end",
    )
    result = resolve(build_aimc(mc), aid_model)
    assert result.all_resolved, result.interactions[0].unresolved
    interaction = result.interactions[0]
    assert interaction.protocol == "rest"
    assert interaction.command_sink.base == "http://192.168.0.60:8080/plc/api"
    assert interaction.command_sink.topic == "/actions/Restart"
    # a synchronous protocol carries its reply inherently — no response form
    # is required (unlike MQTT's correlated reply leg)


# --------------------------------------------------------------------------- #
# Dual-binding operation delegations (DMP v3): one CCI operation mapped once
# per native binding (MQTT command + OPC UA method call). The derived REST
# surface must stay unambiguous — the MQTT leg keeps the plain path (the
# surface the invocationDelegation qualifier advertises), further bindings
# get a deterministic /{protocol} sub-resource.
# --------------------------------------------------------------------------- #
def _dual_binding_aid():
    from aas_model.testing import opcua_interface
    return build_aid(
        mqtt=mqtt_interface(
            "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
            actions={"Halt": ("/CMD/Halt", "/DATA/Halt")}),
        opcua=opcua_interface(
            "opc.tcp://192.168.0.104:4840/stoppering/",
            actions={"Halt": ("/?id=ns=2;s=HaltMethod", "invokeaction",
                              "ns=2;s=Halt")}),
    )


def _halt_delegation_mc(id_short, interface_name):
    return aimc_mapping(
        id_short=id_short,
        sources=[aimc_source("Halt", _operation_ref())],
        sinks=[aimc_sink("Halt", aimc_ref(
            "{aas_id}/submodels/AID", interface_name,
            "InteractionMetadata", "actions", "Halt"))],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    )


def test_dual_binding_delegation_disambiguates_paths():
    result = resolve(build_aimc(
        _halt_delegation_mc("Halt", "interface_mqtt"),
        _halt_delegation_mc("HaltOpcua", "interface_opcua"),
    ), _dual_binding_aid())
    assert result.all_resolved, result.interactions
    mqtt_leg = next(i for i in result.interactions if i.protocol == "mqtt")
    ua_leg = next(i for i in result.interactions if i.protocol == "opcua")
    # plain path stays on the MQTT leg; the OPC UA leg is a sub-resource
    assert mqtt_leg.rest_path == "/operations/{aas_id_short}/Halt"
    assert ua_leg.rest_path == "/operations/{aas_id_short}/Halt/opcua"


def test_opcua_only_delegation_keeps_plain_path():
    result = resolve(build_aimc(
        _halt_delegation_mc("HaltOpcua", "interface_opcua"),
    ), _dual_binding_aid())
    assert result.all_resolved, result.interactions
    assert result.interactions[0].rest_path == "/operations/{aas_id_short}/Halt"


# --------------------------------------------------------------------------- #
# Property write delegations (W2, Shape A): the AIMC mapping carries the
# writeDelegation qualifier; the SOURCE is the device affordance to write and
# the SINK is the AAS property that receives the effective value after the
# device acknowledges. Authored list-item names do not survive the
# SubmodelElementList round-trip (AAS spec: list elements carry no idShort) —
# placeholder-named delegations are renamed from the device reference.
# --------------------------------------------------------------------------- #

REQUEST_BLOB = (
    "function aimc_main(sources)\n"
    "    return { Parameter = sources.Parameter.value }\n"
    "end\n"
)
RESPONSE_BLOB = (
    "function aimc_main(sources)\n"
    "    return { Parameter = sources.Parameter.value }\n"
    "end\n"
)


def _writable_property_aid():
    """AID with a writable Parameter affordance on BOTH bindings: the MQTT
    command/reply property pair (op writeproperty + declared ack topic) and
    the writable OPC UA variable (§6.2 href NodeId)."""
    return build_aid(
        mqtt=mqtt_interface(
            "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
            properties={"Parameter": ("/CMD/Parameter", "/DATA/Parameter",
                                      "writeproperty")}),
        opcua=opcua_interface(
            "opc.tcp://192.168.0.104:4840/stoppering/",
            properties={"Parameter": "/?id=ns=2;s=Parameter"},
            op="writeproperty"),
    )


def _write_mc(interface_name, *, id_short="mapping", response_blob=RESPONSE_BLOB,
              qualifier=True):
    sources = [aimc_source("Parameter", aimc_ref(
        "{aas_id}/submodels/AID", interface_name,
        "InteractionMetadata", "properties", "Parameter"))]
    sinks = [aimc_sink("Parameter", aimc_ref(
        "{aas_id}/submodels/Variables", "Parameter"))]
    return aimc_mapping(
        id_short=id_short,
        sources=sources, sinks=sinks,
        transformation=REQUEST_BLOB,
        response_transformation=response_blob,
        qualifiers=[write_delegation_qualifier()] if qualifier else None,
    )


def test_mqtt_write_delegation_resolves_ack_and_rest_path():
    result = resolve(build_aimc(_write_mc("interface_mqtt")),
                     _writable_property_aid())
    assert result.all_resolved, [w.unresolved for w in result.property_writes]
    write = result.property_writes[0]
    assert write.protocol == "mqtt"
    # placeholder name → derived from the device reference
    assert write.id_short == "Parameter_mqtt"
    # the correlated ack rides the DECLARED response form
    assert write.device_source.response_topic == "DATA/Parameter"
    assert write.device_source.topic == "/CMD/Parameter"
    # the qualifier's {delegation_base} tail is the derived caller surface
    assert write.rest_path == "/properties/{aas_id_short}/Parameter"
    assert write.rest_method == "PUT"
    assert write.request_blob is not None and write.response_blob is not None
    assert write.writeback_keys[-1] == "Parameter"
    assert write.writeback_sink_id == "Parameter"


def test_opcua_write_delegation_resolves_node():
    result = resolve(build_aimc(_write_mc("interface_opcua")),
                     _writable_property_aid())
    assert result.all_resolved, [w.unresolved for w in result.property_writes]
    write = result.property_writes[0]
    assert write.protocol == "opcua"
    assert write.id_short == "Parameter_opcua"
    # the §6.2 href NodeId of the writable variable
    assert write.device_source.node == "ns=2;s=Parameter"
    assert write.rest_path == "/properties/{aas_id_short}/Parameter"


def test_write_delegation_keeps_authored_name():
    result = resolve(build_aimc(_write_mc("interface_mqtt",
                                          id_short="ParameterWriteMqtt")),
                     _writable_property_aid())
    assert result.all_resolved, [w.unresolved for w in result.property_writes]
    assert result.property_writes[0].id_short == "ParameterWriteMqtt"


def test_write_delegation_name_collision_gets_index_suffix():
    result = resolve(build_aimc(
        _write_mc("interface_mqtt"), _write_mc("interface_mqtt")),
        _writable_property_aid())
    names = [w.id_short for w in result.property_writes]
    assert names[0] == "Parameter_mqtt"
    assert names[1] == "Parameter_mqtt_1"


def test_mqtt_write_delegation_requires_response_form_and_blobs():
    # no declared ack form and no ack → envelope blob: unresolved, both defects
    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
        properties={"Parameter": ("/CMD/Parameter", None, "writeproperty")}))
    mc = aimc_mapping(
        sources=[aimc_source("Parameter", aimc_ref(
            "{aas_id}/submodels/AID", "interface_mqtt",
            "InteractionMetadata", "properties", "Parameter"))],
        sinks=[aimc_sink("Parameter", aimc_ref(
            "{aas_id}/submodels/Variables", "Parameter"))],
        transformation=REQUEST_BLOB,
        qualifiers=[write_delegation_qualifier()],
    )
    result = resolve(build_aimc(mc), aid_model)
    write = result.property_writes[0]
    assert not result.all_resolved
    assert any("no response form" in u for u in write.unresolved), write.unresolved
    assert any("ResponseTransformation" in u for u in write.unresolved), write.unresolved


def test_write_delegation_rest_path_falls_back_to_writeback_keys():
    # a qualifier value without the {delegation_base} convention: the derived
    # surface comes from the writeback reference's last key
    qualifier = write_delegation_qualifier()
    qualifier.value = "true"
    mc = _write_mc("interface_mqtt")
    mc.qualifiers = [qualifier]
    result = resolve(build_aimc(mc), _writable_property_aid())
    assert result.all_resolved, [w.unresolved for w in result.property_writes]
    assert result.property_writes[0].rest_path == \
        "/properties/{aas_id_short}/Parameter"


def test_write_delegation_rejects_device_affordance_writeback():
    # the writeback sink must be the AAS property, never a device affordance
    mc = aimc_mapping(
        sources=[aimc_source("Parameter", aimc_ref(
            "{aas_id}/submodels/AID", "interface_mqtt",
            "InteractionMetadata", "properties", "Parameter"))],
        sinks=[aimc_sink("Parameter", aimc_ref(
            "{aas_id}/submodels/AID", "interface_mqtt",
            "InteractionMetadata", "properties", "Parameter"))],
        transformation=REQUEST_BLOB, response_transformation=RESPONSE_BLOB,
        qualifiers=[write_delegation_qualifier()],
    )
    result = resolve(build_aimc(mc), _writable_property_aid())
    write = result.property_writes[0]
    assert any("must be the AAS property" in u for u in write.unresolved), \
        write.unresolved


def test_data_mapping_with_qualifier_keyword_stays_data_mapping():
    # the qualifier must carry the writeDelegation type — a mapping without it
    # (same shape otherwise) resolves as a plain data mapping
    mc = aimc_mapping(
        sources=[aimc_source("Parameter", aimc_ref(
            "{aas_id}/submodels/AID", "interface_mqtt",
            "InteractionMetadata", "properties", "Parameter"))],
        sinks=[aimc_sink("Parameter", aimc_ref(
            "{aas_id}/submodels/Variables", "Parameter"))],
        transformation=REQUEST_BLOB,
    )
    result = resolve(build_aimc(mc), _writable_property_aid())
    assert len(result.property_writes) == 0
    assert len(result.mappings) == 1


def test_declared_polling_interval_pulls_mqtt_affordance():
    """An EXPLICIT IDTA polling interval on an MQTT source overrides the
    transport-default push classification: the DMP samples the retained
    topic on a timer route (scenario A2, the sync surface)."""
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
    aid_model = build_aid(mqtt=mqtt_interface(
        "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
        properties={"StationState": "/DATA/State"}))
    result = resolve(build_aimc(mc), aid_model)
    assert result.all_resolved, result.mappings[0].unresolved
    source = result.mappings[0].sources[0]
    assert source.delivery == "sync"
    assert source.polling_interval == 2.0
