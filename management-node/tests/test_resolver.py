"""Tests for reference resolution (management_node.resolver)."""

from __future__ import annotations

import json
import pathlib

import pytest

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface, rest_interface,
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
    mqtt = next(m for m in result.mappings if m.id_short == "StationState")

    source = mqtt.sources[0]
    assert source.protocol == "mqtt"
    assert source.interface == "interface_mqtt"
    assert source.base == "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
    assert source.topic == "/DATA/State"
    assert source.data_point == "StationState"


def test_keeps_sink_references_and_blob(models):
    aid_model, aimc_model = models
    result = resolve(aimc_model, aid_model)
    mqtt = next(m for m in result.mappings if m.id_short == "StationState")

    assert mqtt.transformation is not None and "function aimc_main" in mqtt.transformation
    assert len(mqtt.sink_ids) == 2
    assert reference_values(mqtt.sink_refs[0])[-1] == "PackMLState"


def test_unresolved_source_when_interface_missing():
    # an AIMC source pointing at an interface not present in the AID
    mc = aimc_mapping(
        sources=[aimc_source("s", aimc_ref("a/submodels/AID", "interface_kafka"))],
        sinks=[],
    )
    aimc_model = build_aimc(mc)
    aid_model = build_aid()
    result = resolve(aimc_model, aid_model)
    assert not result.all_resolved


def _halt_aid():
    return build_aid(
        rest=rest_interface("http://dmp-syntegonstopperingsystemaas:8080",
                            actions={"Halt": "/operations/syntegonStopperingSystemAAS/Halt"}),
        mqtt=mqtt_interface("mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
                            actions={"Halt": "/CMD/Halt"}),
    )


def _halt_aimc():
    rest_ref = aimc_ref("a/submodels/AID", "interface_rest", "InteractionMetadata",
                        "actions", "Halt")
    mqtt_ref = aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                        "actions", "Halt")
    request = aimc_mapping(
        id_short="HaltRequest",
        sources=[aimc_source("Halt", rest_ref)],
        sinks=[aimc_sink("Halt", mqtt_ref)],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    )
    response = aimc_mapping(
        id_short="Halt",
        sources=[aimc_source("Halt", mqtt_ref)],
        sinks=[aimc_sink("Halt", rest_ref)],
        transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
    )
    return build_aimc(request, response)


def test_resolves_bidirectional_mapping_to_rest_and_mqtt_endpoints():
    result = resolve(_halt_aimc(), _halt_aid())

    assert result.all_resolved, result.bidirectional[0].unresolved
    bidi = result.bidirectional[0]
    assert bidi.id_short == "Halt"
    assert bidi.rest_base == "http://dmp-syntegonstopperingsystemaas:8080"
    assert bidi.rest_path == "/operations/syntegonStopperingSystemAAS/Halt"
    assert bidi.rest_method == "POST"
    assert bidi.mqtt_command_topic == "NN/Nybrovej/InnoLab/Stoppering/CMD/Halt"
    assert bidi.mqtt_response_topic == "NN/Nybrovej/InnoLab/Stoppering/DATA/Halt"
    assert "function aimc_main" in bidi.request_blob
    assert "function aimc_main" in bidi.response_blob
