"""Tests for reference resolution (management_node.resolver)."""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.parsers import aid, aimc
from management_node.resolver import resolve

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
    assert source.mqtt is not None
    assert source.mqtt.retain is True
    assert source.mqtt.control_packet == "publish"
    assert source.data_point == "StationState"


def test_keeps_sink_references_and_blob(models):
    aid_model, aimc_model = models
    result = resolve(aimc_model, aid_model)
    mqtt = next(m for m in result.mappings if m.id_short == "StationState")

    assert mqtt.transformation is not None and "function aimc_main" in mqtt.transformation
    assert len(mqtt.sink_ids) == 2
    assert mqtt.sink_refs[0].as_path()[-1] == "PackMLState"


def test_unresolved_source_when_interface_missing():
    # an AIMC source pointing at an interface not present in the AID
    aimc_model = aimc.AimcModel(
        id_short="X",
        mappings=[
            aimc.AimcMapping(
                id_short="m",
                sources=[
                    aimc.AimcSource(
                        id_short="s",
                        source_id="s",
                        source=aimc.AimcReference(
                            type="ModelReference",
                            keys=[aimc.AimcReferenceKey(type="Submodel", value="a/submodels/AID"),
                                  aimc.AimcReferenceKey(type="SubmodelElementCollection", value="interface_kafka")],
                        ),
                    )
                ],
                sinks=[],
            )
        ],
    )
    aid_model = aid.AidModel(id_short="AID")
    result = resolve(aimc_model, aid_model)
    assert not result.all_resolved


def _halt_aid() -> aid.AidModel:
    """AID with interface_rest + interface_mqtt Halt actions."""
    return aid.AidModel(
        id_short="AID",
        interfaces={
            "interface_rest": aid.AidInterface(
                name="interface_rest",
                base="http://dmp-syntegonstopperingsystemaas:8080",
                actions={
                    "Halt": aid.AidAction(
                        name="Halt",
                        synchronous=True,
                        forms=[aid.AidForm(
                            href="/operations/syntegonStopperingSystemAAS/Halt",
                            method="POST",
                        )],
                    ),
                },
            ),
            "interface_mqtt": aid.AidInterface(
                name="interface_mqtt",
                base="mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering",
                actions={
                    "Halt": aid.AidAction(
                        name="Halt",
                        forms=[aid.AidForm(
                            href="/CMD/Halt",
                            response=aid.AidForm(href="/DATA/Halt"),
                        )],
                    ),
                },
            ),
        },
    )


def _refs():
    """Reference key builder for the two directional Halt mappings."""
    def ref(interface):
        return aimc.AimcReference(
            type="ModelReference",
            keys=[
                aimc.AimcReferenceKey(type="Submodel", value="a/submodels/AID"),
                aimc.AimcReferenceKey(type="SubmodelElementCollection", value=interface),
                aimc.AimcReferenceKey(type="SubmodelElementCollection", value="InteractionMetadata"),
                aimc.AimcReferenceKey(type="SubmodelElementCollection", value="actions"),
                aimc.AimcReferenceKey(type="SubmodelElementCollection", value="Halt"),
            ],
        )
    return ref("interface_rest"), ref("interface_mqtt")


def _halt_aimc() -> aimc.AimcModel:
    rest_ref, mqtt_ref = _refs()
    # A bidirectional (operation-delegation) mapping = TWO plain directional
    # MappingConfigurations: request (REST -> MQTT) + response (MQTT -> REST).
    return aimc.AimcModel(
        id_short="AIMC",
        mappings=[
            aimc.AimcMapping(
                id_short="HaltRequest",
                sources=[aimc.AimcSource(
                    id_short="Halt", source_id="Halt", source=rest_ref)],
                sinks=[aimc.AimcSink(id_short="Halt", sink_id="Halt", sink=mqtt_ref)],
                transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
            ),
            aimc.AimcMapping(
                id_short="Halt",
                sources=[aimc.AimcSource(
                    id_short="Halt", source_id="Halt", source=mqtt_ref)],
                sinks=[aimc.AimcSink(id_short="Halt", sink_id="Halt", sink=rest_ref)],
                transformation="function aimc_main(sources) return { Halt = sources.Halt } end",
            ),
        ],
    )


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
