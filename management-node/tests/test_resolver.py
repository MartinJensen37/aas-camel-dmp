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
    mqtt = next(m for m in result.mappings if m.id_short == "MQTT")

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
    mqtt = next(m for m in result.mappings if m.id_short == "MQTT")

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
