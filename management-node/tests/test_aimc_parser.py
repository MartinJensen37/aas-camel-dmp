"""Tests for the dedicated AIMC parser (management_node.parsers.aimc).

Uses the real AIMC submodel extracted from
``AASDescriptions/Resource/configs/syntegonStoppering.json``.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.parsers import aimc

DATA = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"


@pytest.fixture(scope="module")
def submodel() -> dict:
    return json.loads(DATA.read_text())


def test_parses_mapping_configurations(submodel):
    model = aimc.parse_aimc(submodel)
    assert model.id_short == "AssetInterfacesMappingConfiguration"
    assert len(model.mappings) == 5


def test_parses_sources_with_references(submodel):
    model = aimc.parse_aimc(submodel)
    mqtt = next(m for m in model.mappings if m.id_short == "MQTT")

    assert len(mqtt.sources) == 1
    source = mqtt.sources[0]
    assert source.source_id == "StationState"
    assert source.polling_interval is None
    # reference path into the AID
    assert source.source.type == "ModelReference"
    assert source.source.as_path()[-1] == "StationState"
    assert "interface_mqtt" in source.source.as_path()


def test_parses_transformation_lua_blob(submodel):
    model = aimc.parse_aimc(submodel)
    mqtt = next(m for m in model.mappings if m.id_short == "MQTT")

    assert mqtt.transformation is not None
    assert "function aimc_main" in mqtt.transformation


def test_parses_sinks(submodel):
    model = aimc.parse_aimc(submodel)
    mqtt = next(m for m in model.mappings if m.id_short == "MQTT")

    assert len(mqtt.sinks) == 2
    sink_ids = {s.sink_id for s in mqtt.sinks}
    assert sink_ids == {"PackMLState", "OccupationState"}
    for sink in mqtt.sinks:
        # sink references point at the asset's own Variables submodel
        assert "submodels/Variables" in sink.sink.as_path()[0]


def test_accepts_json_string(submodel):
    model = aimc.parse_aimc(json.dumps(submodel))
    assert len(model.mappings) == 5


def test_empty_input_produces_empty_model():
    model = aimc.parse_aimc({})
    assert model.mappings == []
