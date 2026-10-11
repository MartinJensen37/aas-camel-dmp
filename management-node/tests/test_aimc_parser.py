"""Tests for the AIMC parser (management_node.parsers.aimc), which now returns
the shared ``aas_model.Aimc`` pydantic model (ADR-016).

Uses the real AIMC submodel extracted from
``AASDescriptions/Resource/configs/syntegonStoppering.json``.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.parsers import aimc
from management_node.resolver import reference_values, resolve
from management_node.parsers import aid

DATA = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"
DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"


@pytest.fixture(scope="module")
def submodel() -> dict:
    return json.loads(DATA.read_text())


@pytest.fixture(scope="module")
def mcs(submodel):
    return aimc.parse_aimc(submodel).MappingConfigurations.value


def _resolved_aimc(submodel):
    return resolve(aimc.parse_aimc(submodel),
                   aid.parse_aid(json.loads(DATA_AID.read_text())))


def test_parses_mapping_configurations(submodel):
    assert len(aimc.parse_aimc(submodel).MappingConfigurations.value) == 5


def test_parses_sources_with_references(submodel):
    mqtt = next(m for m in _resolved_aimc(submodel).mappings if m.id_short == "mapping_0")

    assert len(mqtt.sources) == 1
    source = mqtt.sources[0]
    assert source.source_id == "StationState"
    assert source.polling_interval is None
    # reference path into the AID (via the resolved sink refs, same refs)
    assert "interface_mqtt" in mqtt.sources[0].interface


def test_parses_transformation_lua_blob(submodel):
    mqtt = next(m for m in _resolved_aimc(submodel).mappings if m.id_short == "mapping_0")
    assert mqtt.transformation is not None
    assert "function aimc_main" in mqtt.transformation


def test_parses_sinks(submodel):
    mqtt = next(m for m in _resolved_aimc(submodel).mappings if m.id_short == "mapping_0")
    assert set(mqtt.sink_ids) == {"PackMLState", "OccupationState"}
    # sink references point at the asset's own Variables submodel
    assert all("submodels/Variables" in reference_values(r)[0] for r in mqtt.sink_refs)


def test_accepts_json_string(submodel):
    model = aimc.parse_aimc(json.dumps(submodel))
    assert len(model.MappingConfigurations.value) == 5


def test_resolved_mapping_has_no_engine_selector(submodel):
    mqtt = next(m for m in _resolved_aimc(submodel).mappings if m.id_short == "mapping_0")
    assert not hasattr(mqtt, "transformation_engine")


def test_empty_input_returns_none():
    assert aimc.parse_aimc({}) is None
