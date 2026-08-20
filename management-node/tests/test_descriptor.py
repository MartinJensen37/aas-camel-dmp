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
    assert entry["topic"] == "/NN/Nybrovej/InnoLab/Stoppering/DATA/State"


def test_emits_lua_transformers_and_blob_files(descriptor):
    transformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert transformers
    for t in transformers:
        assert t["queryLanguage"] == "LUA"
        assert t["queryPath"] in descriptor.files, "blob file must be materialized"
        blob = descriptor.files[t["queryPath"]]
        assert "function aimc_main" in blob


def test_emits_aas_sink_config(descriptor):
    sinks = json.loads(descriptor.files[AAS_SERVER_FILE])
    assert sinks
    for s in sinks:
        assert s["type"] == "PROPERTY"
        assert s["submodelEndpoint"].startswith("http://aas-server:8081/")
        assert "/submodels/" in s["submodelEndpoint"]
        assert s["idShortPath"]

    # the MQTT mapping writes its variables back to the asset's Variables submodel
    variable_sinks = [s for s in sinks if "submodels/Variables" in s["submodelEndpoint"]]
    assert variable_sinks, "expected at least one sink on the Variables submodel"


def test_write_to_directory(descriptor, tmp_path):
    descriptor.write_to(tmp_path)
    assert (tmp_path / ROUTES_FILE).exists()
    assert (tmp_path / LUA_TRANSFORMERS_FILE).exists()
    # blob files live under queries/
    assert list((tmp_path / "queries").glob("*.lua"))
