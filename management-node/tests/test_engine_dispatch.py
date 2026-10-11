"""Lua-only transformation compilation and retired-engine rejection tests."""

from __future__ import annotations

import json

import pytest

from management_node.descriptor import LUA_TRANSFORMER_FILE, ROUTES_FILE, compile_descriptor
from management_node.resolver import resolve

from aas_model.testing import (
    aimc_mapping, aimc_ref, aimc_sink, aimc_source, build_aid, build_aimc,
    mqtt_interface,
)
from aas_model.resource_template.asset_interfaces_mapping_configuration import operation_ref

BASE = "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
AIMC_TRANSFORMATION_SID = (
    "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/"
    "2/0/MappingConfiguration/Transformation"
)
JSONATA_SID = "https://example.org/jsonata/expression/1/0"


def _aid():
    return build_aid(mqtt=mqtt_interface(BASE, properties={
        "StationState": "/DATA/State", "StationMode": "/DATA/Mode"}))


def _ref(data_point: str):
    return aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                    "properties", data_point)


def _mapping(id_short: str, blob: str, data_point: str = "StationState", sid: str = AIMC_TRANSFORMATION_SID):
    return aimc_mapping(
        id_short=id_short,
        sources=[aimc_source(data_point, _ref(data_point))],
        sinks=[aimc_sink("Out", aimc_ref("{aas_id}/submodels/Variables", "Out"))],
        transformation=blob,
        transformation_semantic_id=sid,
    )


def _compile(*mappings):
    return compile_descriptor(
        resolve(build_aimc(*mappings), _aid()),
        aas_base_url="http://aas-server:8081",
        aas_id="syntegonStopperingSystemAAS",
        aas_id_short="syntegonStopperingSystem",
    )


def test_mapping_emits_only_lua_configuration_with_decoded_value_contract():
    lua = "function aimc_main(sources) return { Out = sources.StationState } end"
    descriptor = _compile(_mapping("StateMap", lua))

    assert LUA_TRANSFORMER_FILE in descriptor.files
    assert "jsonatatransformer.json" not in descriptor.files
    transformers = json.loads(descriptor.files[LUA_TRANSFORMER_FILE])
    assert len(transformers) == 1
    entry = transformers[0]
    assert entry["queryPath"] == "queries/syntegonStopperingSystem_StateMap.lua"
    assert entry["sourceKeys"] == "StationState"
    assert not ({"queryLanguage", "inputType", "outputType"} & entry.keys())
    assert descriptor.files[entry["queryPath"]] == lua
    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["transformers"] == [entry["uniqueId"]]


def test_retired_jsonata_semantic_id_fails_loudly_instead_of_running_as_lua():
    with pytest.raises(ValueError, match="JSONata transformations are no longer supported"):
        _compile(_mapping("StateMap", '{"Out": station_state}', sid=JSONATA_SID))


@pytest.mark.parametrize("action", [False, True], ids=["ordinary", "action"])
@pytest.mark.parametrize("blob_name", ["Transformation", "ResponseTransformation"])
@pytest.mark.parametrize("supplemental", [False, True], ids=["semantic", "supplemental"])
def test_retired_jsonata_is_rejected_on_both_mapping_legs(action, blob_name, supplemental):
    lua = "function aimc_main(sources) return {} end"
    mapping = aimc_mapping(
        sources=[aimc_source("Invoke", operation_ref("Move"))] if action
                else [aimc_source("StationState", _ref("StationState"))],
        sinks=[aimc_sink("Command", aimc_ref("a/submodels/AID", "interface_mqtt",
                                             "InteractionMetadata", "actions", "Move"))] if action
              else [aimc_sink("Out", aimc_ref("a/submodels/Variables", "Out"))],
        transformation=lua,
        response_transformation=lua,
    )
    blob = getattr(mapping, blob_name)
    if supplemental:
        blob.supplemental_semantic_ids = [JSONATA_SID.upper()]
    else:
        blob.semantic_id = JSONATA_SID
    aid = build_aid(mqtt=mqtt_interface(BASE, properties={"StationState": "/state"},
                                       actions={"Move": ("/move", "/reply")}))
    with pytest.raises(ValueError, match="JSONata.*" + blob_name):
        resolve(build_aimc(mapping), aid)


def test_standard_aimc_transformation_blob_remains_lua():
    descriptor = _compile(_mapping(
        "StateMap", "function aimc_main(sources) return { Out = sources.StationState } end"))
    entry = json.loads(descriptor.files[LUA_TRANSFORMER_FILE])[0]
    assert entry["queryPath"].endswith(".lua")


def test_identical_push_feeds_across_mappings_fail_compilation():
    with pytest.raises(ValueError, match="source-overlap"):
        _compile(
            _mapping("LuaMap", "function aimc_main(sources) return { Out = sources.StationState } end"),
            _mapping("SecondLuaMap", "function aimc_main(sources) return { Out = sources.StationState } end"),
        )
