"""Tests for transformation engine dispatch by semantic id (ADR-015 / SPEC §7.6).

The AIMC ``Transformation`` blob's semantic id selects Lua (AIMC-compliant,
default) or JSONata (fast/simple). Both share the same SourceId/SinkId contract
and the same dis/assembly pipeline; the descriptor emits ``luatransformers.json``
or ``jsonatatransformer.json`` accordingly.
"""

from __future__ import annotations

import json

import pytest

from management_node.descriptor import (
    JSONATA_TRANSFORMERS_FILE,
    LUA_TRANSFORMERS_FILE,
    ROUTES_FILE,
    compile_descriptor,
)
from management_node.resolver import resolve

from aas_model.testing import (
    build_aid, build_aimc, aimc_mapping, aimc_source, aimc_sink, aimc_ref,
    mqtt_interface,
)

BASE = "mqtt://192.168.0.104:1883/NN/Nybrovej/InnoLab/Stoppering"
AIMC_LUA_SID = ("https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/"
                "2/0/MappingConfiguration/Transformation")
JSONATA_SID = "https://example.org/jsonata/expression/1/0"


def _aid():
    return build_aid(mqtt=mqtt_interface(BASE, properties={"StationState": "/DATA/State",
                                                           "StationMode": "/DATA/Mode"}))


def _ref(data_point: str = "StationState"):
    return aimc_ref("a/submodels/AID", "interface_mqtt", "InteractionMetadata",
                    "properties", data_point)


def _vars_ref():
    return aimc_ref("{aas_id}/submodels/Variables", "Out")


def _mapping(id_short: str, sid: str, blob: str, data_point: str = "StationState"):
    return aimc_mapping(
        id_short=id_short,
        sources=[aimc_source(data_point, _ref(data_point))],
        sinks=[aimc_sink("Out", _vars_ref())],
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


def test_jsonata_semantic_id_dispatches_to_jsonata_transformer():
    descriptor = _compile(_mapping("StateMap", JSONATA_SID, '{"out": station_state}'))

    assert JSONATA_TRANSFORMERS_FILE in descriptor.files
    assert LUA_TRANSFORMERS_FILE not in descriptor.files

    xformers = json.loads(descriptor.files[JSONATA_TRANSFORMERS_FILE])
    assert len(xformers) == 1
    t = xformers[0]
    assert t["queryLanguage"] == "JSONATA"
    assert t["queryPath"] == "queries/syntegonStopperingSystem_StateMap.jsonata"
    assert descriptor.files[t["queryPath"]] == '{"out": station_state}'

    route = json.loads(descriptor.files[ROUTES_FILE])[0]
    assert route["transformers"] == [t["uniqueId"]]


def test_aimc_default_semantic_id_uses_lua():
    descriptor = _compile(_mapping("StateMap", AIMC_LUA_SID,
                                   "function aimc_main(sources) return { x = sources.StationState } end"))

    assert LUA_TRANSFORMERS_FILE in descriptor.files
    assert JSONATA_TRANSFORMERS_FILE not in descriptor.files

    xformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert xformers[0]["queryLanguage"] == "LUA"
    assert xformers[0]["queryPath"].endswith(".lua")


def test_jsonata_outside_private_namespace_uses_lua():
    sid = "https://other.example.org/transforms/jsonata/expression/1/0"
    descriptor = _compile(_mapping(
        "StateMap", sid,
        "function aimc_main(sources) return { x = sources.StationState } end",
    ))

    assert LUA_TRANSFORMERS_FILE in descriptor.files
    assert JSONATA_TRANSFORMERS_FILE not in descriptor.files
    xformers = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    assert xformers[0]["queryLanguage"] == "LUA"
    assert xformers[0]["queryPath"].endswith(".lua")


def test_mixed_engines_in_one_asset():
    """Two mappings of one asset may each pick their engine; they consume
    DISTINCT feeds (identical push feeds across mappings fail compilation —
    the source-overlap guard, W2.7)."""
    descriptor = _compile(
        _mapping("LuaMap", AIMC_LUA_SID,
                 "function aimc_main(sources) return { x = sources.StationState } end"),
        _mapping("FastMap", JSONATA_SID, '{"x": station_mode}', "StationMode"),
    )

    lua = json.loads(descriptor.files[LUA_TRANSFORMERS_FILE])
    jx = json.loads(descriptor.files[JSONATA_TRANSFORMERS_FILE])
    assert [t["queryLanguage"] for t in lua] == ["LUA"]
    assert [t["queryLanguage"] for t in jx] == ["JSONATA"]

    routes = json.loads(descriptor.files[ROUTES_FILE])
    assert len(routes) == 2
    by_id = {r["transformers"][0]: r for r in routes}
    assert set(by_id) == {lua[0]["uniqueId"], jx[0]["uniqueId"]}


def test_identical_push_feeds_across_mappings_fail_compilation():
    """Two mappings subscribing to the same broker+topic compile to
    byte-identical consumer URIs (Camel refuses the second consumer) — the
    overlap guard fails loudly and names both mappings + the fan-in remedy."""
    with pytest.raises(ValueError, match="source-overlap"):
        _compile(
            _mapping("LuaMap", AIMC_LUA_SID,
                     "function aimc_main(sources) return { x = sources.StationState } end"),
            _mapping("FastMap", JSONATA_SID, '{"x": station_state}'),
        )
