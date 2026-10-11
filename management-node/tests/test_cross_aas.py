"""Tests for reference-anchored cross-asset AID resolution (ADR-019) and
AID-event dependency invalidation (T3.1/T3.2), with a mocked registry.

References are self-contained: their first key anchors at the exact submodel
instance they address, so no ownership tracking exists — only a parsed-AID
cache keyed by that anchor id, with a semanticId validation gate."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from management_node.kafka_listener import AimcEvent, AimcEventListener
from management_node.submodel_locator import AID_SEMANTIC_ID, AidDirectory

DATA_AID = Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"


class FakeRegistryClient:
    """Minimal GET /submodels/{id} facade serving complete AID submodels."""

    def __init__(self) -> None:
        self.fetches: list[str] = []
        real_aid = json.loads(DATA_AID.read_text())
        # one device asset whose AID instance is addressable by its own id
        self.submodels: dict[str, dict] = {
            "device/submodels/AID": {**real_aid, "id": "device/submodels/AID"},
            "device/submodels/NotAnAid": {
                "modelType": "Submodel",
                "id": "device/submodels/NotAnAid",
                "semanticId": {"keys": [{"type": "GlobalReference",
                                         "value": "https://example.org/other/Template"}]},
            },
        }

    def get_submodel(self, submodel_id: str) -> dict:
        self.fetches.append(submodel_id)
        return self.submodels[submodel_id]


def test_aid_directory_resolves_the_anchor_instance_itself():
    """The reference anchor IS the defining AID instance — one fetch, no
    ownership scan."""
    directory = AidDirectory(FakeRegistryClient())
    model = directory.get("device/submodels/AID")
    assert model is not None
    # cached: repeated resolution hits the cache, not the REST layer again
    directory.get("device/submodels/AID")
    assert len(directory._client.fetches) == 1


def test_aid_directory_rejects_non_aid_semantics_without_guessing():
    """Nothing in a reference proves the anchored submodel follows the AID
    template; a mismatched semanticId is a loud skip, not a shape-based guess."""
    directory = AidDirectory(FakeRegistryClient())
    assert directory.get("device/submodels/NotAnAid") is None


def test_aid_directory_handles_missing_instances_gracefully():
    class BrokenClient(FakeRegistryClient):
        def get_submodel(self, submodel_id):
            raise LookupError(submodel_id)

    directory = AidDirectory(BrokenClient())
    assert directory.get("does/not/exist") is None


def test_invalidate_forces_refetch_after_change_events():
    client = FakeRegistryClient()
    directory = AidDirectory(client)
    directory.get("device/submodels/AID")
    directory.invalidate("device/submodels/AID")
    directory.get("device/submodels/AID")
    assert len(client.fetches) == 2


# --------------------------------------------------------------------------- #
# Resolver integration: the reference anchor governs which AID instance
# defines the interface (remote-first through the directory, local fallback)
# --------------------------------------------------------------------------- #
from aas_model.testing import (
    aimc_mapping, aimc_sink, aimc_source, build_aid, build_aimc,
    mqtt_interface,
)

from management_node.resolver import resolve

REMOTE_BASE = "mqtt://192.168.0.110:1883/NN/Device"
DEVICE_AID_ANCHOR = "device/submodels/AID"


def _local_aid():
    # the mapping's own AID: structurally has interface_mqtt but defines no
    # data points — the remote instance must win for the DeviceState source
    return build_aid()


def _remote_aid():
    return build_aid(mqtt=mqtt_interface(REMOTE_BASE,
                                         properties={"DeviceState": "/DATA/State"}))


def _aimc():
    from aas_model.testing import aimc_ref
    ref = aimc_ref(DEVICE_AID_ANCHOR, "interface_mqtt", "InteractionMetadata",
                   "properties", "DeviceState")
    mc = aimc_mapping(
        id_short="CrossKpi",
        sources=[aimc_source("DeviceState", ref)],
        sinks=[],
    )
    return build_aimc(mc)


def test_resolver_resolves_via_referenced_aid_instance():
    resolution = resolve(_aimc(), _local_aid(),
                         aid_for_anchor=lambda anchor: _remote_aid()
                         if anchor == DEVICE_AID_ANCHOR else None)
    src = next(m for m in resolution.mappings).sources[0]
    assert src.base == REMOTE_BASE
    assert src.topic == "/DATA/State"
    assert src.delivery == "async"


def test_unresolvable_anchor_stays_unresolved():
    resolution = resolve(_aimc(), _local_aid())  # no directory wired
    src = next(m for m in resolution.mappings).sources[0]
    assert src.protocol == "" and src.base is None


def test_repository_lookup_returns_actual_nested_element_type():
    from aas_pydantic import Key, ModelReference
    from management_node.submodel_locator import referenced_element_type

    class Client:
        def get_submodel(self, _submodel_id):
            return {"submodelElements": [{
                "idShort": "State", "modelType": "SubmodelElementCollection",
                "value": [{"idShort": "Temperature", "modelType": "Property"}],
            }]}

    ref = ModelReference(key=(
        Key(type_="Submodel", value="unit/submodels/Runtime"),
        Key(type_="SubmodelElementCollection", value="State"),
        Key(type_="SubmodelElementCollection", value="Temperature"),
    ))
    assert referenced_element_type(Client(), ref) == "Property"


def test_repository_lookup_rejects_missing_nested_element():
    from aas_pydantic import Key, ModelReference
    from management_node.submodel_locator import referenced_element_type

    class Client:
        def get_submodel(self, _submodel_id):
            return {"submodelElements": []}

    ref = ModelReference(key=(
        Key(type_="Submodel", value="unit/submodels/Runtime"),
        Key(type_="Property", value="Missing"),
    ))
    with pytest.raises(ValueError, match="not found"):
        referenced_element_type(Client(), ref)


def test_directory_failure_does_not_substitute_local_aid():
    """A remote anchor must not be replaced with a similarly named local feed."""
    resolution = resolve(_aimc(), build_aid(
        mqtt=mqtt_interface("mqtt://local:1883/BASE",
                            properties={"DeviceState": "/DATA/State"})),
        aid_for_anchor=lambda anchor: None)
    src = next(m for m in resolution.mappings).sources[0]
    assert src.protocol == "" and src.base is None


def test_remote_aid_missing_affordance_does_not_substitute_local_aid():
    resolution = resolve(_aimc(), build_aid(mqtt=mqtt_interface(
        "mqtt://local:1883/BASE", properties={"DeviceState": "/DATA/State"})),
        aid_for_anchor=lambda anchor: build_aid(mqtt=mqtt_interface(
            "mqtt://remote:1883/OTHER", properties={"OtherState": "/DATA/Other"})))
    src = resolution.mappings[0].sources[0]
    assert src.protocol == "" and src.base is None


def test_foreign_aid_sink_requires_its_own_resolvable_anchor():
    from aas_model.testing import aimc_ref

    ref = aimc_ref(DEVICE_AID_ANCHOR, "interface_mqtt", "InteractionMetadata",
                   "properties", "DeviceState")
    model = build_aimc(aimc_mapping(
        id_short="CrossSink",
        sources=[aimc_source("DeviceState", ref)],
        sinks=[aimc_sink("RemoteTarget", ref)],
    ))
    unresolved = resolve(model, _local_aid(), aid_for_anchor=lambda _: None)
    assert "sink:RemoteTarget" in unresolved.mappings[0].unresolved
    assert not unresolved.all_resolved

    resolved = resolve(model, _local_aid(), aid_for_anchor=lambda _: _remote_aid())
    assert resolved.all_resolved
    assert resolved.mappings[0].resolved_sinks[0].base == REMOTE_BASE


def test_two_device_references_compile_into_one_line_aas_sink():
    """An Example-3-style KPI needs both AIDs' endpoint metadata, but no
    lookup of either device's owning shell."""
    from aas_model.testing import aimc_ref
    from management_node.descriptor import compile_descriptor, ROUTES_FILE

    anchors = ("device-1/submodels/AID", "device-2/submodels/AID")
    aids = {
        anchor: build_aid(mqtt=mqtt_interface(
            f"mqtt://192.168.0.110:1883/NN/Device{i}",
            properties={"DeviceState": "/DATA/State"}))
        for i, anchor in enumerate(anchors, 1)
    }
    mc = aimc_mapping(
        id_short="LineKpi",
        sources=[aimc_source(f"Device{i}", aimc_ref(
            anchor, "interface_mqtt", "InteractionMetadata", "properties",
            "DeviceState")) for i, anchor in enumerate(anchors, 1)],
        sinks=[aimc_sink("Kpi", aimc_ref("line/submodels/KPI", "Kpi"))],
        transformation="function aimc_main(sources) return { Kpi = sources.Device1 } end",
    )
    resolution = resolve(build_aimc(mc), None, aid_for_anchor=aids.get)
    assert resolution.all_resolved
    descriptor = compile_descriptor(resolution, aas_base_url="http://aas-server:8081",
                                    aas_id="line/aas", aas_id_short="line")
    routes = json.loads(descriptor.files[ROUTES_FILE])
    assert len(routes) == 1
    assert len(routes[0]["datasources"]) == 2
    consumers = json.loads(descriptor.files["mqttdatasource.json"])
    assert {c["sourceId"] for c in consumers} == {"Device1", "Device2"}
    assert {c["topic"] for c in consumers} == {
        "NN/Device1/DATA/State", "NN/Device2/DATA/State"}


# --------------------------------------------------------------------------- #
# Dependency invalidation on AID events (T3.2), under ADR-021 attribution:
# the managed set is keyed by AIMC instance ids; AID change events reconcile
# the owning unit plus every unit whose compiled DMP references that AID.
# --------------------------------------------------------------------------- #
from management_node.orchestrator import ManagedAimc, Orchestrator

ORIG_AID_URL = ("https://smartproductionlab.aau.dk/aas/"
                "syntegonStopperingSystemAAS/submodels/AssetInterfacesDescription")
DEVICE_AIMC_ID = "device/submodels/AssetInterfacesMappingConfiguration"
DEVICE_AID_ID = "device/submodels/AID"
LINE_AIMC_ID = "line/submodels/AssetInterfacesMappingConfiguration"
LINE_AID_ID = "line/submodels/AID"


def _rehosted_aimc(anchor_prefix: str) -> dict:
    """The fixture AIMC re-anchored: its AID references point at this unit's
    own AID instance instead of the original absolute URL."""
    raw = json.loads(DATA_AIMC.read_text())
    raw["id"] = f"{anchor_prefix}/submodels/AssetInterfacesMappingConfiguration"
    text = json.dumps(raw).replace(ORIG_AID_URL, f"{anchor_prefix}/submodels/AID")
    return json.loads(text)


class FakeAasClient:
    """Registry serving two rehosted units, each with its own AID instance."""

    def __init__(self) -> None:
        self.aid = json.loads(DATA_AID.read_text())
        self.units = {
            DEVICE_AIMC_ID: _rehosted_aimc("device"),
            LINE_AIMC_ID: _rehosted_aimc("line"),
        }

    def get_submodel(self, submodel_id: str) -> dict:
        key = submodel_id.rstrip("/")
        if key in self.units:
            return self.units[key]
        if key in (DEVICE_AID_ID, LINE_AID_ID):
            return self.aid
        raise LookupError(submodel_id)

    def list_shells(self) -> list[dict]:
        shells = []
        for prefix, id_short in (("device", "device"), ("line", "line")):
            shells.append({
                "id": f"{prefix}/aas",
                "idShort": id_short,
                "submodels": [
                    {"keys": [{"type": "Submodel",
                               "value": f"{prefix}/submodels/AssetInterfacesMappingConfiguration"}]},
                    {"keys": [{"type": "Submodel", "value": f"{prefix}/submodels/AID"}]},
                ],
            })
        return shells


class FakeDeployer:
    def __init__(self) -> None:
        self.deployed: list[tuple[str, str]] = []
        self.stopped: list[str] = []

    def deploy(self, unit_name: str, config_dir: str, *,
               container_name: str | None = None) -> str:
        self.deployed.append((unit_name, config_dir, container_name))
        return f"dmp-{unit_name}"

    def stop(self, unit_name: str, *,
             container_name: str | None = None) -> None:
        self.stopped.append((unit_name, container_name))


def _two_unit_orchestrator(tmp_path) -> Orchestrator:
    orch = Orchestrator(
        aas_base_url="http://aas-server:8081",
        config_root=tmp_path,
        managed_aimcs={
            DEVICE_AIMC_ID: ManagedAimc(aimc_sm_id=DEVICE_AIMC_ID, label="device"),
            LINE_AIMC_ID: ManagedAimc(aimc_sm_id=LINE_AIMC_ID, label="line"),
        },
    )
    orch.aas = FakeAasClient()
    orch.aid_directory._client = orch.aas
    orch.shell_directory._client = orch.aas
    orch.deployer = FakeDeployer()
    return orch


def _deployed_prefixes(orch: Orchestrator) -> list[str]:
    return [name.split("-", 2)[1] for name, _, _ in orch.deployer.deployed]


def test_aid_event_reconciles_recorded_dependents(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)

    # compile both units once: this records their referenced submodel ids
    orch.reconcile(orch.managed[DEVICE_AIMC_ID])
    orch.reconcile(orch.managed[LINE_AIMC_ID])
    assert len(orch.deployer.deployed) == 2

    # an AID change on the device asset invalidates the device DMP only here
    orch.handle_event(AimcEvent(
        event_type="SM_UPDATED",
        submodel_id=DEVICE_AID_ID,
        semantic_id=AID_SEMANTIC_ID,
    ))
    orch.flush()

    redeployed = _deployed_prefixes(orch)[2:]
    assert redeployed == ["device"]


def test_invalid_explicit_local_aid_pin_does_not_deploy_discovered_aid(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    pinned = ManagedAimc(aimc_sm_id=DEVICE_AIMC_ID, label="device",
                         local_aid_id="device/submodels/nonexistent")
    orch.reconcile(pinned)
    assert orch.deployer.deployed == []


def test_deleted_pinned_aid_stops_existing_deployment(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    unit = ManagedAimc(aimc_sm_id=DEVICE_AIMC_ID, label="device",
                       local_aid_id=DEVICE_AID_ID)
    orch.managed[DEVICE_AIMC_ID] = unit
    orch.reconcile(unit)
    assert len(orch.deployer.deployed) == 1

    original_get = orch.aas.get_submodel

    def without_aid(submodel_id):
        if submodel_id == DEVICE_AID_ID:
            raise LookupError(submodel_id)
        return original_get(submodel_id)

    orch.aas.get_submodel = without_aid
    orch.handle_event(AimcEvent("SM_DELETED", DEVICE_AID_ID))
    orch.flush()
    assert len(orch.deployer.stopped) == 1
    assert len(orch.deployer.deployed) == 1


def test_aid_event_for_unmanaged_submodel_is_ignored(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    orch.reconcile(orch.managed[DEVICE_AIMC_ID])
    before = len(orch.deployer.deployed)

    orch.handle_event(AimcEvent(
        event_type="SM_UPDATED",
        submodel_id="elsewhere/submodels/AID",
        semantic_id=AID_SEMANTIC_ID,
    ))
    orch.flush()

    assert len(orch.deployer.deployed) == before


def test_listener_delivers_remote_aid_update_to_referenced_unit(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    orch.aas.units[LINE_AIMC_ID] = json.loads(json.dumps(
        orch.aas.units[LINE_AIMC_ID]).replace(LINE_AID_ID, DEVICE_AID_ID))
    orch.reconcile(orch.managed[LINE_AIMC_ID])
    assert _deployed_prefixes(orch) == ["line"]

    # Exercise the production listener filter, not handle_event directly.
    listener = object.__new__(AimcEventListener)
    listener._handler = orch.handle_event
    listener._handle("submodel-events", json.dumps({
        "type": "SM_UPDATED", "id": DEVICE_AID_ID,
        "submodel": {"semanticId": {"keys": [{"type": "GlobalReference",
                                               "value": AID_SEMANTIC_ID}]}},
    }).encode(), None)
    orch.flush()
    assert _deployed_prefixes(orch) == ["line", "line"]


def test_startup_indexes_remote_references_without_redeploy(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    orch.aas.units[LINE_AIMC_ID] = json.loads(json.dumps(
        orch.aas.units[LINE_AIMC_ID]).replace(LINE_AID_ID, DEVICE_AID_ID))
    orch.index_managed_references()
    assert orch.deployer.deployed == []

    orch.handle_event(AimcEvent("SM_UPDATED", DEVICE_AID_ID,
                                semantic_id=AID_SEMANTIC_ID))
    orch.flush()
    assert sorted(_deployed_prefixes(orch)) == ["device", "line"]


def test_changed_references_replace_old_reverse_dependencies(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    unit = orch.managed[LINE_AIMC_ID]
    orch.reconcile(unit)
    orch.aas.units[LINE_AIMC_ID] = json.loads(json.dumps(
        orch.aas.units[LINE_AIMC_ID]).replace(LINE_AID_ID, DEVICE_AID_ID))
    orch.reconcile(unit)
    assert len(orch.deployer.deployed) == 2

    orch.handle_event(AimcEvent("SM_UPDATED", LINE_AID_ID,
                                semantic_id=AID_SEMANTIC_ID))
    orch.flush()
    assert len(orch.deployer.deployed) == 2
    orch.handle_event(AimcEvent("SM_UPDATED", DEVICE_AID_ID,
                                semantic_id=AID_SEMANTIC_ID))
    orch.flush()
    assert len(orch.deployer.deployed) == 3


def test_deleted_aimc_does_not_redeploy_on_later_aid_update(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    orch.reconcile(orch.managed[LINE_AIMC_ID])
    orch.handle_event(AimcEvent("SM_DELETED", LINE_AIMC_ID))
    orch.handle_event(AimcEvent("SM_UPDATED", LINE_AID_ID,
                                semantic_id=AID_SEMANTIC_ID))
    orch.flush()
    assert len(orch.deployer.deployed) == 1
    assert len(orch.deployer.stopped) == 1


def test_bodyless_aid_delete_invalidates_cache_and_stops_stale_dmp(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    unit = orch.managed[DEVICE_AIMC_ID]
    orch.reconcile(unit)
    assert orch.aid_directory.get(DEVICE_AID_ID) is not None

    original_get = orch.aas.get_submodel

    def without_aid(submodel_id):
        if submodel_id == DEVICE_AID_ID:
            raise LookupError(submodel_id)
        return original_get(submodel_id)

    orch.aas.get_submodel = without_aid
    listener = object.__new__(AimcEventListener)
    listener._handler = orch.handle_event
    listener._handle("submodel-events", json.dumps({
        "type": "SM_DELETED", "id": DEVICE_AID_ID,
    }).encode(), None)
    orch.flush()
    assert orch.aid_directory.get(DEVICE_AID_ID) is None
    assert len(orch.deployer.deployed) == 1
    assert len(orch.deployer.stopped) == 1

    orch.aas.get_submodel = original_get
    orch.handle_event(AimcEvent("SM_CREATED", DEVICE_AID_ID,
                                semantic_id=AID_SEMANTIC_ID))
    orch.flush()
    assert len(orch.deployer.deployed) == 2


def test_foreign_aid_references_deploy_without_local_aid_guess(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    unit = orch.managed[LINE_AIMC_ID]
    raw = orch.aas.units[LINE_AIMC_ID]
    template = raw["submodelElements"][0]["value"][0]
    raw["submodelElements"][0]["value"] = []
    remote_ids = [f"remote-{i}/submodels/AID" for i in range(3)]
    for index, aid_id in enumerate(remote_ids):
        mapping = json.loads(json.dumps(template).replace(LINE_AID_ID, aid_id))
        mapping["idShort"] = f"RemoteMapping{index}"
        raw["submodelElements"][0]["value"].append(mapping)
    original_get = orch.aas.get_submodel

    def get_remote(submodel_id):
        if submodel_id in remote_ids:
            index = remote_ids.index(submodel_id)
            return json.loads(json.dumps(orch.aas.aid).replace(
                "NN/Nybrovej/InnoLab/Stoppering",
                f"NN/Nybrovej/InnoLab/Remote{index}"))
        return original_get(submodel_id)

    orch.aas.get_submodel = get_remote
    orch.reconcile(unit)
    assert _deployed_prefixes(orch) == ["line"]
    assert LINE_AIMC_ID not in orch._local_aids
    assert all(LINE_AIMC_ID in orch._dependents[aid_id] for aid_id in remote_ids)


def test_response_only_references_are_indexed_for_dependency_invalidation(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    source_id = "remote-source/submodels/AID"
    sink_id = "remote-sink/submodels/AID"

    def ref(value):
        return SimpleNamespace(key=[SimpleNamespace(value=value)])

    mapping = SimpleNamespace(
        Sources=SimpleNamespace(value=[]),
        Sinks=SimpleNamespace(value=[]),
        ResponseSources=SimpleNamespace(value=[SimpleNamespace(Source=SimpleNamespace(
            value=ref(source_id)))]),
        ResponseSinks=SimpleNamespace(value=[SimpleNamespace(Sink=SimpleNamespace(
            value=ref(sink_id)))])
    )
    aimc = SimpleNamespace(MappingConfigurations=SimpleNamespace(value=[mapping]))
    unit = orch.managed[LINE_AIMC_ID]
    orch._record_dependencies(unit, aimc)

    assert orch._dependents[source_id] == {LINE_AIMC_ID}
    assert orch._dependents[sink_id] == {LINE_AIMC_ID}

    invalidated = []
    submitted = []
    orch.aid_directory.invalidate = invalidated.append
    orch.submit = lambda unit: submitted.append(unit.aimc_sm_id)
    orch.handle_event(AimcEvent("SM_UPDATED", source_id, semantic_id=AID_SEMANTIC_ID))
    orch.handle_event(AimcEvent("SM_DELETED", sink_id))
    assert invalidated == [source_id, sink_id]
    assert submitted == [LINE_AIMC_ID, LINE_AIMC_ID]


def test_index_reports_incomplete_when_the_repository_is_not_reachable(tmp_path):
    """A node that starts before aas-env must know its index is incomplete,
    rather than silently running with no cross-AAS dependency view."""
    orch = _two_unit_orchestrator(tmp_path)
    orch.aas.units.pop(DEVICE_AIMC_ID)

    assert orch.index_managed_references() is False


def test_index_reports_complete_when_every_unit_is_fetchable(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    assert orch.index_managed_references() is True


def test_reindex_until_complete_converges_once_the_repository_appears(tmp_path):
    """The background retry is what makes a cold start converge: the unit
    missing at first appears later and the index is completed."""
    orch = _two_unit_orchestrator(tmp_path)
    # Both units depend on the device AID, so an AID change invalidates both —
    # but only the device AIMC is missing from the repository at first.
    orch.aas.units[LINE_AIMC_ID] = json.loads(json.dumps(
        orch.aas.units[LINE_AIMC_ID]).replace(LINE_AID_ID, DEVICE_AID_ID))
    hidden = json.loads(json.dumps(orch.aas.units[DEVICE_AIMC_ID]))
    del orch.aas.units[DEVICE_AIMC_ID]

    assert orch.index_managed_references() is False

    orch.aas.units[DEVICE_AIMC_ID] = hidden
    thread = orch.reindex_until_complete(
        interval_s=0, attempts=3, sleep=lambda _s: None)
    thread.join(timeout=5)

    assert not thread.is_alive()
    # the retry indexed it, so a remote AID change now invalidates the unit
    orch.handle_event(AimcEvent("SM_UPDATED", DEVICE_AID_ID,
                                semantic_id=AID_SEMANTIC_ID))
    orch.flush()
    assert sorted(_deployed_prefixes(orch)) == ["device", "line"]


def test_reindex_stops_when_asked(tmp_path):
    orch = _two_unit_orchestrator(tmp_path)
    del orch.aas.units[DEVICE_AIMC_ID]
    orch.index_managed_references()

    thread = orch.reindex_until_complete(
        interval_s=0, attempts=1000, should_stop=lambda: True, sleep=lambda _s: None)
    thread.join(timeout=5)
    assert not thread.is_alive()
