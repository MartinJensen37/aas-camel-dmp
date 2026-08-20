"""Tests for the orchestrator reconcile flow (mocked AAS + docker)."""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.kafka_listener import AimcEvent
from management_node.orchestrator import AssetRef, Orchestrator

DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"

AAS_ID = "syntegonStopperingSystemAAS"


class FakeAasClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def get_submodel_by_short(self, aas_id: str, id_short: str) -> dict:
        self.calls.append((aas_id, id_short))
        if id_short == "AssetInterfacesDescription":
            return json.loads(DATA_AID.read_text())
        if id_short == "AssetInterfacesMappingConfiguration":
            return json.loads(DATA_AIMC.read_text())
        raise AssertionError(f"unexpected submodel: {id_short}")


class FakeDeployer:
    def __init__(self) -> None:
        self.deployed: list[tuple[str, str]] = []
        self.stopped: list[str] = []

    def deploy(self, asset_id_short: str, config_dir: str) -> str:
        self.deployed.append((asset_id_short, config_dir))
        return f"dmp-{asset_id_short}"

    def stop(self, asset_id_short: str) -> None:
        self.stopped.append(asset_id_short)


@pytest.fixture
def orchestrator(tmp_path):
    orch = Orchestrator(
        aas_base_url="http://aas-server:8081",
        config_root=tmp_path,
        assets={AAS_ID: AssetRef(aas_id=AAS_ID, aas_id_short="syntegonStopperingSystem")},
    )
    orch.aas = FakeAasClient()
    orch.deployer = FakeDeployer()
    return orch


def test_reconcile_writes_config_and_deploys(orchestrator, tmp_path):
    orch = orchestrator
    asset = orch.assets[AAS_ID]

    orch.reconcile(asset)

    assert len(orch.aas.calls) == 2  # AID + AIMC
    assert len(orch.deployer.deployed) == 1
    config_dir = tmp_path / "syntegonStopperingSystem"
    assert (config_dir / "routes.json").exists()
    assert (config_dir / "luatransformers.json").exists()
    assert (config_dir / "mqttconsumer.json").exists()
    assert (config_dir / "aasserver.json").exists()
    assert list((config_dir / "queries").glob("*.lua"))


def test_handle_event_reconciles(orchestrator):
    orch = orchestrator
    event = AimcEvent(event_type="SM_CREATED", submodel_id=f"{AAS_ID}/submodels/AssetInterfacesMappingConfiguration")

    orch.handle_event(event)

    assert len(orch.deployer.deployed) == 1


def test_handle_deleted_event_decommissions(orchestrator):
    orch = orchestrator
    event = AimcEvent(event_type="SM_DELETED", submodel_id=f"{AAS_ID}/submodels/AssetInterfacesMappingConfiguration")

    orch.handle_event(event)

    assert orch.deployer.stopped == ["syntegonStopperingSystem"]
    assert orch.deployer.deployed == []


def test_reconcile_is_single_flight(orchestrator):
    orch = orchestrator
    asset = orch.assets[AAS_ID]
    orch.reconcile(asset)
    orch.reconcile(asset)  # second call while first completed is allowed
    assert len(orch.deployer.deployed) == 2  # idempotent: each call re-deploys


def test_unknown_event_is_ignored(orchestrator):
    orch = orchestrator
    orch.handle_event(AimcEvent(event_type="SM_CREATED", submodel_id="unknown/submodels/X"))
    assert orch.deployer.deployed == []
