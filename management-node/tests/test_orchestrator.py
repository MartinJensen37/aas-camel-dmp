"""Tests for the orchestrator reconcile flow (mocked AAS + docker).

The managed set is keyed by the AIMC submodel INSTANCE id (ADR-021): events
attribute exactly, deletes decommission exactly that unit, and shell context
(AAS id/idShort) is derived from the registry — never configured statically."""

from __future__ import annotations

import json
import pathlib

import pytest

from management_node.kafka_listener import AimcEvent
from management_node.orchestrator import ManagedAimc, Orchestrator

DATA_AID = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aid.json"
DATA_AIMC = pathlib.Path(__file__).parent / "data" / "syntegon_stoppering_aimc.json"

AIMC_SM_ID = ("https://smartproductionlab.aau.dk/aas/"
              "syntegonStopperingSystemAAS/submodels/AssetInterfacesMappingConfiguration")
AID_SM_ID = ("https://smartproductionlab.aau.dk/aas/"
             "syntegonStopperingSystemAAS/submodels/AssetInterfacesDescription")
SHELL_ID = "https://smartproductionlab.aau.dk/aas/syntegonStopperingSystemAAS"


class FakeAasClient:
    """Serves the fixture AIMC/AID instances plus the owning-shell scan."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.aimc = json.loads(DATA_AIMC.read_text())
        self.aid = json.loads(DATA_AID.read_text())

    def get_submodel(self, submodel_id: str) -> dict:
        self.calls.append(submodel_id)
        if submodel_id.rstrip("/") == AIMC_SM_ID:
            return self.aimc
        if submodel_id.rstrip("/") == AID_SM_ID:
            return self.aid
        raise LookupError(submodel_id)

    def list_shells(self) -> list[dict]:
        return [{
            "id": SHELL_ID,
            "idShort": "syntegonStopperingSystem",
            "submodels": [
                {"keys": [{"type": "Submodel", "value": AIMC_SM_ID}]},
                {"keys": [{"type": "Submodel", "value": AID_SM_ID}]},
            ],
        }]


class FakeDeployer:
    def __init__(self) -> None:
        self.deployed: list[tuple[str, str]] = []
        self.stopped: list[str] = []

    def deploy(self, unit_name: str, config_dir: str, *,
               container_name: str | None = None) -> str:
        self.deployed.append((unit_name, config_dir, container_name))
        return f"container-{unit_name}"

    def stop(self, unit_name: str, *,
             container_name: str | None = None) -> None:
        self.stopped.append((unit_name, container_name))


@pytest.fixture
def orchestrator(tmp_path):
    orch = Orchestrator(
        aas_base_url="http://aas-server:8081",
        config_root=tmp_path,
        managed_aimcs={AIMC_SM_ID: ManagedAimc(aimc_sm_id=AIMC_SM_ID,
                                               label="syntegonStopperingSystem")},
    )
    orch.aas = FakeAasClient()
    orch.aid_directory._client = orch.aas
    orch.shell_directory._client = orch.aas
    orch.deployer = FakeDeployer()
    return orch


def _unit(orch: Orchestrator) -> ManagedAimc:
    return next(iter(orch.managed.values()))


def test_reconcile_writes_config_and_deploys(orchestrator, tmp_path):
    orch = orchestrator
    unit = _unit(orch)

    orch.reconcile(unit)

    assert len(orch.deployer.deployed) == 1
    deployed_name, config_dir, container_name = orch.deployer.deployed[0]
    # container/config identity derives from the AIMC instance id hash (ADR-021)
    assert deployed_name.startswith("dmp-syntegonStopperingSystem-")
    config_path = tmp_path / deployed_name
    assert (config_path / "routes.json").exists()
    assert (config_path / "luatransformer.json").exists()
    assert (config_path / "mqttconsumer.json").exists()
    assert (config_path / "aasdatasink.json").exists()
    assert list((config_path / "queries").glob("*.lua"))


def test_handle_event_reconciles_by_exact_instance_id(orchestrator):
    orch = orchestrator
    event = AimcEvent(event_type="SM_CREATED", submodel_id=AIMC_SM_ID)

    orch.handle_event(event)
    orch.flush()  # reconciles run off the Kafka thread (T1.6)

    assert len(orch.deployer.deployed) == 1


def test_rapid_events_coalesce_to_one_followup_reconcile(orchestrator):
    """A burst of updates for one unit must not serialize N full redeploys;
    pending requests coalesce (last-write-wins, T1.6)."""
    orch = orchestrator
    event = AimcEvent(event_type="SM_UPDATED", submodel_id=AIMC_SM_ID)
    for _ in range(10):
        orch.handle_event(event)
    orch.flush()
    # every drain pass reconciles once per coalesced batch — far fewer than 10
    assert 1 <= len(orch.deployer.deployed) <= 4


def test_delete_event_decommissions_exactly_the_matched_unit(orchestrator):
    orch = orchestrator
    unit = _unit(orch)
    event = AimcEvent(event_type="SM_DELETED", submodel_id=AIMC_SM_ID)

    orch.handle_event(event)

    # deletes decommission synchronously on the event path, keyed by the
    # matched unit only (multi-asset delete collisions are impossible)
    assert orch.deployer.stopped == [(unit.name, "dmp-syntegonstopperingsystem")]
    assert orch.deployer.deployed == []


def test_unmanaged_instance_event_is_ignored():
    orch = _standalone_orchestrator()
    orch.handle_event(AimcEvent(event_type="SM_CREATED",
                                submodel_id="other-asset/submodels/AIMC"))
    assert orch.deployer.deployed == []


def test_reconcile_is_idempotent_per_call(orchestrator):
    orch = orchestrator
    unit = _unit(orch)
    orch.reconcile(unit)
    orch.reconcile(unit)  # second synchronous call re-deploys (idempotent)
    assert len(orch.deployer.deployed) == 2


def _standalone_orchestrator() -> Orchestrator:
    import tempfile
    orch = Orchestrator(
        aas_base_url="http://aas-server:8081",
        config_root=tempfile.mkdtemp(),
        managed_aimcs={},
    )
    orch.aas = FakeAasClient()
    orch.aid_directory._client = orch.aas
    orch.shell_directory._client = orch.aas
    orch.deployer = FakeDeployer()
    return orch
