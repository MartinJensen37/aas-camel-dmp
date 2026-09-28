"""Tests for the shared-model validation gate (ADR-016).

The management node parses AID/AIMC through the same pydantic models the
registration service authors them with (``aas_model``). A generator/model
change must be forced to break the management-node parse — this is the
lockstep guarantee the gate provides.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import management_node.parsers.validate as v
from management_node.parsers import aid, aimc
from management_node.parsers.validate import validate_aimc, validate_aid

from aas_model.submodel_templates import Aimc

HERE = Path(__file__).parent
DATA = HERE / "data"


def test_valid_aimc_validates_to_shared_model():
    submodel = json.loads((DATA / "syntegon_stoppering_aimc.json").read_text())
    shared = validate_aimc(submodel)
    assert shared is not None
    # the AIMC is the GENERATED IDTA template used directly (decision
    # 2026-09-15 — no DMP subclass remains); Aimc is an alias onto it
    assert isinstance(shared, Aimc)
    # parse_aimc returns the same pydantic model (no dataclass layer)
    model = aimc.parse_aimc(submodel)
    assert isinstance(model, Aimc)
    assert len(model.MappingConfigurations.value) == 5


def test_valid_aid_validates_to_shared_model():
    submodel = json.loads((DATA / "syntegon_stoppering_aid.json").read_text())
    shared = validate_aid(submodel)
    assert shared is not None
    assert type(shared).__name__ == "DmpAssetInterfacesDescription"
    model = aid.parse_aid(submodel)
    assert model.InterfaceTemplateForMQTT["interface_mqtt"] is not None


def test_strict_mode_raises_on_invalid_submodel(monkeypatch):
    monkeypatch.setenv("DMP_MGMT_STRICT_MODELS", "1")
    (v._STRICT if hasattr(v, "_STRICT") else None)
    # Corrupt: claimed Submodel but the shared model cannot parse the body.
    bad = {"modelType": "Submodel", "submodelElements": []}
    with pytest.raises(ValueError, match="shared-model validation"):
        validate_aimc(bad)
    with pytest.raises(ValueError, match="shared-model validation"):
        validate_aid(bad)


def test_lenient_mode_falls_back(monkeypatch):
    monkeypatch.setenv("DMP_MGMT_STRICT_MODELS", "0")
    # reload the module so the env is picked up
    import importlib
    importlib.reload(v)
    bad = {"modelType": "Submodel", "submodelElements": []}
    assert validate_aimc(bad) is None
    assert validate_aid(bad) is None
