"""AIMC parser — returns the shared ``aas_model.Aimc`` pydantic model.

The parser dataclasses (``AimcModel``/``AimcMapping``/``AimcSource``/...) that
previously lived here have been **removed** (ADR-016): one model source of truth
is the ``aas_model`` package, shared with the registration service. This module
is now a thin entry that validates/parses the BaSyx REST AIMC JSON into the
shared ``Aimc`` model; the resolver navigates it directly.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from aas_model.submodel_templates import Aimc

from .validate import validate_aimc

logger = logging.getLogger(__name__)

#: Semantic id of the AIMC 2.0 submodel (event discriminator).
AIMC_SEMANTIC_ID = (
    "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel"
)

#: Transformation engines selectable by the blob's semantic id (ADR-015 / SPEC §7.6).
ENGINE_LUA = "LUA"
ENGINE_JSONATA = "JSONATA"


def transformation_engine(semantic_ids: list[str] | None) -> str:
    """Selects the transformation engine from the blob's semantic ids.

    A semantic id containing ``jsonata`` selects the fast/simple JSONata engine;
    anything else (including the AIMC 2.0 default Transformation semantic id)
    selects the AIMC-compliant Lua engine."""
    for sid in semantic_ids or []:
        if "jsonata" in sid.lower():
            return ENGINE_JSONATA
    return ENGINE_LUA


def parse_aimc(submodel: dict[str, Any] | str) -> Aimc:
    """Parse an AIMC submodel into the shared ``Aimc`` pydantic model.

    Accepts either the raw submodel dict (as returned by the AAS REST API or in
    a resource config) or its JSON string representation. Validates through the
    shared model (strict by default — a generator/model change breaks the parse,
    ADR-016)."""
    if isinstance(submodel, str):
        submodel = json.loads(submodel)
    return validate_aimc(submodel)
