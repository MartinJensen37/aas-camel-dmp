"""Shared-model validation gate (ADR-016).

The management node parses AID/AIMC through the *same* pydantic models the
registration service authors them with (``aas_model``). This module reverse-
parses each incoming BaSyx AAS submodel into the shared pydantic model and
exposes the result, so a change to the model schema is forced to break the
management node's parse — the lockstep guarantee.

The lenient dict-walk parser (``parsers.aid`` / ``parsers.aimc``) still builds
the lightweight resolver dataclasses; the shared model is the validation gate
and the source of truth for the shape. Strictness can be disabled at runtime
(``DMP_MGMT_STRICT_MODELS=0``) to fall back to the lenient parse with a logged
warning if a submodel ever fails shared-model validation.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from aas_model import _serde
from aas_model.submodel_templates import Aimc
from aas_model.submodel_templates.aid import DmpAssetInterfacesDescription

logger = logging.getLogger(__name__)

#: Controls whether shared-model validation is fatal (default) or a logged
#: warning that falls back to the lenient parser.
_STRICT = os.environ.get("DMP_MGMT_STRICT_MODELS", "1").strip().lower() not in ("0", "false", "no")


def _is_basyx_submodel(submodel: Any) -> bool:
    return isinstance(submodel, dict) and (
        submodel.get("modelType") == "Submodel" or "submodelElements" in submodel
    )


def validate_aimc(submodel: dict[str, Any]):
    """Reverse-parse an AIMC submodel dict into the shared ``Aimc`` model."""
    if not _is_basyx_submodel(submodel):
        return None
    try:
        return _serde.parse_submodel_dict(submodel, Aimc)
    except Exception as e:  # noqa: BLE001 - surface a clear parser/model mismatch
        _handle_failure("AIMC", submodel, e)
    return None


def validate_aid(submodel: dict[str, Any]):
    """Reverse-parse an AID submodel dict into the shared ``DmpAssetInterfacesDescription``."""
    if not _is_basyx_submodel(submodel):
        return None
    try:
        return _serde.parse_submodel_dict(submodel, DmpAssetInterfacesDescription)
    except Exception as e:  # noqa: BLE001
        _handle_failure("AID", submodel, e)
    return None


def _handle_failure(kind: str, submodel: Any, error: Exception) -> None:
    """Strict → raise (lockstep: model change breaks the parser). Lenient →
    log and continue with the fallback parser (never silently drop)."""
    if _STRICT:
        raise ValueError(
            f"{kind} submodel failed shared-model validation (ADR-016). The AAS "
            f"model and the management-node parser are out of sync — update the "
            f"shared 'aas_model' package. Underlying error: {error}"
        ) from error
    logger.warning(
        "%s submodel failed shared-model validation (ADR-016); falling back to "
        "lenient parse: %s", kind, error
    )
