"""AID parser — returns the shared ``aas_model.DmpAssetInterfacesDescription``.

The parser dataclasses (``AidModel``/``AidInterface``/``AidDataPoint``/``AidForm``
/``MqttForm``) that previously lived here have been **removed** (ADR-016): one
model source of truth is the ``aas_model`` package, shared with the registration
service. This module is now a thin entry that validates/parses the BaSyx REST
AID JSON into the shared model; the resolver navigates it directly (including
the generic ``InterfaceTemplateFor*`` dicts for OPC UA / Modbus / HTTP).
"""

from __future__ import annotations

import json
import logging
from typing import Any

from aas_model.submodel_templates.aid import DmpAssetInterfacesDescription

from .validate import validate_aid

logger = logging.getLogger(__name__)


def parse_aid(submodel: dict[str, Any] | str) -> DmpAssetInterfacesDescription:
    """Parse an AID submodel into the shared ``DmpAssetInterfacesDescription``.

    Accepts either the raw submodel dict (as returned by the AAS REST API or in
    a resource config) or its JSON string representation. Validates through the
    shared model (strict by default — a generator/model change breaks the parse,
    ADR-016)."""
    if isinstance(submodel, str):
        submodel = json.loads(submodel)
    return validate_aid(submodel)
