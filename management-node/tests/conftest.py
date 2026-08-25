"""Expose the shared ``aas_model`` package (ADR-016) for local test runs.

``aas-model`` is a sibling git repository. Until it is published and
pip-installable, add its ``src`` layout to the path so
``from aas_model...`` resolves here too.
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO = os.path.abspath(os.path.join(_HERE, ".."))
_AAS_MODEL_SRC = os.path.abspath(os.path.join(_REPO, "..", "..", "aas-model", "src"))
_RS_AAS_PYDANTIC = os.path.abspath(
    os.path.join(_REPO, "..", "Registration_Service", "third_party", "aas_pydantic")
)

for p in (_AAS_MODEL_SRC, _RS_AAS_PYDANTIC):
    if p not in sys.path and os.path.isdir(p):
        sys.path.insert(0, p)
