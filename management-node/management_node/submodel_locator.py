"""Reference-anchored cross-asset AID resolution (ADR-019).

References inside an AAS are self-contained: their first key anchors at the
precise submodel instance they address (e.g.
``[Submodel=device/submodels/AID, …, "interface_mqtt", …]``). There is no
ownership question — no registry scan is needed; to resolve an ``interface_*``
name defined on ANOTHER asset's AID we simply fetch exactly that instance,
verify it really is an AssetInterfacesDescription (**by semanticId** — the AID
template allows any idShort), parse it once and cache it keyed by its id.
When a directory is wired, the resolver uses only this anchored instance;
an unavailable or mismatched anchor cannot be replaced by the mapping's AID.
The separate owning-shell lookup below supplies only the managed AIMC's local
shell identity for naming/macros/operation context, not cross-asset resolution.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

from .aas_client import AasClient
from .resolver import reference_keys

logger = logging.getLogger(__name__)


def referenced_element_type(client: AasClient, reference: object) -> str | None:
    """Return the repository-stored type at a typed submodel element path."""
    keys = reference_keys(reference)
    if len(keys) < 2 or keys[0].type_ != "Submodel":
        return None
    try:
        raw = client.get_submodel(keys[0].value)
    except Exception as exc:  # unavailable repository: retain typed-reference fallback
        logger.warning("referenced submodel %s could not be verified: %s",
                       keys[0].value, exc)
        return None
    elements = raw.get("submodelElements") or []
    found = None
    for key in keys[1:]:
        collection = elements if isinstance(elements, list) else []
        found = next((element for element in collection
                      if element.get("idShort") == key.value), None)
        if found is None:
            raise ValueError(f"referenced element {key.value!r} not found in {keys[0].value}")
        value = found.get("modelType")
        actual_type = value.get("name") if isinstance(value, dict) else value
        if actual_type and actual_type != key.type_:
            return str(actual_type)
        elements = found.get("value") or []
        if isinstance(elements, dict):
            elements = list(elements.values())
    if found is None:
        return None
    value = found.get("modelType")
    actual_type = value.get("name") if isinstance(value, dict) else value
    return str(actual_type) if actual_type else None

#: Semantic id of the AssetInterfacesDescription submodel (IDTA AID).
#: The idShort may be chosen freely by the authoring side — matching happens
#: on this semantic id only.
AID_SEMANTIC_ID = (
    "https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel"
)

DEFAULT_TTL_S = 60.0


def _submodel_semantic(submodel: dict) -> str | None:
    sem = submodel.get("semanticId")
    if isinstance(sem, dict):
        keys = sem.get("keys") or []
        if keys:
            return str(keys[0].get("value", ""))
    return None


class AidDirectory:
    """Parsed-AID cache keyed by the referenced submodel instance itself."""

    def __init__(self, client: AasClient, *, ttl_s: float = DEFAULT_TTL_S) -> None:
        self._client = client
        self._ttl = ttl_s
        # anchor submodel id -> parsed AID model
        self._cache: dict[str, tuple[Any | None, float]] = {}

    def get(self, submodel_id: str) -> Any | None:
        """The parsed AID model denoted by ``submodel_id``, or ``None`` when
        it does not exist or is not an AssetInterfacesDescription (by
        semanticId — never guessed). Failures cache too, briefly."""
        anchor = submodel_id.rstrip("/")
        hit = self._cache.get(anchor)
        now = time.monotonic()
        if hit and now - hit[1] < self._ttl:
            return hit[0]

        model = self._load_uncached(anchor)
        self._cache[anchor] = (model, now)
        return model

    def _load_uncached(self, anchor: str) -> Any | None:
        try:
            raw = self._client.get_submodel(anchor)
        except Exception as e:  # noqa: BLE001 - REST errors mean "not available"
            logger.warning("referenced AID %s not fetchable: %s", anchor, e)
            return None
        if _submodel_semantic(raw) != AID_SEMANTIC_ID:
            # an authoring error: do not guess semantics from the payload shape
            logger.warning(
                "submodel %s is referenced as an AID but declares semanticId %r "
                "(expected %s); skipping", anchor, _submodel_semantic(raw),
                AID_SEMANTIC_ID)
            return None
        from .parsers.aid import parse_aid

        model = parse_aid(raw)
        if model is None:
            logger.warning("submodel %s carries the AID semantic id but did not "
                           "parse into the shared model", anchor)
        return model

    def invalidate(self, submodel_id: str | None = None) -> None:
        """Drop cached entries (after a change event touching them)."""
        if submodel_id is None:
            self._cache.clear()
            return
        self._cache.pop(submodel_id.rstrip("/"), None)


@dataclass
class OwningShell:
    """The AAS that hosts a submodel instance (registry-derived, never
    configured): supplies the ``{aas_id}``/``{aas_id_short}`` context."""
    id: str
    id_short: str


class OwningShellDirectory:
    """Reverse lookup submodel instance -> owning AAS shell.

    One registry scan (``GET /shells``) is cached and scanned locally — a
    handful of shells covers a lab; the TTL keeps it honest. This replaces
    every static aas_id/aas_id_short configuration: ownership is DERIVED.
    """

    def __init__(self, client: AasClient, *, ttl_s: float = DEFAULT_TTL_S) -> None:
        self._client = client
        self._ttl = ttl_s
        self._scan: tuple[float, dict[str, OwningShell]] | None = None

    def owner_of(self, submodel_id: str) -> OwningShell | None:
        target = submodel_id.rstrip("/")
        scan = self._scanned()
        return scan.get(target)

    def invalidate(self) -> None:
        self._scan = None

    def _scanned(self) -> dict[str, OwningShell]:
        now = time.monotonic()
        if self._scan is not None and now - self._scan[0] < self._ttl:
            return self._scan[1]
        index: dict[str, OwningShell] = {}
        try:
            for shell in self._client.list_shells():
                shell_id = str(shell.get("id", "")).rstrip("/")
                if not shell_id:
                    continue
                for ref in shell.get("submodels") or []:
                    # AAS 2.0: submodel references are a list of References
                    keys = (ref.get("keys") if isinstance(ref, dict) else None) or []
                    if not keys:
                        continue
                    sm_id = str(keys[0].get("value", "")).rstrip("/")
                    if sm_id and sm_id not in index:
                        index[sm_id] = OwningShell(
                            id=shell_id,
                            id_short=str(shell.get("idShort", "")) or shell_id)
        except Exception as e:  # noqa: BLE001 - registry hiccups degrade softly
            logger.warning("owning-shell scan failed: %s", e)
        self._scan = (now, index)
        return index
