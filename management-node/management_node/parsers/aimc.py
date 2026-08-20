"""Dedicated AIMC parser — Asset Interfaces Mapping Configuration (IDTA 2/0).

Self-contained: parses an AIMC Submodel (AAS REST API JSON or config dict)
into typed dataclasses. This module has NO knowledge of AID or any other
submodel; resolving the ``Source``/``Sink`` references against AID endpoints
is a separate step (``management_node.resolver``).
"""

from __future__ import annotations

import base64
import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

#: Semantic id of the AIMC 2.0 submodel (event discriminator).
AIMC_SEMANTIC_ID = (
    "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel"
)


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class AimcReferenceKey:
    """One key of a ModelReference (AAS serialized as ``{"type":…, "value":…}``)."""

    type: str
    value: str


@dataclass(frozen=True)
class AimcReference:
    """A reference to a SubmodelElement (source or sink)."""

    type: str
    keys: list[AimcReferenceKey] = field(default_factory=list)

    def as_path(self) -> list[str]:
        """The reference key values (the dotted path into the AAS)."""
        return [k.value for k in self.keys]

    def last_key_value(self) -> str | None:
        return self.keys[-1].value if self.keys else None


@dataclass
class AimcSource:
    """One entry of ``MappingConfiguration.Sources``."""

    id_short: str
    source_id: str
    source: AimcReference
    polling_interval: float | None = None


@dataclass
class AimcSink:
    """One entry of ``MappingConfiguration.Sinks``."""

    id_short: str
    sink_id: str
    sink: AimcReference


@dataclass
class AimcMapping:
    """One logical mapping (sources -> transformation -> sinks).

    A skill's operation delegation is bidirectional and expressed as TWO of
    these directional mappings (one per direction, identified by the
    Source/Sink references): request = REST action -> MQTT action,
    response = MQTT action -> REST action.
    """

    id_short: str
    sources: list[AimcSource] = field(default_factory=list)
    sinks: list[AimcSink] = field(default_factory=list)
    transformation: str | None = None
    default_polling_interval: float | None = None


@dataclass
class AimcModel:
    """Parsed AIMC submodel."""

    id_short: str
    mappings: list[AimcMapping] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# AAS serialization helpers (lenient across serializers)
# --------------------------------------------------------------------------- #
def _get_name(obj: dict) -> str:
    return str(obj.get("id_short") or obj.get("idShort") or obj.get("id") or "")


def _unwrap_sml(raw: Any) -> list:
    """A SubmodelElementList serializes as a list, or ``{"value":[...]}`` /
    ``{"submodelElements":[...]}``. Returns a plain list."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("value", "submodelElements", "submodel_elements"):
            v = raw.get(key)
            if isinstance(v, list):
                return v
    return []


def _unwrap_property(raw: Any) -> Any:
    """A Property serializes as ``{"value": …}``; return the raw value."""
    if isinstance(raw, dict) and "value" in raw:
        return raw["value"]
    return raw


def _blob_text(raw: Any) -> str | None:
    """A Blob's ``value`` may be raw text (aas_pydantic) or base64 (AAS REST).

    Lua source is not valid base64, so a successful base64 decode is a safe
    way to recover the text payload."""
    v = _unwrap_property(raw)
    if v is None:
        return None
    text = v if isinstance(v, str) else str(v)
    try:
        decoded = base64.b64decode(text, validate=True).decode("utf-8")
        if "\n" in decoded or "function" in decoded:
            return decoded
    except Exception:
        pass
    return text


def _to_float(raw: Any) -> float | None:
    v = _unwrap_property(raw)
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        logger.warning("non-numeric polling interval: %r", v)
        return None


def _parse_reference(raw: Any) -> AimcReference:
    if isinstance(raw, dict) and "value" in raw and isinstance(raw["value"], dict):
        raw = raw["value"]
    if not isinstance(raw, dict):
        return AimcReference(type="ModelReference", keys=[])
    keys = []
    for k in raw.get("keys") or []:
        if isinstance(k, dict):
            keys.append(AimcReferenceKey(type=str(k.get("type", "")), value=str(k.get("value", ""))))
    return AimcReference(type=str(raw.get("type", "ModelReference")), keys=keys)


def _parse_sources(raw: Any) -> list[AimcSource]:
    sources = []
    for item in _unwrap_sml(raw):
        if not isinstance(item, dict):
            continue
        sources.append(
            AimcSource(
                id_short=_get_name(item),
                source_id=str(_unwrap_property(item.get("SourceId")) or _get_name(item)),
                source=_parse_reference(item.get("Source")),
                polling_interval=_to_float(item.get("PollingInterval")),
            )
        )
    return sources


def _parse_sinks(raw: Any) -> list[AimcSink]:
    sinks = []
    for item in _unwrap_sml(raw):
        if not isinstance(item, dict):
            continue
        sinks.append(
            AimcSink(
                id_short=_get_name(item),
                sink_id=str(_unwrap_property(item.get("SinkId")) or _get_name(item)),
                sink=_parse_reference(item.get("Sink")),
            )
        )
    return sinks


# --------------------------------------------------------------------------- #
# Raw AAS REST normalization
# --------------------------------------------------------------------------- #
def _flatten_smc(raw: Any) -> dict:
    """An SMC serializes its children under ``value``; promote them to keys."""
    if not isinstance(raw, dict):
        return {}
    flat = dict(raw)
    children = flat.pop("value", None)
    if isinstance(children, list):
        for child in children:
            if isinstance(child, dict) and child.get("idShort"):
                flat[child["idShort"]] = child
    return flat


def _normalize_aimc(submodel: dict[str, Any]) -> dict[str, Any]:
    """Normalize the AIMC as delivered by BaSyx REST.

    BaSyx serializes the AIMC with ``submodelElements``: ``MappingConfigurations``
    is a SubmodelElementList of SMCs whose children live under ``value``.
    Flatten them into the internal form the parser consumes.
    """
    normalized: dict[str, Any] = {"MappingConfigurations": []}
    if not isinstance(submodel, dict):
        return normalized
    normalized.update(submodel)

    for elem in submodel.get("submodelElements", []):
        if elem.get("idShort") == "MappingConfigurations":
            normalized["MappingConfigurations"] = _unwrap_sml(elem)

    mcs = []
    for mc in normalized["MappingConfigurations"]:
        flat_mc = _flatten_smc(mc)
        for key in ("Sources", "Sinks"):
            flat_mc[key] = [_flatten_smc(item) for item in _unwrap_sml(flat_mc.get(key))]
        mcs.append(flat_mc)
    normalized["MappingConfigurations"] = mcs
    return normalized


# --------------------------------------------------------------------------- #
# Parser entry point
# --------------------------------------------------------------------------- #
def parse_aimc(submodel: dict[str, Any] | str) -> AimcModel:
    """Parse an AIMC submodel into an :class:`AimcModel`.

    Accepts either the raw submodel dict (as returned by the AAS REST API or
    present in a resource config) or its JSON string representation.
    """
    if isinstance(submodel, str):
        submodel = json.loads(submodel)

    submodel = _normalize_aimc(submodel)

    mappings = []
    for mc in _unwrap_sml(submodel.get("MappingConfigurations")):
        if not isinstance(mc, dict):
            continue
        mappings.append(
            AimcMapping(
                id_short=_mapping_name(mc),
                sources=_parse_sources(mc.get("Sources")),
                sinks=_parse_sinks(mc.get("Sinks")),
                transformation=_blob_text(mc.get("Transformation")),
                default_polling_interval=_to_float(mc.get("DefaultPollingInterval")),
            )
        )

    return AimcModel(
        id_short=_get_name(submodel),
        mappings=mappings,
    )


def _mapping_name(mc: dict) -> str:
    """Raw AAS mapping configs (SMCs) carry no idShort; fall back to the
    first SourceId so transformer/blob names stay meaningful."""
    name = _get_name(mc)
    if name:
        return name
    for src in _unwrap_sml(mc.get("Sources")):
        if isinstance(src, dict):
            source_id = _unwrap_property(src.get("SourceId"))
            if source_id:
                return str(source_id)
    return "mapping"
