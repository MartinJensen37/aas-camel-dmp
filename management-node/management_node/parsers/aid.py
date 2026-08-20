"""Dedicated AID parser — Asset Interfaces Description.

Self-contained: parses an AID Submodel (AAS REST API JSON or config dict)
into typed dataclasses describing each interface's EndpointMetadata and
InteractionMetadata (data points and actions). This module has NO knowledge
of AIMC; linking AID data points to AIMC sources/sinks is a separate step
(``management_node.resolver``).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

#: Keys that are never interfaces (metadata of the submodel itself).
_NON_INTERFACE_KEYS = {
    "id_short", "idShort", "id", "semantic_id", "semanticId",
    "model_type", "modelType", "description", "kind", "administrative_information",
    "supplemental_semantic_ids", "embedded_data_specifications",
}


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #
@dataclass
class MqttForm:
    """MQTT-specific form options (taken verbatim from the AID spec)."""

    retain: bool | None = None
    qos: int | None = None
    control_packet: str | None = None  # "subscribe" | "publish"


@dataclass
class AidForm:
    """One form (protocol binding) of a data point / action."""

    href: str
    op: str | None = None
    content_type: str | None = None
    method: str | None = None  # htv_methodName (REST), e.g. "POST"
    mqtt: MqttForm | None = None
    response: "AidForm | None" = None


@dataclass
class AidDataPoint:
    """An addressable data point in InteractionMetadata (a property)."""

    name: str
    key: str | None = None
    forms: list[AidForm] = field(default_factory=list)
    semantic_ids: list[str] = field(default_factory=list)


@dataclass
class AidAction(AidDataPoint):
    """An interaction in InteractionMetadata (an action)."""

    synchronous: bool | None = None
    input_semantic_ids: list[str] = field(default_factory=list)
    output_semantic_ids: list[str] = field(default_factory=list)


@dataclass
class AidInterface:
    """One interface (e.g. ``interface_mqtt``) of the asset."""

    name: str
    base: str | None = None
    content_type: str | None = None
    properties: dict[str, AidDataPoint] = field(default_factory=dict)
    actions: dict[str, AidAction] = field(default_factory=dict)


@dataclass
class AidModel:
    """Parsed AID submodel."""

    id_short: str
    interfaces: dict[str, AidInterface] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# AAS serialization helpers
# --------------------------------------------------------------------------- #
def _get_name(obj: dict) -> str:
    return str(obj.get("id_short") or obj.get("idShort") or obj.get("id") or "")


def _unwrap_value(raw: Any) -> Any:
    if isinstance(raw, dict) and "value" in raw:
        return raw["value"]
    return raw


def _to_bool(raw: Any) -> bool | None:
    v = _unwrap_value(raw)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"


def _to_int(raw: Any) -> int | None:
    v = _unwrap_value(raw)
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _semantic_ids(raw: Any) -> list[str]:
    if not isinstance(raw, dict):
        return []
    ids = []
    for item in raw.get("supplemental_semantic_ids") or []:
        if isinstance(item, dict):
            ids.append(str(item.get("value", "")))
        else:
            ids.append(str(item))
    return [i for i in ids if i]


def _parse_form(raw: Any) -> AidForm | None:
    if not isinstance(raw, dict):
        return None
    href = _unwrap_value(raw.get("href"))
    if href is None:
        return None

    retain = _to_bool(raw.get("mqv_retain"))
    qos = _to_int(raw.get("mqv_qos"))
    control_packet = str(_unwrap_value(raw.get("mqv_control_packet")) or "") or None
    mqtt = None
    if retain is not None or qos is not None or control_packet:
        mqtt = MqttForm(retain=retain, qos=qos, control_packet=control_packet)

    response_raw = raw.get("response")
    response = _parse_form(response_raw) if isinstance(response_raw, dict) else None

    return AidForm(
        href=str(href),
        op=str(_unwrap_value(raw.get("op")) or "") or None,
        content_type=str(_unwrap_value(raw.get("contentType")) or "") or None,
        method=str(_unwrap_value(raw.get("htv_methodName")) or "") or None,
        mqtt=mqtt,
        response=response,
    )


def _parse_forms(raw: Any) -> list[AidForm]:
    """``forms`` may be a single dict or an SML list of dicts."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return [f for item in raw if (f := _parse_form(item)) is not None]
    if isinstance(raw, dict):
        # unwrap a possible {"value": {...}} / {"value": [...]} wrapper
        inner = raw.get("value", raw)
        if isinstance(inner, list):
            return [f for item in inner if (f := _parse_form(item)) is not None]
        f = _parse_form(inner)
        return [f] if f is not None else []
    return []


def _is_data_point(raw: Any) -> bool:
    """A data point is a dict that directly carries ``forms``."""
    return isinstance(raw, dict) and "forms" in raw


def _collect_entry(raw: Any, is_action: bool, name: str) -> AidDataPoint | AidAction | None:
    if not isinstance(raw, dict):
        return None
    semantic_ids = _semantic_ids(raw)
    if is_action:
        return AidAction(
            name=name,
            key=str(_unwrap_value(raw.get("key")) or "") or None,
            synchronous=_to_bool(raw.get("synchronous")),
            forms=_parse_forms(raw.get("forms")),
            semantic_ids=semantic_ids,
            input_semantic_ids=_semantic_ids(raw.get("input")),
            output_semantic_ids=_semantic_ids(raw.get("output")),
        )
    return AidDataPoint(
        name=name,
        key=str(_unwrap_value(raw.get("key")) or "") or None,
        forms=_parse_forms(raw.get("forms")),
        semantic_ids=semantic_ids,
    )


def _parse_group(raw: Any, is_action: bool) -> dict[str, Any]:
    """InteractionMetadata entries may be flat or nested under a grouping
    wrapper (e.g. ``property_name``). Recurse through wrappers."""
    result: dict[str, Any] = {}
    if not isinstance(raw, dict):
        return result
    for name, value in raw.items():
        if _is_data_point(value):
            entry = _collect_entry(value, is_action, name)
            if entry is not None:
                result[name] = entry
        elif isinstance(value, dict):
            result.update(_parse_group(value, is_action))
    return result


# --------------------------------------------------------------------------- #
# Parser entry point
# --------------------------------------------------------------------------- #
def parse_aid(submodel: dict[str, Any] | str) -> AidModel:
    """Parse an AID submodel into an :class:`AidModel`.

    Interfaces are the top-level keys whose value is a dict carrying
    ``EndpointMetadata`` and/or ``InteractionMetadata``.
    """
    if isinstance(submodel, str):
        submodel = json.loads(submodel)

    interfaces: dict[str, AidInterface] = {}
    for name, value in submodel.items():
        if name in _NON_INTERFACE_KEYS or not isinstance(value, dict):
            continue
        has_metadata = "EndpointMetadata" in value or "InteractionMetadata" in value
        if not has_metadata:
            continue

        endpoint = value.get("EndpointMetadata") or {}
        interaction = value.get("InteractionMetadata") or {}
        interfaces[name] = AidInterface(
            name=name,
            base=str(_unwrap_value(endpoint.get("base")) or "") or None,
            content_type=str(_unwrap_value(endpoint.get("contentType")) or "") or None,
            properties=_parse_group(interaction.get("properties"), is_action=False),
            actions=_parse_group(interaction.get("actions"), is_action=True),
        )

    return AidModel(id_short=_get_name(submodel), interfaces=interfaces)
