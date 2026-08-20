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
    """Supplemental semantic ids from both the flat (aas_pydantic) and the raw
    BaSyx REST form (``supplementalSemanticIds``, values under ``keys[0]``)."""
    if not isinstance(raw, dict):
        return []
    ids = []
    for key in ("supplemental_semantic_ids", "supplementalSemanticIds"):
        for item in raw.get(key) or []:
            if isinstance(item, dict):
                v = item.get("value")
                if isinstance(v, dict):
                    v = None
                if not v and item.get("keys"):
                    v = (item["keys"][0] or {}).get("value")
                if v:
                    ids.append(str(v))
            elif item:
                ids.append(str(item))
    return ids


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
    response = None
    if isinstance(response_raw, dict):
        inner = response_raw.get("value")
        if isinstance(inner, list) and inner and isinstance(inner[0], dict) and "idShort" in inner[0]:
            # raw BaSyx REST: the response is a Property whose value is the
            # nested form property list
            response = _parse_form({p.get("idShort"): p for p in inner if isinstance(p, dict) and p.get("idShort")})
        else:
            response = _parse_form(response_raw)
    elif isinstance(response_raw, list) and response_raw:
        # raw BaSyx REST: the response form is itself a property list
        response = _parse_form({p.get("idShort"): p for p in response_raw if isinstance(p, dict) and p.get("idShort")})

    return AidForm(
        href=str(href),
        op=str(_unwrap_value(raw.get("op")) or "") or None,
        content_type=str(_unwrap_value(raw.get("contentType")) or "") or None,
        method=str(_unwrap_value(raw.get("htv_methodName")) or "") or None,
        mqtt=mqtt,
        response=response,
    )


def _parse_forms(raw: Any) -> list[AidForm]:
    """``forms`` may be a single dict, an SML list of dicts, or — in the raw
    AAS REST serialization — an SMC whose ``value`` is a flat list of
    Properties (``{idShort, value}``). All forms are accepted."""
    if raw is None:
        return []

    def _from_property_list(items: list) -> list[AidForm]:
        """Raw AAS form: [Property, ...] -> one form dict -> AidForm."""
        form = {p.get("idShort"): p for p in items if isinstance(p, dict) and p.get("idShort")}
        f = _parse_form(form)
        return [f] if f is not None else []

    if isinstance(raw, list):
        if raw and all(isinstance(i, dict) and "idShort" in i and "href" not in i for i in raw):
            return _from_property_list(raw)
        return [f for item in raw if (f := _parse_form(item)) is not None]
    if isinstance(raw, dict):
        # unwrap a possible {"value": {...}} / {"value": [...]} wrapper
        inner = raw.get("value", raw)
        if isinstance(inner, list):
            if inner and all(isinstance(i, dict) and "idShort" in i and "href" not in i for i in inner):
                return _from_property_list(inner)
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
# Raw AAS REST normalization
# --------------------------------------------------------------------------- #
def _flatten_smc(raw: Any) -> dict:
    """A raw AAS SMC ({value:[children]}) -> {idShort: child} plus the outer
    semantic keys (semanticId/supplementalSemanticIds live on the SMC itself,
    not among the value children); else unchanged."""
    if isinstance(raw, dict) and isinstance(raw.get("value"), list):
        flat = {
            c.get("idShort"): c
            for c in raw["value"]
            if isinstance(c, dict) and c.get("idShort")
        }
        for key in ("semanticId", "supplementalSemanticIds"):
            if key in raw:
                flat[key] = raw[key]
        return flat
    return raw


def _unwrap_sml(raw: Any) -> list:
    """SML -> plain list (accepts a list or ``{"value":[...]}``)."""
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for key in ("value", "submodelElements"):
            if isinstance(raw.get(key), list):
                return raw[key]
    return []


def _normalize_aid(submodel: dict[str, Any]) -> dict[str, Any]:
    """Normalize the AID as delivered by BaSyx REST.

    BaSyx serializes the AID with ``submodelElements``: each interface
    (``interface_mqtt``/``interface_rest``) is an SMC whose children
    (``EndpointMetadata``, ``InteractionMetadata`` -> ``actions``/``properties``)
    live under ``value``. Flatten them into the internal form ``parse_aid``
    consumes.
    """
    normalized: dict[str, Any] = {}
    if not isinstance(submodel, dict):
        return normalized
    normalized.update(submodel)

    for elem in submodel.get("submodelElements", []):
        name = elem.get("idShort")
        if not name or not name.startswith("interface_") or elem.get("modelType") != "SubmodelElementCollection":
            continue

        flat_iface = _flatten_smc(elem)  # {title, EndpointMetadata, InteractionMetadata}
        endpoint = _flatten_smc(flat_iface.get("EndpointMetadata"))
        interaction_raw = _flatten_smc(flat_iface.get("InteractionMetadata"))

        interaction: dict[str, Any] = {}
        for group in ("properties", "actions"):
            entries: dict[str, Any] = {}
            for entry in _unwrap_sml(interaction_raw.get(group)):
                if not isinstance(entry, dict):
                    continue
                flat_entry = _flatten_smc(entry)  # {key, title, forms, synchronous, ...}
                key = flat_entry.get("key")
                if isinstance(key, dict):
                    key = key.get("value")
                key = str(key) if key else entry.get("idShort")
                if key:
                    entries[key] = flat_entry
            interaction[group] = entries

        normalized[name] = {"EndpointMetadata": endpoint, "InteractionMetadata": interaction}

    return normalized


# --------------------------------------------------------------------------- #
# Parser entry point
# --------------------------------------------------------------------------- #
def parse_aid(submodel: dict[str, Any] | str) -> AidModel:
    """Parse an AID submodel into an :class:`AidModel`.

    Interfaces are the top-level keys whose value is a dict carrying
    ``EndpointMetadata`` and/or ``InteractionMetadata``. Both the raw AAS REST
    form (``submodelElements``) and the flat config-dict form are accepted.
    """
    if isinstance(submodel, str):
        submodel = json.loads(submodel)

    submodel = _normalize_aid(submodel)

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
