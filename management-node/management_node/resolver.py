"""Reference resolution — links AIMC source/sink references to AID endpoints.

This is deliberately a SEPARATE step from the parsers: :mod:`parsers.aid` and
:mod:`parsers.aimc` return the shared pydantic models (``aas_model``, ADR-016);
only here do the two models meet. For every AIMC source the resolver finds the
AID interface and data point it points at and produces the concrete protocol
endpoint the DMP needs (base, topic/href, MQTT options, polling, delivery).

The ``Resolved*`` dataclasses are DERIVED state (protocol, endpoint, delivery,
fine ``value_path``) — they are not a re-implementation of the model; the
pydantic models are the source of truth.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from aas_model.submodel_templates import Aimc
from aas_model.submodel_templates.mqtt_aid import MqttAssetInterfacesDescription

logger = logging.getLogger(__name__)

#: AID dict fields holding generic (non-MQTT/REST) interface templates.
_GENERIC_INTERFACE_FIELDS = (
    "InterfaceTemplateForMQTT", "InterfaceTemplateForOPCUA", "InterfaceTemplateForHTTP",
    "InterfaceTemplateForMODBUS", "InterfaceTemplateForBacnet",
    "InterfaceTemplateForIOLINK_OVER_PROFINET_REST",
)


@dataclass
class ResolvedSource:
    """A concrete endpoint a DMP can consume, derived from AID + AIMC."""

    source_id: str
    protocol: str  # e.g. "mqtt", "http", "aas", "opcua", "modbus"
    interface: str  # AID interface name, e.g. "interface_mqtt" ("" for AAS-element)
    base: str | None = None  # endpoint base from AID EndpointMetadata
    topic: str | None = None  # href for MQTT (or path for other protocols)
    synchronous: bool | None = None
    polling_interval: float | None = None
    data_point: str | None = None  # AID data point name
    value_path: str | None = None  # fine resolution (ADR-014 / §6.5): JSON Pointer
    # OPC UA: absolute node path in the address space (uav_browsePath).
    node: str | None = None
    # Modbus binding.
    modbus_entity: str | None = None
    modbus_function: str | None = None
    # An AAS-element source (case 2, AAS->AAS): the submodel reference path
    # used to build the polling-consumer endpoint. protocol == "aas".
    ref_path: list[str] = field(default_factory=list)
    # How the source delivers data, derived from the AID (protocol-agnostic):
    # "async" = push / subscription (event); "sync" = request/response (polled).
    delivery: str = "sync"


@dataclass
class ResolvedSink:
    """A concrete endpoint a DMP can write to, derived from AID + AIMC."""

    sink_id: str
    aas: bool = False
    protocol: str = ""
    interface: str = ""
    base: str | None = None
    topic: str | None = None
    data_point: str | None = None
    value_path: str | None = None


@dataclass
class ResolvedMapping:
    """An AIMC mapping with its sources resolved against the AID."""

    id_short: str
    sources: list[ResolvedSource] = field(default_factory=list)
    sink_refs: list[Any] = field(default_factory=list)  # pydantic ModelReference
    sink_ids: list[str] = field(default_factory=list)
    resolved_sinks: list[ResolvedSink] = field(default_factory=list)
    transformation: str | None = None
    transformation_engine: str = "LUA"  # ADR-015 / SPEC §7.6
    default_polling_interval: float | None = None

    @property
    def unresolved(self) -> list[str]:
        return [s.source_id for s in self.sources if not s.protocol]


@dataclass
class ResolvedBidirectionalMapping:
    """A resolved operation-delegation (request/response) mapping."""

    id_short: str
    rest_base: str | None = None
    rest_path: str | None = None
    rest_method: str | None = None
    mqtt_base: str | None = None
    mqtt_command_topic: str | None = None
    mqtt_response_topic: str | None = None
    request_blob: str | None = None
    response_blob: str | None = None
    unresolved: list[str] = field(default_factory=list)


@dataclass
class ResolutionResult:
    """All mappings of an asset, with sources resolved against its AID."""

    asset_id_short: str
    mappings: list[ResolvedMapping] = field(default_factory=list)
    bidirectional: list[ResolvedBidirectionalMapping] = field(default_factory=list)

    @property
    def all_resolved(self) -> bool:
        return all(not m.unresolved for m in self.mappings) and \
            all(not m.unresolved for m in self.bidirectional)


# --------------------------------------------------------------------------- #
# Reference helpers (pydantic ModelReference)
# --------------------------------------------------------------------------- #
def reference_values(ref: Any) -> list[str]:
    """The key value path of a pydantic ``ModelReference`` (``ref.key[].value``)."""
    if ref is None:
        return []
    return [k.value for k in getattr(ref, "key", None) or ()]


# --------------------------------------------------------------------------- #
# AID navigation (pydantic)
# --------------------------------------------------------------------------- #
def _protocol_from_interface(interface: str) -> str:
    return interface.removeprefix("interface_").split("_")[0]


def _get_interface(aid: MqttAssetInterfacesDescription, name: str):
    """Resolve an interface by name (typed ``interface_*`` field or a generic
    ``InterfaceTemplateFor*`` dict entry)."""
    iface = getattr(aid, name, None)
    if iface is not None:
        return iface
    for dict_field in _GENERIC_INTERFACE_FIELDS:
        d = getattr(aid, dict_field, None) or {}
        if name in d:
            return d[name]
    return None


def _all_interfaces(aid: MqttAssetInterfacesDescription):
    """Yield (interface_name, interface) for every interface on the AID."""
    seen = set()
    for field_name in ("interface_mqtt", "interface_rest"):
        iface = getattr(aid, field_name, None)
        if iface is not None:
            yield field_name, iface
            seen.add(field_name)
    for dict_field in _GENERIC_INTERFACE_FIELDS:
        for name, iface in (getattr(aid, dict_field, None) or {}).items():
            if name not in seen:
                yield name, iface


def _iface_base(iface) -> str | None:
    em = getattr(iface, "EndpointMetadata", None)
    base = getattr(em, "base", None)
    return base.value if base is not None else None


def _iface_groups(iface) -> tuple[dict, dict, dict]:
    im = getattr(iface, "InteractionMetadata", None) or {}
    props = getattr(getattr(im, "properties", None), "property_name", None) or {}
    acts = getattr(getattr(im, "actions", None), "property_name", None) or {}
    evts = getattr(getattr(im, "events", None), "property_name", None) or {}
    return props, acts, evts


def _form_prop(dp, field: str) -> str | None:
    """The string value of a form/field property, leniently (may be absent)."""
    f = getattr(dp, "forms", None)
    if f is None:
        f = dp
    p = getattr(f, field, None)
    return p.value if p is not None else None


def _dp_observable(dp) -> bool | None:
    obs = getattr(dp, "observable", None)
    if obs is None:
        return None
    return str(obs.value).lower() in ("true", "1")


#: Sub-protocols / operation ids that declare a push (event) delivery mechanism.
_EVENT_OP_MARKERS = ("observe", "subscribe", "unobserve", "subscribeevent")
_EVENT_SUBPROTOCOLS = ("sse", "websub", "longpoll", "monitored-item", "monitoreditem",
                       "subscription", "push")


def _classify_delivery(protocol: str, dp, in_events: bool) -> str:
    """Classifies how a source delivers its data, protocol-agnostically (SPEC §6.1)."""
    if in_events:
        return "async"
    if protocol in ("mqtt", "kafka"):
        return "async"
    if protocol in ("modbus", "plc4x", "http", "aas", "timer"):
        return "sync"
    op = _form_prop(dp, "op") or ""
    if op and any(m in op.lower() for m in _EVENT_OP_MARKERS):
        return "async"
    sub = _form_prop(dp, "subprotocol") or ""
    if sub and sub.lower() in _EVENT_SUBPROTOCOLS:
        return "async"
    if _dp_observable(dp) is True:
        return "async"
    return "sync"


def _locate_data_point(props, acts, evts, keys: list[str]) -> tuple[str | None, list[str], bool]:
    """Locate the addressed data point + nested path; returns (name, nested, in_events)."""
    for i in range(len(keys) - 1, -1, -1):
        k = keys[i]
        if k in props or k in acts:
            return k, keys[i + 1:], False
        if k in evts:
            return k, keys[i + 1:], True
    return None, [], False


def _nested_pointer(nested: list[str]) -> str | None:
    if not nested:
        return None
    return "$." + ".".join(nested)


def _is_aas_element_reference(keys: list[str]) -> bool:
    if not keys:
        return False
    return "submodel" in keys[0].lower()


def _resolve_source(source, aid: MqttAssetInterfacesDescription) -> ResolvedSource:
    keys = reference_values(source.Source.value)
    interface_name = next((k for k in keys if k.startswith("interface_")), None)

    resolved = ResolvedSource(
        source_id=source.SourceId.value,
        protocol="",
        interface=interface_name or "",
        polling_interval=_float(source.PollingInterval),
    )

    if not interface_name:
        if _is_aas_element_reference(keys):
            resolved.protocol = "aas"
            resolved.ref_path = list(keys)
            resolved.synchronous = True
            resolved.delivery = "sync"
            return resolved
        logger.warning("source %s: no interface_* key in reference %s",
                       resolved.source_id, keys)
        return resolved

    interface = _get_interface(aid, interface_name)
    if interface is None:
        logger.warning("source %s: AID interface %r not found",
                       resolved.source_id, interface_name)
        return resolved

    resolved.base = _iface_base(interface)
    resolved.protocol = _protocol_from_interface(interface_name)

    props, acts, evts = _iface_groups(interface)
    data_point, nested, in_events = _locate_data_point(props, acts, evts, keys)
    if data_point is None:
        logger.warning("source %s: no AID data point found in %s",
                       resolved.source_id, keys)
        return resolved
    resolved.data_point = data_point
    resolved.value_path = _nested_pointer(nested)

    dp = props.get(data_point) or acts.get(data_point) or evts.get(data_point)
    if dp is not None:
        resolved.topic = _form_prop(dp, "href")
        resolved.modbus_entity = _form_prop(dp, "modv_entity")
        resolved.modbus_function = _form_prop(dp, "modv_function")
        resolved.node = _form_prop(dp, "uav_browsePath") or resolved.topic
        if resolved.node in (None, resolved.topic):
            node = getattr(dp, "node", None)
            if node:
                resolved.node = node.value
    resolved.delivery = _classify_delivery(resolved.protocol, dp, in_events)

    return resolved


def _float(prop) -> float | None:
    if prop is None or prop.value in (None, ""):
        return None
    try:
        return float(prop.value)
    except (TypeError, ValueError):
        return None


def _resolve_sink(sink, aid: MqttAssetInterfacesDescription) -> ResolvedSink:
    keys = reference_values(sink.Sink.value)
    interface_name = next((k for k in keys if k.startswith("interface_")), None)
    if interface_name is None:
        return ResolvedSink(sink_id=sink.SinkId.value, aas=True)

    resolved = ResolvedSink(sink_id=sink.SinkId.value, interface=interface_name)
    interface = _get_interface(aid, interface_name)
    if interface is None:
        logger.warning("sink %s: AID interface %r not found", resolved.sink_id, interface_name)
        return resolved

    resolved.base = _iface_base(interface)
    resolved.protocol = _protocol_from_interface(interface_name)

    props, acts, evts = _iface_groups(interface)
    data_point, nested, _ = _locate_data_point(props, acts, evts, keys)
    if data_point is None:
        logger.warning("sink %s: no AID data point found in %s", resolved.sink_id, keys)
        return resolved
    resolved.data_point = data_point
    resolved.value_path = _nested_pointer(nested)
    dp = props.get(data_point) or acts.get(data_point) or evts.get(data_point)
    if dp is not None:
        resolved.topic = _form_prop(dp, "href")

    return resolved


def resolve(aimc: Aimc, aid: MqttAssetInterfacesDescription) -> ResolutionResult:
    """Resolve every mapping's sources against the asset's AID."""
    result = ResolutionResult(asset_id_short=aimc.id_short or "")
    for mc in aimc.MappingConfigurations.value:
        resolved_sources = [_resolve_source(s, aid) for s in mc.Sources.value]
        sink_refs = [s.Sink.value for s in mc.Sinks.value]
        resolved_sinks = [_resolve_sink(s, aid) for s in mc.Sinks.value]
        result.mappings.append(
            ResolvedMapping(
                id_short=_mapping_name(mc, resolved_sources),
                sources=resolved_sources,
                sink_refs=sink_refs,
                sink_ids=[s.SinkId.value for s in mc.Sinks.value],
                resolved_sinks=resolved_sinks,
                transformation=_transformation_text(mc),
                transformation_engine=transformation_engine(mc),
                default_polling_interval=_float(mc.DefaultPollingInterval),
            )
        )
    result.bidirectional = _pair_bidirectional(aimc, aid)
    return result


def _mapping_name(mc, sources: list[ResolvedSource]) -> str:
    """The mapping's name: the SMC ``id_short`` when meaningful, else the first
    SourceId (raw MappingConfigurations carry only placeholder id_shorts in the
    serialized AAS)."""
    name = getattr(mc, "id_short", None) or ""
    if name and name not in ("SubmodelElementCollection", "MappingConfigurations", "mapping"):
        return name
    if sources and sources[0].source_id:
        return sources[0].source_id
    return "mapping"


def _transformation_text(mc) -> str | None:
    if getattr(mc, "Transformation", None) is None:
        return None
    v = mc.Transformation.value
    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8")
        except UnicodeDecodeError:
            v = v.decode("utf-8", errors="replace")
    else:
        v = str(v)
    # aas_model's default Transformation blob value is the placeholder "None";
    # treat it (and empty) as "no transformation" (passthrough, R3).
    if not v or v.strip() in ("None", "none", "null", "b''"):
        return None
    return v


def transformation_engine(mc) -> str:
    """Engine selected by the Transformation blob's semantic id (ADR-015 / §7.6)."""
    tr = getattr(mc, "Transformation", None)
    if tr is None:
        return "LUA"
    sids = [getattr(tr, "semantic_id", None) or ""]
    sids += list(getattr(tr, "supplemental_semantic_ids", None) or [])
    for sid in sids:
        if "jsonata" in str(sid).lower():
            return "JSONATA"
    return "LUA"


# --------------------------------------------------------------------------- #
# Operation-delegation (bidirectional) pairing
# --------------------------------------------------------------------------- #
def _pair_bidirectional(aimc: Aimc, aid: MqttAssetInterfacesDescription):
    requests: dict[str, Any] = {}
    responses: dict[str, Any] = {}
    for mc in aimc.MappingConfigurations.value:
        if not mc.Sources.value or not mc.Sinks.value:
            continue
        src_path = reference_values(mc.Sources.value[0].Source.value)
        snk_path = reference_values(mc.Sinks.value[0].Sink.value)
        action = src_path[-1] if src_path else getattr(mc, "id_short", None) or ""
        if "interface_rest" in src_path and "interface_mqtt" in snk_path:
            requests[action] = mc
        elif "interface_mqtt" in src_path and "interface_rest" in snk_path:
            responses[action] = mc

    result = []
    for action, req in requests.items():
        result.append(_resolve_bidirectional_pair(action, req, responses.get(action), aid))
    return result


def _resolve_bidirectional_pair(action, req, resp, aid) -> ResolvedBidirectionalMapping:
    resolved = ResolvedBidirectionalMapping(id_short=action)

    rest_interface = _get_interface(aid, "interface_rest")
    mqtt_interface = _get_interface(aid, "interface_mqtt")

    _, rest_acts, _ = _iface_groups(rest_interface) if rest_interface else ({}, {}, {})
    _, mqtt_acts, _ = _iface_groups(mqtt_interface) if mqtt_interface else ({}, {}, {})
    rest_action = rest_acts.get(action) if rest_acts else None
    mqtt_action = mqtt_acts.get(action) if mqtt_acts else None

    if rest_interface is None or rest_action is None or not _iface_base(rest_interface):
        resolved.unresolved.append(f"{action}: REST action not found in AID")
    else:
        resolved.rest_base = _iface_base(rest_interface)
        resolved.rest_path = _form_prop(rest_action, "href")
        method = _form_prop(rest_action, "htv_methodName")
        resolved.rest_method = method or "POST"

    if mqtt_interface is None or mqtt_action is None or not _iface_base(mqtt_interface):
        resolved.unresolved.append(f"{action}: MQTT action not found in AID")
    else:
        resolved.mqtt_base = _iface_base(mqtt_interface)
        href = _form_prop(mqtt_action, "href")
        if href:
            _, _, base_path = _split_mqtt_path(resolved.mqtt_base)
            resolved.mqtt_command_topic = (base_path + href).lstrip("/")
            response_href = None
            resp_form = getattr(mqtt_action, "forms", None)
            if resp_form is not None and resp_form.response is not None:
                rh = resp_form.response.href.value if resp_form.response.href is not None else None
                if isinstance(rh, str) and rh.startswith("/"):
                    response_href = rh
            if response_href is None:
                response_href = href.replace("/CMD/", "/DATA/")
            resolved.mqtt_response_topic = (base_path + response_href).lstrip("/")
        if not resolved.mqtt_command_topic:
            resolved.unresolved.append(f"{action}: no MQTT command topic")

    resolved.request_blob = _transformation_text(req)
    if resp is not None:
        resolved.response_blob = _transformation_text(resp)
    if not resolved.request_blob or not resolved.response_blob:
        resolved.unresolved.append(f"{action}: missing request/response blob")

    return resolved


def _split_mqtt_path(base: str) -> tuple[str | None, int | None, str]:
    """``mqtt://host:port/path`` -> (host, port, path)."""
    import re
    if not base:
        return None, None, ""
    m = re.match(r"^(?:mqtts?|tcp)://([^:/]+):?(\d*)(.*)$", base)
    if not m:
        return None, None, base
    return m.group(1), int(m.group(2)) if m.group(2) else None, m.group(3) or ""
