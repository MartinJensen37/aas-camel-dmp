"""Reference resolution — links AIMC source/sink references to AID endpoints.

This is deliberately a SEPARATE step from the parsers: :mod:`parsers.aid` and
:mod:`parsers.aimc` return the shared pydantic models (``aas_model``, ADR-016);
only here do the two models meet. For every AIMC source the resolver finds the
AID interface and data point it points at and produces the concrete protocol
endpoint the DMP needs (base, topic/href, MQTT options, polling, delivery).

ADR-020 (v2): everything below is DECLARED, never guessed —

- the PROTOCOL comes from the interface's supplemental TD semantic ids
  (e.g. ``http://www.w3.org/2011/mqtt``); the historical ``interface_<prot>``
  name prefix is only a documented fallback;
- DELIVERY comes from declared facts (affordance group, ``op``
  observe/subscribe verbs, ``observable``, push transports) via the
  model-layer helper ``classify_delivery`` (registration tooling shares it);
- the REPLY topic of an interactive action comes exclusively from
  ``forms.response.href`` — no string rewriting, ever;
- GRANULARITY comes from the reference keys themselves, walked through the
  affordance's DECLARED data schema (the IDTA AID PropertyDefinition carries
  the WoT-2.0-ized JSON-Schema mirror — ``properties`` containers keyed by the
  exact payload keys, ``items`` for arrays). Every trailing key must exist in
  that schema mirror; typos and non-schema keys fail loudly. Array elements
  cannot be indexed by the schema (no per-index element exists) — per-index
  selection is the transformation's job;
- INTERACTIONS (DMP v3): a mapping whose SOURCE reference targets an
  **Operation SubmodelElement** (own or cross-AAS) is an operation delegation,
  never a poll. One AIMC mapping per operation: the sink is the native command
  affordance, the Transformation blob is the request blob (the reply blob is
  optional; without one replies pass through untransformed). The caller-facing
  REST surface is DERIVED BY CONVENTION — ``/operations/{aas_id_short}/{action}``
  where the action name is the idShort of the skill (the Operation's parent
  container) that owns the delegated operation, matching the
  ``invocationDelegation`` qualifier convention; the ``{aas_id_short}`` macro
  is resolved by the descriptor.
"""

from __future__ import annotations

import contextlib
import logging
import re
from dataclasses import dataclass, field

from aas_model.json_schema_aid import schema_from_datapoint
from aas_model.submodel_templates import Aimc
from aas_model.submodel_templates.aid import DmpAssetInterfacesDescription
from management_node.execution_policy import (
    ACTION_AFFORDANCE,
    EVENT_AFFORDANCE,
    PROPERTY_AFFORDANCE,
    classify_delivery,
    protocol_from_declared_semantic_ids,
    protocol_from_interface_name,
)

logger = logging.getLogger(__name__)

#: AID dict fields holding generic (non-MQTT/REST) interface templates.
_GENERIC_INTERFACE_FIELDS = (
    "InterfaceTemplateForMQTT", "InterfaceTemplateForOPCUA", "InterfaceTemplateForHTTP",
    "InterfaceTemplateForMODBUS", "InterfaceTemplateForBacnet",
    "InterfaceTemplateForIOLINK_OVER_PROFINET_REST",
)

_INTERFACE_PROTOCOLS = {
    "InterfaceTemplateForMQTT": "mqtt",
    "InterfaceTemplateForOPCUA": "opcua",
    "InterfaceTemplateForHTTP": "rest",
    "InterfaceTemplateForMODBUS": "modbus",
    "InterfaceTemplateForBacnet": "bacnet",
    "InterfaceTemplateForIOLINK_OVER_PROFINET_REST": "rest",
}

_RETIRED_JSONATA_SEMANTIC_NAMESPACE = "https://example.org/jsonata/"


@dataclass
class ResolvedSource:
    """A concrete endpoint a DMP can consume, derived from AID + AIMC."""

    source_id: str
    protocol: str  # canonical ("mqtt", "rest", "aas", "opcua", "modbus", ...)
    interface: str  # AID interface name, e.g. "interface_mqtt" ("" for AAS-element)
    affordance_kind: str = ""  # "property" | "action" | "event" | "" (AAS-element)
    base: str | None = None  # endpoint base from AID EndpointMetadata
    topic: str | None = None  # href for MQTT (or path for other protocols)
    #: DECLARED MQTT delivery qualifiers (mqv_qos / mqv_retain) — wired into
    #: the compiled runtime config, never guessed
    qos: int | None = None
    retain: bool | None = None
    #: interactive actions only — the DECLARED reply topic (forms.response.href
    #: or the MQTT 5 ``mqv_response_topic`` form term)
    response_topic: str | None = None
    http_method: str | None = None  # HTTP binding form methodName
    #: MQTT 5 request/reply correlation DECLARED on the form
    #: (``mqv_request_reply``) — see ResolvedSink
    request_reply: bool = False
    #: the affordance's DECLARED parameter types (JSON-schema types from the
    #: embedded input/output DataSchemas, e.g. {"Uuid": "string"}) — see
    #: ResolvedSink
    input_types: dict[str, str] = field(default_factory=dict)
    output_types: dict[str, str] = field(default_factory=dict)
    polling_interval: float | None = None
    data_point: str | None = None  # AID data point name
    value_path: str | None = None  # schema-walk pointer derived from the reference keys
    schema: dict | None = None  # selected device payload (or member) schema
    payload_group: str | None = None
    # OPC UA (OPC-10101): see ResolvedSink — ``node`` doubles as the Variable
    # NodeId (writes) or the owning Object NodeId (method calls).
    node: str | None = None
    method_node: str | None = None
    component_of: str | None = None
    # Modbus binding.
    modbus_entity: str | None = None
    modbus_function: str | None = None
    # An AAS-element source (case 2, AAS->AAS): the submodel reference path
    # used to build the polling-consumer endpoint. protocol == "aas".
    ref_path: list[str] = field(default_factory=list)
    # AAS Property sources have AIMC-specific event/poll semantics.  Keep the
    # distinction in the typed resolution instead of inferring it from a
    # submodel name or from the target sink.
    aas_property: bool = False
    # Last referenced AAS key type.  This distinguishes a complete SMC from
    # a Property without relying on submodel names or station conventions.
    aas_element_type: str | None = None
    # How the source delivers data, derived from DECLARED facts:
    # "async" = push / subscription (event); "sync" = request/response (polled).
    delivery: str = "sync"


@dataclass
class ResolvedSink:
    """A concrete endpoint a DMP can write to, derived from AID + AIMC."""

    sink_id: str
    aas: bool = False
    protocol: str = ""
    interface: str = ""
    affordance_kind: str = ""
    base: str | None = None
    topic: str | None = None
    data_point: str | None = None
    value_path: str | None = None  # schema-walk pointer derived from the reference keys
    schema: dict | None = None  # selected member schema
    payload_schema: dict | None = None  # complete affordance payload schema
    output_schema: dict | None = None  # correlated action reply schema
    payload_group: str | None = None  # anchored affordance identity, not endpoint URI
    #: DECLARED MQTT delivery qualifiers (mqv_qos / mqv_retain) — wired into
    #: the compiled runtime config, never guessed
    qos: int | None = None
    retain: bool | None = None
    #: interactive actions only — the DECLARED reply topic (forms.response.href
    #: or the MQTT 5 ``mqv_response_topic`` form term)
    response_topic: str | None = None
    #: MQTT 5 request/reply correlation DECLARED on the form
    #: (``mqv_request_reply``): the requester publishes the command with the
    #: response-topic + correlation-data properties; the responder delivers
    #: the reply on that topic echoing the correlation. Absence = the legacy
    #: uncorrelated pub/sub convention (payload-field correlation).
    request_reply: bool = False
    #: the affordance's DECLARED parameter types (JSON-schema types from the
    #: embedded input/output DataSchemas, e.g. {"Uuid": "string"}) — wired
    #: into the DMP at instantiation so value parsing is data-driven, never
    #: name-hardcoded
    input_types: dict[str, str] = field(default_factory=dict)
    output_types: dict[str, str] = field(default_factory=dict)
    # OPC UA (OPC-10101): ``node`` = the Variable NodeId for writes / the
    # owning Object NodeId for method calls (``uav_componentOf``);
    # ``method_node`` = the Method NodeId (§6.2 href convention);
    # ``component_of`` = the declared owning Object (mandatory on actions).
    node: str | None = None
    method_node: str | None = None
    component_of: str | None = None
    # Modbus binding.
    modbus_entity: str | None = None
    modbus_function: str | None = None


@dataclass
class ResolvedMapping:
    """An AIMC mapping with its sources resolved against the AID."""

    id_short: str
    sources: list[ResolvedSource] = field(default_factory=list)
    sink_refs: list[object] = field(default_factory=list)  # pydantic ModelReference
    sink_ids: list[str] = field(default_factory=list)
    resolved_sinks: list[ResolvedSink] = field(default_factory=list)
    transformation: str | None = None
    default_polling_interval: float | None = None
    #: None preserves legacy any-contribution triggering; [] is never valid.
    trigger_source_ids: list[str] | None = None
    trigger_errors: list[str] = field(default_factory=list)

    @property
    def unresolved(self) -> list[str]:
        return list(self.trigger_errors) + [s.source_id for s in self.sources if not s.protocol] + [
            f"sink:{s.sink_id}" for s in self.resolved_sinks
            if not s.aas and not s.protocol
        ]


@dataclass
class ResolvedInteraction:
    """A resolved operation-delegation mapping (DMP v3, one-MC authoring).

    The AIMC mapping's SOURCE is an Operation SubmodelElement; the SINK is the
    native command affordance (``command_sink``, any protocol — MQTT publish +
    correlated reply, HTTP request/response, OPC UA / Modbus write). The
    descriptor builds the per-protocol legs from it. ``request_blob`` is the
    mapping's Transformation; ``response_blob`` stays ``None`` for one-MC
    authoring — replies pass through untransformed. The caller-facing REST
    surface is derived: ``rest_path`` carries the ``{aas_id_short}`` macro,
    resolved by the descriptor (the DMP exposes the surface itself — no
    interface_rest)."""

    id_short: str
    rest_path: str | None = None
    rest_method: str | None = None
    #: protocol of the command affordance ("mqtt", "rest", "opcua", ...)
    protocol: str = ""
    command_sink: "ResolvedSink | None" = None
    command_sink_id: str = ""
    command_sinks: list[tuple[str, "ResolvedSink"]] = field(default_factory=list)
    operation_source_id: str = ""
    caller_reply_sink_id: str | None = None
    request_source_ids: list[str] = field(default_factory=list)
    trigger_source_ids: list[str] | None = None
    response_sources: list[ResolvedSource] = field(default_factory=list)
    response_sink_refs: list[object] = field(default_factory=list)
    response_sinks: list[ResolvedSink] = field(default_factory=list)
    response_sink_ids: list[str] = field(default_factory=list)
    explicit_response_mapping: bool = False
    request_blob: str | None = None
    response_blob: str | None = None
    #: DECLARED MQTT 5 request/reply correlation (copied from the command
    #: affordance's ``mqv_request_reply`` form term)
    request_reply: bool = False
    #: DECLARED reply topic (``mqv_response_topic`` / forms.response.href of
    #: the command affordance)
    response_topic: str | None = None
    #: DECLARED parameter types (copied from the command affordance's
    #: embedded input/output DataSchemas) — the DMP parses values with them
    input_types: dict[str, str] = field(default_factory=dict)
    output_types: dict[str, str] = field(default_factory=dict)
    caller_response_order: list[str] = field(default_factory=list)
    #: reply deadline (fixed DMP runtime semantic, decision 2026-09-15)
    timeout_seconds: float = 30.0
    unresolved: list[str] = field(default_factory=list)


@dataclass
class ResolutionResult:
    """All mappings of an asset, with sources resolved against its AID."""

    asset_id_short: str
    mappings: list[ResolvedMapping] = field(default_factory=list)
    interactions: list[ResolvedInteraction] = field(default_factory=list)

    @property
    def all_resolved(self) -> bool:
        return all(not m.unresolved for m in self.mappings) and \
            all(not i.unresolved for i in self.interactions)


# --------------------------------------------------------------------------- #
# Reference helpers (pydantic ModelReference)
# --------------------------------------------------------------------------- #
def reference_values(ref: object) -> list[str]:
    """The key value path of a pydantic ``ModelReference`` (``ref.key[].value``)."""
    if ref is None:
        return []
    return [k.value for k in getattr(ref, "key", None) or ()]


def reference_keys(ref: object) -> list:
    """The typed key path of a pydantic ``ModelReference`` (``ref.key[]``)."""
    if ref is None:
        return []
    return list(getattr(ref, "key", None) or ())


# --------------------------------------------------------------------------- #
# AID navigation (pydantic)
# --------------------------------------------------------------------------- #
def _declared_protocol(interface_name: str, iface, template_protocol: str) -> str:
    """The protocol DECLARED by the interface itself (supplemental TD semantic
    ids), falling back to the documented name-prefix convention."""
    sid = getattr(iface, "supplemental_semantic_ids", None)
    declared = protocol_from_declared_semantic_ids(sid or [])
    if declared:
        return declared
    fallback = template_protocol or protocol_from_interface_name(interface_name)
    if fallback:
        logger.info(
            "interface %r declares no TD transport semanticId; protocol %r "
            "derived from the documented name convention", interface_name, fallback)
    return fallback


def _get_interface(aid: DmpAssetInterfacesDescription, name: str):
    """Find the exact AID interface and its transport, rejecting ambiguity."""
    matches = []
    for dict_field in _GENERIC_INTERFACE_FIELDS:
        d = getattr(aid, dict_field, None) or {}
        if name in d:
            matches.append((d[name], _INTERFACE_PROTOCOLS[dict_field]))
    if len(matches) > 1:
        raise ValueError(f"AID interface {name!r} is ambiguous across templates")
    return matches[0] if matches else (None, "")


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


def _form_prop(dp, prop: str) -> str | None:
    """The string value of a form/field property, leniently (may be absent)."""
    f = getattr(dp, "forms", None)
    if f is None:
        f = dp
    p = getattr(f, prop, None)
    return p.value if p is not None else None


def _declared_response_form(dp) -> str | None:
    """The EXPLICITLY DECLARED reply href (``forms.response.href``); absence is
    meaningful — callers report unresolved rather than derive a rewrite."""
    resp = getattr(getattr(dp, "forms", None), "response", None)
    href = getattr(resp, "href", None)
    return href.value if href is not None else None


def _dp_observable(dp) -> bool | None:
    obs = getattr(dp, "observable", None)
    if obs is None:
        return None
    return str(obs.value).lower() in ("true", "1")


def _locate_data_point(props, acts, evts, keys: list[str]) -> tuple[str | None, str, int]:
    """Locate the addressed data point; returns (name, affordance_kind, index)."""
    if len(keys) < 5 or keys[2] != "InteractionMetadata":
        return None, "", -1
    group = {"properties": (props, PROPERTY_AFFORDANCE),
             "actions": (acts, ACTION_AFFORDANCE),
             "events": (evts, EVENT_AFFORDANCE)}.get(keys[3])
    if group and keys[4] in group[0]:
        return keys[4], group[1], 4
    return None, "", -1


def _schema_target(dp, keys: list[str], data_point_idx: int,
                   kind: str, *, outgoing: bool) -> tuple[str | None, object]:
    """Derive the fine-granularity JSON Pointer from the reference keys by
    walking the affordance's DECLARED data schema (ADR-020 as revised: no
    authored marker — the AID schema mirror IS the payload contract).

    Walk rules over the trailing keys past the data point:
    - actions start at their ``input`` (sink) or ``output`` (source) schema;
      the corresponding reference key is transparent to the payload pointer;
    - a key naming a schema child inside the current node's ``properties``
      container contributes the payload key as a pointer token;
    - the literal container step ``properties`` is transparent (no token),
      so both ``[..., DP, properties, State]`` and ``[..., DP, State]`` walk
      the same mirror (a payload key literally named ``properties`` wins —
      it is IN the mirror);
    - ``items`` (array): the schema describes the ITEM shape, one element for
      all items — a JSON Pointer would need a runtime index no schema element
      names, so the walk stops at the array key; keys past ``items`` fail
      loudly (per-index selection belongs in the transformation);
    - anything else addresses nothing in the schema and fails loudly naming
      the available keys (an authoring error, never silently ignored).
    """
    node = getattr(dp, "input" if outgoing else "output", None) if kind == ACTION_AFFORDANCE else dp
    if node is None:
        if keys[data_point_idx + 1:]:
            raise ValueError("action reference selects a parameter but its declared "
                             "input/output schema is absent")
        return None, dp
    tokens: list[str] = []
    trailing = keys[data_point_idx + 1:]
    if kind == ACTION_AFFORDANCE and trailing and trailing[0] in ("input", "output"):
        expected = "input" if outgoing else "output"
        if trailing[0] != expected:
            raise ValueError(f"action reference must select {expected}, not {trailing[0]}")
        trailing = trailing[1:]
    for pos, k in enumerate(trailing):
        container = getattr(node, "properties", None)
        named = dict(getattr(container, "property_name", {}) or {})
        child = named.get(k)
        if child is None:
            # Unsafe JSON Schema property names are stored under generated
            # AAS-safe idShort aliases; references use the original wire name.
            child = next((candidate for candidate in named.values()
                          if getattr(getattr(candidate, "key", None), "value", None) == k), None)
        if child is not None:
            node = child
            key_element = getattr(node, "key", None)
            payload_name = getattr(key_element, "value", None) or k
            tokens.append(str(payload_name))
            continue
        if k == "properties" and container is not None:
            continue
        if k == "items" and getattr(node, "items", None) is not None:
            if pos == len(trailing) - 1:
                break  # stop at the array key: the whole array is delivered
            raise ValueError(
                f"reference keys past 'items' ({trailing[pos + 1:]}) cannot be "
                "expressed: the schema names no per-index element — select the "
                "index in the transformation")
        raise ValueError(
            f"reference key '{k}' is not a key of the declared data schema "
            f"(available: {sorted(named) or 'none'}); fix the reference or "
            "bind the whole data point")
    if not tokens:
        return None, node
    if any(not token or any(c in token for c in ".[]/ ~") for token in tokens):
        if any(not token for token in tokens):
            raise ValueError("an empty schema property name cannot be addressed by a DMP valuePath")
        pointer = "/" + "/".join(t.replace("~", "~0").replace("/", "~1") for t in tokens)
    else:
        pointer = "$." + ".".join(tokens)
    return pointer, node


def _resolved_schema(node) -> dict | None:
    # Older AIDs can omit the DataSchema entirely; do not invent constraints.
    if node is None:
        return None
    schema = schema_from_datapoint(node)
    return schema or None


def _apply_interface(aid_model, container, keys: list[str], interface_name: str,
                     resolved) -> bool:
    """Fully resolves ``resolved``'s endpoint fields from ``aid_model``.

    Returns ``True`` only when the interface exists on the model AND the
    reference's data point is found inside it (both must hold — an interface
    shell without the data point does not resolve)."""
    interface, template_protocol = _get_interface(aid_model, interface_name)
    if interface is None:
        return False
    props, acts, evts = _iface_groups(interface)
    data_point, kind, data_point_idx = _locate_data_point(props, acts, evts, keys)
    if data_point is None:
        return False

    resolved.base = _iface_base(interface)
    resolved.protocol = _declared_protocol(interface_name, interface, template_protocol)
    resolved.data_point = data_point

    dp = {PROPERTY_AFFORDANCE: props, ACTION_AFFORDANCE: acts,
          EVENT_AFFORDANCE: evts}[kind][data_point]
    resolved.affordance_kind = kind
    resolved.value_path, selected = _schema_target(dp, keys, data_point_idx, kind,
                                                    outgoing=False)
    resolved.schema = _resolved_schema(selected)
    resolved.payload_group = "/".join(keys[:data_point_idx + 1])
    if dp is not None:
        _apply_declared_form_facts(resolved, dp, kind)
        if hasattr(resolved, "modbus_entity"):
            resolved.modbus_entity = _form_prop(dp, "modv_entity")
            resolved.modbus_function = _form_prop(dp, "modv_function")
            resolved.node = (
                _form_prop(dp, "uav_browsePath")
                or _opcua_node_id_from_href(resolved.topic)
                or resolved.topic)
            if resolved.node in (None, resolved.topic):
                node = getattr(dp, "node", None)
                if node:
                    resolved.node = node.value
    resolved.delivery = classify_delivery(
        affordance_kind=kind,
        protocol=resolved.protocol,
        op=_form_prop(dp, "op") if dp is not None else "",
        observable=_dp_observable(dp) if dp is not None else None,
    )
    # A subscription queue cannot implement a fresh MQTT Read. Reject rather
    # than treating retained/event messages as periodic values.
    if resolved.protocol == "mqtt" and kind == PROPERTY_AFFORDANCE:
        if resolved.polling_interval or resolved.delivery == "sync":
            raise ValueError(
                "MQTT property sources support Observe, not Read; "
                "remove readproperty/PollingInterval or use a protocol with a Read binding"
            )
    # An explicitly declared polling interval pulls a readable affordance.
    if resolved.polling_interval:
        resolved.delivery = "sync"
    return True


#: Cross-asset resolution (ADR-019): references anchor at precise submodel
#: instances; the callable returns the parsed AID for that anchor id, or None.
AidForAnchor = object  # Callable[[str], DmpAssetInterfacesDescription | None]


def _resolve_source(source, aid: DmpAssetInterfacesDescription,
                    aid_for_anchor=None) -> ResolvedSource:
    keys = reference_values(source.Source.value)
    interface_name = keys[1] if len(keys) >= 4 and keys[2] == "InteractionMetadata" else None

    resolved = ResolvedSource(
        source_id=source.SourceId.value,
        protocol="",
        interface=interface_name or "",
        polling_interval=_float(getattr(source, "PollingInterval", None)),
    )

    if not interface_name:
        typed_keys = reference_keys(source.Source.value)
        if len(keys) >= 2 and typed_keys[0].type_ == "Submodel":
            resolved.protocol = "aas"
            resolved.ref_path = list(keys)
            key_types = [getattr(k, "type_", "") for k in typed_keys]
            resolved.aas_element_type = key_types[-1] if key_types else "Submodel"
            resolved.aas_property = key_types[-1].lower() in {
                "property", "submodelelementproperty"
            }
            return resolved
        logger.warning("source %s: no addressed interface or AAS element in reference %s",
                       resolved.source_id, keys)
        return resolved

    # The anchor identity governs: references anchor at a precise submodel
    # instance, so when a directory is wired that instance gets the first
    # shot — including the asset's own AID instance. An explicit anchor must
    # never silently resolve to an unrelated local model.
    if aid_for_anchor is not None and keys:
        remote = aid_for_anchor(keys[0])
        if remote is not None and _apply_interface(remote, source, keys,
                                                    interface_name, resolved):
            logger.info("source %s resolved via referenced AID instance %s",
                        resolved.source_id, keys[0])
            return resolved
    elif _apply_interface(aid, source, keys, interface_name, resolved):
        return resolved

    logger.warning("source %s: AID interface %r or its data point not found "
                   "(keys=%s)", resolved.source_id, interface_name, keys)
    return resolved


def _float(prop) -> float | None:
    if prop is None or prop.value in (None, ""):
        return None
    try:
        return float(prop.value)
    except (TypeError, ValueError):
        raise ValueError(f"polling interval must be numeric, got {prop.value!r}")


def _resolve_sink(sink, aid: DmpAssetInterfacesDescription, aid_for_anchor=None) -> ResolvedSink:
    keys = reference_values(sink.Sink.value)
    interface_name = keys[1] if len(keys) >= 4 and keys[2] == "InteractionMetadata" else None
    if interface_name is None:
        return ResolvedSink(sink_id=sink.SinkId.value, aas=True)

    resolved = ResolvedSink(sink_id=sink.SinkId.value, interface=interface_name)

    # anchor identity governs (see _resolve_source)
    if aid_for_anchor is not None and keys:
        remote = aid_for_anchor(keys[0])
        if remote is not None and _apply_sink_interface(remote, sink, keys,
                                                         interface_name, resolved):
            logger.info("sink %s resolved via referenced AID instance %s",
                        resolved.sink_id, keys[0])
            return resolved
    elif _apply_sink_interface(aid, sink, keys, interface_name, resolved):
        return resolved

    logger.warning("sink %s: AID interface %r or its data point not found "
                   "(keys=%s)", resolved.sink_id, interface_name, keys)
    return resolved


def _apply_sink_interface(aid_model, sink, keys: list[str], interface_name: str,
                          resolved) -> bool:
    """Fully resolves ``resolved`` from ``aid_model``; both the interface and
    the data point must exist for a success."""
    interface, template_protocol = _get_interface(aid_model, interface_name)
    if interface is None:
        return False
    props, acts, evts = _iface_groups(interface)
    data_point, kind, data_point_idx = _locate_data_point(props, acts, evts, keys)
    if data_point is None:
        return False

    resolved.base = _iface_base(interface)
    resolved.protocol = _declared_protocol(interface_name, interface, template_protocol)
    resolved.data_point = data_point
    resolved.affordance_kind = kind
    dp = {
        PROPERTY_AFFORDANCE: props,
        ACTION_AFFORDANCE: acts,
        EVENT_AFFORDANCE: evts,
    }[kind].get(data_point)
    resolved.value_path, selected = _schema_target(dp, keys, data_point_idx, kind,
                                                    outgoing=True)
    resolved.schema = _resolved_schema(selected)
    payload = getattr(dp, "input", None) if kind == ACTION_AFFORDANCE else dp
    resolved.payload_schema = _resolved_schema(payload)
    if kind == ACTION_AFFORDANCE:
        resolved.output_schema = _resolved_schema(getattr(dp, "output", None))
    resolved.payload_group = "/".join(keys[:data_point_idx + 1])
    if dp is not None:
        _apply_declared_form_facts(resolved, dp, kind)
    return True


#: OPC-10101 §6.2 href convention: ``/?id=<nodeId>`` relative to the OPC UA
#: base (hash/ampersand percent-encoded per the spec).
_OPCUA_HREF_ID_RE = re.compile(r"^/?\?id=(?P<node>.+)$", re.IGNORECASE)


def _opcua_node_id_from_href(href: str | None) -> str | None:
    """The NodeId addressed by an OPC UA binding href (§6.2), or ``None`` when
    the href does not follow the convention."""
    if not href:
        return None
    m = _OPCUA_HREF_ID_RE.match(href.strip())
    if m is None:
        return None
    return m.group("node").replace("%23", "#").replace("%26", "&")


def _data_schema_types(dp, attr: str) -> dict[str, str]:
    """name → JSON-schema type for ``dp.<attr>`` (the affordance's
    ``input``/``output`` DataSchema) — from the embedded
    ``properties.property_name[*].type`` declarations."""
    data_schema = getattr(dp, attr, None)
    if data_schema is None:
        return {}
    props = getattr(data_schema, "properties", None)
    names = getattr(props, "property_name", None) or {}
    out: dict[str, str] = {}

    def schema_type(schema) -> str | None:
        type_element = getattr(schema, "type", None)
        type_name = getattr(type_element, "value", None)
        if type_name != "array":
            return str(type_name).strip() if type_name else None
        item_schema = getattr(schema, "items", None)
        item_type = schema_type(item_schema) if item_schema is not None else None
        return f"array<{item_type or 'unknown'}>"

    for name, schema in names.items():
        value = schema_type(schema)
        if value:
            key_element = getattr(schema, "key", None)
            payload_name = getattr(key_element, "value", None) or name
            out[str(payload_name)] = str(value).strip()
    return out


def _apply_declared_form_facts(resolved, dp, kind: str) -> None:
    """The DECLARED facts of one affordance instance onto its resolved
    endpoint: topic, the interactive reply topic, the MQTT delivery
    qualifiers (``mqv_qos``/``mqv_retain`` — wired into the compiled runtime
    config verbatim; absence stays absent, WoT defaults apply), the MQTT 5
    request/reply correlation terms (``mqv_request_reply`` /
    ``mqv_response_topic``) and the embedded input/output parameter types."""
    resolved.topic = _form_prop(dp, "href")
    if kind == ACTION_AFFORDANCE:
        resolved.http_method = _form_prop(dp, "htv_methodName")
        response_href = _declared_response_form(dp)
        if isinstance(response_href, str) and response_href.startswith("/"):
            resolved.response_topic = response_href.lstrip("/")
        # MQTT 5 request/reply correlation: the DECLARED form term wins; the
        # declared response topic refines the reply destination
        rr = (_form_prop(dp, "mqv_request_reply") or "").strip().lower()
        resolved.request_reply = rr in ("true", "1")
        rt = _form_prop(dp, "mqv_response_topic")
        if isinstance(rt, str) and rt.strip():
            resolved.response_topic = rt.strip().lstrip("/")
        # the affordance's DECLARED parameter types (input/output DataSchemas)
        resolved.input_types = _data_schema_types(dp, "input")
        resolved.output_types = _data_schema_types(dp, "output")
    qos_text = _form_prop(dp, "mqv_qos")
    if qos_text is not None and qos_text.strip():
        with contextlib.suppress(ValueError):
            resolved.qos = int(qos_text)
    retain_text = _form_prop(dp, "mqv_retain")
    if retain_text is not None and retain_text.strip():
        resolved.retain = retain_text.strip().lower() in ("true", "1")
    # OPC UA binding (OPC-10101): the §6.2-parsed NodeId, plus — on actions —
    # the MANDATORY owning object (uav_componentOf on the protocol-specific
    # FORM, §7.1.3); for a method invocation the resolved node IS the owning
    # Object (the Call objectId)
    if hasattr(resolved, "method_node"):
        resolved.method_node = _opcua_node_id_from_href(resolved.topic)
        comp = _form_prop(dp, "uav_componentOf")
        if comp:
            resolved.component_of = comp
            resolved.node = comp


def resolve(aimc: Aimc, aid: DmpAssetInterfacesDescription | None, *,
            aid_for_anchor=None,
            element_type_for_reference=None,
            operation_output_order: dict[str, list[str]] | None = None) -> ResolutionResult:
    """Resolve mappings from their anchored references.

    ``aid_for_anchor`` enables cross-asset resolution (ADR-019): references
    anchor at a precise submodel instance — when wired, that instance (via
    the :class:`~management_node.submodel_locator.AidDirectory` cache) gets
    the exclusive shot at defining the referenced interface. A missing or
    mismatched anchor stays unresolved rather than selecting a local endpoint.
    A local ``aid`` is optional when ``aid_for_anchor`` resolves each reference.

    ``operation_output_order`` carries the CCI operation's output_variable
    id_shorts per action name (positionally ordered) — the awaited UA Call
    zip's declared positional contract.

    Mappings whose SOURCE targets an Operation SubmodelElement (DMP v3) are
    classified as interactions and never enter ``mappings`` — they compile to
    ProtocolBridge routes, not execution-policy polls."""
    result = ResolutionResult(asset_id_short=aimc.id_short or "")
    order = operation_output_order or {}

    def verify_reference(ref, *, context: str) -> None:
        if element_type_for_reference is None:
            return
        keys = reference_keys(ref)
        if len(keys) < 2 or keys[0].type_ != "Submodel":
            raise ValueError(f"{context}: reference must identify a submodel element")
        if len(keys) >= 4 and keys[2].value == "InteractionMetadata":
            return  # AID affordance paths are validated against the AID model.
        actual = element_type_for_reference(ref)
        if actual is None:
            return  # repository unavailable; reference type remains provisional
        expected = keys[-1].type_
        if actual != expected:
            raise ValueError(
                f"{context}: reference claims {expected}, repository stores {actual}"
            )

    for index, mc in enumerate(aimc.MappingConfigurations.value):
        mapping_name = _mapping_name(mc, index)
        for qualifier in getattr(mc, "qualifiers", None) or []:
            if (getattr(qualifier, "type_", "") or "").lower() == "writedelegation":
                raise ValueError(f"mapping {mapping_name}: retired writeDelegation qualifier "
                                 "is no longer supported; migrate the mapping before use")
        _reject_retired_jsonata(mc, mapping_name)
        request_source_ids = [source.SourceId.value for source in mc.Sources.value]
        if len(request_source_ids) != len(set(request_source_ids)):
            raise ValueError(f"mapping {_mapping_name(mc, index)}: request SourceIds must be unique")
        trigger_source_ids = _declared_trigger_source_ids(mc, index)
        if trigger_source_ids is not None:
            unknown = [source_id for source_id in trigger_source_ids
                       if source_id not in set(request_source_ids)]
            if unknown:
                raise ValueError(f"mapping {_mapping_name(mc, index)}: ExecutionSemantics selects "
                                 f"unknown request SourceIds {unknown}")
        for source in mc.Sources.value:
            verify_reference(source.Source.value,
                             context=f"mapping {_mapping_name(mc, index)} source {source.SourceId.value}")
        for sink in mc.Sinks.value:
            verify_reference(sink.Sink.value,
                             context=f"mapping {_mapping_name(mc, index)} sink {sink.SinkId.value}")
        operation = _operation_interaction(mc, aid, aid_for_anchor,
                                           response_order=order.get(_operation_action_name(mc)),
                                           trigger_source_ids=trigger_source_ids)
        if operation is not None:
            result.interactions.append(operation)
            continue
        resolved_sources = [_resolve_source(s, aid, aid_for_anchor) for s in mc.Sources.value]
        for source in resolved_sources:
            if source.affordance_kind == ACTION_AFFORDANCE:
                raise ValueError(
                    f"mapping {_mapping_name(mc, index)} source {source.source_id}: "
                    "an action affordance requires an operation delegation"
                )
            if source.protocol in ("rest", "http", "modbus", "plc4x", "bacnet") \
                    and source.delivery == "async":
                raise ValueError(
                    f"mapping {_mapping_name(mc, index)} source {source.source_id}: "
                    f"{source.protocol} has no asynchronous event consumer"
                )
        sink_refs = [s.Sink.value for s in mc.Sinks.value]
        resolved_sinks = [_resolve_sink(s, aid, aid_for_anchor) for s in mc.Sinks.value]
        for sink in resolved_sinks:
            if sink.affordance_kind == ACTION_AFFORDANCE:
                raise ValueError(
                    f"mapping {_mapping_name(mc, index)} sink {sink.sink_id}: "
                    "an action affordance requires an operation delegation"
                )
        result.mappings.append(
            ResolvedMapping(
                id_short=_mapping_name(mc, index),
                sources=resolved_sources,
                sink_refs=sink_refs,
                sink_ids=[s.SinkId.value for s in mc.Sinks.value],
                resolved_sinks=resolved_sinks,
                transformation=_blob_text(mc.Transformation),
                default_polling_interval=_float(mc.DefaultPollingInterval),
                trigger_source_ids=trigger_source_ids,
            )
        )
    _disambiguate_binding_paths(result.interactions)
    return result


def _disambiguate_binding_paths(interactions: list) -> None:
    """Dual-binding operation delegations (DMP v3): one CCI operation can be
    mapped once per native binding (MQTT command + OPC UA method call, …),
    but the derived REST surface must stay unambiguous — the FIRST mapping
    of an action (the MQTT leg when present: the surface the
    ``invocationDelegation`` qualifier advertises) keeps the plain
    ``/operations/{aas_id_short}/{action}`` path; every further binding gets
    a deterministic ``/{protocol}`` sub-resource."""
    by_action: dict[str, list] = {}
    for interaction in interactions:
        by_action.setdefault(interaction.id_short, []).append(interaction)
    for siblings in by_action.values():
        if len(siblings) < 2:
            continue
        protocols = [i.protocol for i in siblings]
        primary = siblings[protocols.index("mqtt")] if "mqtt" in protocols \
            else siblings[0]
        for interaction in siblings:
            if interaction is primary:
                continue
            suffix = interaction.protocol or "binding"
            interaction.rest_path = \
                _OPERATION_PATH_TEMPLATE.format(action=interaction.id_short) \
                + f"/{suffix}"


#: idShort values serialized AAS tooling leaves on MappingConfiguration SMCs
#: instead of a real name; none of them is a usable mapping name.
_MAPPING_PLACEHOLDER_NAMES = (
    "SubmodelElementCollection", "MappingConfigurations", "mapping",
    "MappingConfiguration",
)


def _declared_trigger_source_ids(mc, index: int) -> list[str] | None:
    semantics = getattr(mc, "ExecutionSemantics", None)
    if semantics is None:
        return None
    source_list = getattr(semantics, "TriggerSourceIds", None)
    values = getattr(source_list, "value", None) if source_list is not None else None
    ids = [str(getattr(value, "value", value)) for value in (values or [])]
    name = _mapping_name(mc, index)
    if not ids or any(not source_id or source_id == "None" for source_id in ids):
        raise ValueError(f"mapping {name}: authored ExecutionSemantics.TriggerSourceIds must be nonempty")
    if len(ids) != len(set(ids)):
        raise ValueError(f"mapping {name}: ExecutionSemantics.TriggerSourceIds must be unique")
    return ids


def _mapping_name(mc, index: int) -> str:
    """The mapping's name: the SMC ``id_short`` when meaningful, else
    ``mapping_<index>`` from the MappingConfigurations list position."""
    name = getattr(mc, "id_short", None) or ""
    if name and name not in _MAPPING_PLACEHOLDER_NAMES:
        return name
    return f"mapping_{index}"


def _blob_text(blob) -> str | None:
    """The Lua text of a Transformation-style Blob, leniently (None when the
    blob is absent or carries only a placeholder/empty value)."""
    if blob is None:
        return None
    v = blob.value
    if isinstance(v, bytes):
        try:
            v = v.decode("utf-8")
        except UnicodeDecodeError:
            v = v.decode("utf-8", errors="replace")
    else:
        v = str(v)
    # aas_model's default blob value is the placeholder "None"; treat it (and
    # empty) as "no transformation" (passthrough).
    if not v or v.strip() in ("None", "none", "null", "b''"):
        return None
    return v


def _reject_retired_jsonata(mc, mapping_name: str) -> None:
    """Do not reinterpret a retired JSONata descriptor as Lua source."""
    for blob_name in ("Transformation", "ResponseTransformation"):
        tr = getattr(mc, blob_name, None)
        if tr is None:
            continue
        sids = [getattr(tr, "semantic_id", None) or ""]
        sids += list(getattr(tr, "supplemental_semantic_ids", None) or [])
        for sid in sids:
            if str(sid).lower().startswith(_RETIRED_JSONATA_SEMANTIC_NAMESPACE):
                raise ValueError(
                    f"mapping {mapping_name}: JSONata transformations are no longer supported; "
                    f"migrate the {blob_name} blob to Lua"
                )


# --------------------------------------------------------------------------- #
# Operation delegation — one mapping per operation (DMP v3)
#
# A mapping whose SOURCE reference targets an Operation SubmodelElement (the
# last reference key carries the ``Operation`` key type) IS an operation
# delegation: sink = the native command affordance, Transformation = request
# blob, replies pass through untransformed. The caller-facing REST surface is
# derived by convention from the reference's idShortPath — no interface_rest,
# no two-mapping pairing, no caller protocol anywhere.
# --------------------------------------------------------------------------- #
#: AAS key type of the Operation SubmodelElement.
OPERATION_KEY_TYPE = "Operation"

#: Container key types an Operation may be nested under — the owning skill's
#: idShort names the action on the derived REST surface.
_CONTAINER_KEY_TYPES = ("SubmodelElementCollection", "SubmodelElementList",
                        "SubmodelElement")

#: The caller-facing operation path convention (DMP v3). The macro resolves
#: at descriptor compile time (the descriptor knows the unit's AAS idShort).
_OPERATION_PATH_TEMPLATE = "/operations/{{aas_id_short}}/{action}"


def _operation_source_keys(mc) -> tuple[list, list] | None:
    """The typed key paths of the mapping's Operation-element sources.

    Returns ``(operation_keys, other_keys)`` — empty ``other_keys`` when the
    mapping is a clean one-operation authoring; ``None`` when no source
    targets an Operation element at all."""
    operation_keys: list[list] = []
    other_keys: list[list] = []
    for src in mc.Sources.value:
        keys = reference_keys(getattr(src.Source, "value", None))
        if keys and keys[-1].type_ == OPERATION_KEY_TYPE:
            operation_keys.append(keys)
        else:
            other_keys.append(keys)
    if not operation_keys:
        return None
    return operation_keys, other_keys


def _operation_action(keys: list) -> str:
    """The action name addressed by an Operation-element reference.

    The operation leaf itself is the fixed delegation entry point
    (``.../Skills/<skill>/operation``); the action named on the derived REST
    surface is the idShort of the skill container that owns the operation —
    the same segment the ``invocationDelegation`` qualifier advertises. A
    reference anchored directly at a meaningfully-named Operation element
    (no owning container) falls back to that element's own idShort."""
    if len(keys) >= 2 and keys[-2].type_ in _CONTAINER_KEY_TYPES:
        return keys[-2].value
    return keys[-1].value


def _operation_action_name(mc) -> str | None:
    """The operation mapping's action name without resolving its endpoints —
    the Operation source's last path segment (the CCI operation lookup key)."""
    classified = _operation_source_keys(mc)
    if classified is None:
        return None
    operation_keys, _ = classified
    return _operation_action(operation_keys[0])


def _operation_interaction(mc, aid: DmpAssetInterfacesDescription,
                           aid_for_anchor=None,
                           response_order: list[str] | None = None,
                           trigger_source_ids: list[str] | None = None) -> ResolvedInteraction | None:
    """Classify a mapping as an operation delegation and resolve its endpoints.

    Returns ``None`` when no source targets an Operation element (the mapping
    stays a normal data mapping). The sink must be an ACTION affordance on the
    AID's own interfaces — any protocol (MQTT publish + correlated reply, HTTP
    request/response, OPC UA node write, Modbus write); AAS-element sinks
    (properties) and non-action affordances never compile. The reply leg is
    protocol-inherent where the protocol is synchronous (HTTP); MQTT commands
    REQUIRE the affordance's declared ``forms.response.href``."""
    classified = _operation_source_keys(mc)
    if classified is None:
        return None
    operation_keys, other_keys = classified

    keys = operation_keys[0]
    action = _operation_action(keys)
    resolved = ResolvedInteraction(
        id_short=action,
        rest_path=_OPERATION_PATH_TEMPLATE.format(action=action),
        rest_method="POST",
        request_blob=_blob_text(mc.Transformation),
        # the AIMC's OPTIONAL ResponseTransformation blob (ADR-022) authors
        # the reply direction; one-MC authoring leaves it absent and replies
        # pass through unchanged
        response_blob=_blob_text(getattr(mc, "ResponseTransformation", None)),
        operation_source_id=mc.Sources.value[
            next(i for i, source in enumerate(mc.Sources.value)
                 if reference_keys(source.Source.value)[-1].type_ == OPERATION_KEY_TYPE)
        ].SourceId.value,
        request_source_ids=[source.SourceId.value for source in mc.Sources.value],
        trigger_source_ids=trigger_source_ids,
        caller_response_order=list(response_order or []),
    )
    if trigger_source_ids is not None and trigger_source_ids != [resolved.operation_source_id]:
        resolved.unresolved.append(
            f"{action}: delegated action ExecutionSemantics must select exactly its Operation "
            f"SourceId {resolved.operation_source_id!r}")
    if len(operation_keys) > 1:
        resolved.unresolved.append(
            f"{action}: mapping targets {len(operation_keys)} Operation "
            "elements — one mapping per operation (split the sources)")
    if other_keys:
        resolved.unresolved.append(
            f"{action}: enrichment sources are declared but invocation-time sampling "
            "is not implemented yet; refusing to invoke Lua with missing SourceIds")
    if len(set(resolved.request_source_ids)) != len(resolved.request_source_ids):
        resolved.unresolved.append(f"{action}: request SourceIds must be unique")
    action_sinks = _command_sinks(mc, aid, aid_for_anchor)
    if len(action_sinks) != len(mc.Sinks.value):
        resolved.unresolved.append(
            f"{action}: every request sink must address a member of the same "
            "ACTION affordance")
    distinct = {item.payload_group or item.data_point for _, item in action_sinks}
    if len(distinct) > 1:
        resolved.unresolved.append(
            f"{action}: one action route cannot target multiple distinct ACTION affordances")
    command_sink_ids = [sink_id for sink_id, _ in action_sinks]
    if len(command_sink_ids) != len(set(command_sink_ids)):
        resolved.unresolved.append(f"{action}: command SinkIds must be unique")
    command_paths = [item.value_path for _, item in action_sinks]
    if len(action_sinks) > 1 and (None in command_paths or len(command_paths) != len(set(command_paths))):
        resolved.unresolved.append(
            f"{action}: multiple command sinks must select distinct members, not mix a whole-affordance sink")
    sink = action_sinks[0][1] if action_sinks else None
    if sink is None:
        resolved.unresolved.append(
            f"{action}: no native command affordance sink (the sink must be an "
            "ACTION affordance on the AID's own interfaces, never an AAS "
            "property)")
    else:
        resolved.command_sink = sink
        resolved.command_sink_id = action_sinks[0][0]
        resolved.command_sinks = action_sinks
        resolved.protocol = sink.protocol
        # the DECLARED contract of the command affordance rides with the
        # interaction: MQTT 5 request/reply correlation, reply topic and the
        # embedded parameter types — the DMP parses values with them
        resolved.request_reply = bool(sink.request_reply)
        resolved.response_topic = sink.response_topic
        resolved.input_types = dict(sink.input_types)
        resolved.output_types = dict(sink.output_types)
        if sink.protocol in ("plc4x", "modbus", "bacnet"):
            resolved.unresolved.append(
                f"{action}: {sink.protocol} exposes tag/register Write, not a native "
                "synchronous action invocation")
        if sink.protocol == "mqtt" and not sink.response_topic:
            # the correlated reply leg rides the DECLARED response form of the
            # command affordance; there is nothing to rewrite, ever
            resolved.unresolved.append(
                f"{action}: command affordance {sink.data_point!r} declares no "
                "response form (set forms.response.href explicitly)")

    response_sources = list(getattr(getattr(mc, "ResponseSources", None), "value", None) or [])
    response_sinks = list(getattr(getattr(mc, "ResponseSinks", None), "value", None) or [])
    resolved.explicit_response_mapping = bool(response_sources or response_sinks)
    response_source_ids = [item.SourceId.value for item in response_sources]
    resolved.response_sink_ids = [item.SinkId.value for item in response_sinks]
    if len(set(response_source_ids)) != len(response_source_ids):
        resolved.unresolved.append(f"{action}: ResponseSource ids must be unique")
    if len(set(resolved.response_sink_ids)) != len(resolved.response_sink_ids):
        resolved.unresolved.append(f"{action}: ResponseSink ids must be unique")
    for item in response_sources:
        source = _resolve_source(item, aid, aid_for_anchor)
        if (sink is None or source.affordance_kind != ACTION_AFFORDANCE
                or source.payload_group != sink.payload_group):
            resolved.unresolved.append(
                f"{action}: ResponseSource {source.source_id} must select output of "
                "the command ACTION affordance")
        resolved.response_sources.append(source)
    member_paths = [source.value_path for source in resolved.response_sources]
    if len(member_paths) != len(set(member_paths)) or (None in member_paths and len(member_paths) > 1):
        resolved.unresolved.append(f"{action}: ResponseSources must select distinct output members")

    for item in response_sinks:
        response_keys = reference_keys(item.Sink.value)
        if response_keys and response_keys[-1].type_ == OPERATION_KEY_TYPE:
            if [key.value for key in response_keys] != [key.value for key in keys]:
                resolved.unresolved.append(
                    f"{action}: Operation ResponseSink must reference the original Operation")
            elif resolved.caller_reply_sink_id is not None:
                resolved.unresolved.append(
                    f"{action}: only one ResponseSink may select the original Operation")
            else:
                resolved.caller_reply_sink_id = item.SinkId.value
        response_sink = _resolve_sink(item, aid, aid_for_anchor)
        if response_sink.affordance_kind == ACTION_AFFORDANCE:
            resolved.unresolved.append(
                f"{action}: an ACTION affordance cannot be a ResponseSink (chained actions are unsupported)")
        resolved.response_sink_refs.append(item.Sink.value)
        resolved.response_sinks.append(response_sink)
    response_targets = [(item.payload_group, item.value_path) for item in resolved.response_sinks
                       if not item.aas]
    if len(response_targets) != len(set(response_targets)):
        resolved.unresolved.append(f"{action}: ResponseSinks must target distinct affordance members")
    if resolved.explicit_response_mapping and resolved.response_blob is None:
        resolved.unresolved.append(
            f"{action}: explicit ResponseSources/ResponseSinks require a ResponseTransformation")

    if resolved.request_blob is None:
        resolved.unresolved.append(f"{action}: mapping carries no Transformation "
                                   "request blob")

    if resolved.unresolved:
        logger.warning("interaction %s unresolved: %s", action,
                       "; ".join(resolved.unresolved))
    return resolved


def _command_sinks(mc, aid: DmpAssetInterfacesDescription,
                   aid_for_anchor=None):
    """The mapping's native command affordance sink: an ACTION affordance on
    one of the AID's own interfaces, any protocol. AAS-element sinks and
    property/event affordances are not command targets — the interaction
    stays unresolved rather than wiring a wrong endpoint."""
    found = []
    for sink in mc.Sinks.value:
        resolved = _resolve_sink(sink, aid, aid_for_anchor)
        if resolved.aas or not resolved.protocol:
            continue
        if resolved.affordance_kind != ACTION_AFFORDANCE:
            logger.warning(
                "interaction sink %r is a %s affordance on %s — not an action "
                "affordance", resolved.data_point,
                resolved.affordance_kind or "unknown", resolved.protocol)
            continue
        found.append((sink.SinkId.value, resolved))
    return found
