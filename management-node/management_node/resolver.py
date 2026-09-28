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

from aas_model.submodel_templates import Aimc
from management_node.execution_policy import (
    ACTION_AFFORDANCE,
    EVENT_AFFORDANCE,
    PROPERTY_AFFORDANCE,
    classify_delivery,
    protocol_from_declared_semantic_ids,
    protocol_from_interface_name,
)
from aas_model.submodel_templates.aid import DmpAssetInterfacesDescription

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

#: Documented private namespace selecting the JSONata engine (ADR-015).
JSONATA_SEMANTIC_NAMESPACE = "https://example.org/jsonata/"


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
    transformation_engine: str = "LUA"  # ADR-015 / SPEC §7.6
    default_polling_interval: float | None = None

    @property
    def unresolved(self) -> list[str]:
        return [s.source_id for s in self.sources if not s.protocol] + [
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
    #: DECLARED positional reply order (the CCI operation's output_variable
    #: id_shorts, in order): the positional contract of the method's output
    #: arguments. The AID's embedded output DataSchema carries TYPES, not the
    #: positional order (its property order follows the schema file, not the
    #: method signature) — the awaited UA Call zip uses THIS order.
    response_order: list[str] = field(default_factory=list)
    #: reply deadline (fixed DMP runtime semantic, decision 2026-09-15)
    timeout_seconds: float = 30.0
    unresolved: list[str] = field(default_factory=list)


@dataclass
class ResolvedPropertyWrite:
    """A resolved property write-delegation (W2, Shape A).

    The AIMC mapping carries the ``writeDelegation`` qualifier; its SOURCE
    is the DEVICE affordance to write (a writable UA variable, an MQTT
    command/reply property pair, an HTTP resource — any supported protocol)
    and its SINK is the AAS property that receives the effective value
    AFTER the device acknowledges (ack-then-writeback). The caller PUTs the
    derived surface: ``rest_path`` = ``/properties/{aas_id_short}/{name}``
    (from the qualifier value's ``{delegation_base}...`` convention or by
    convention from the writeback reference)."""

    id_short: str
    rest_path: str | None = None
    rest_method: str = "PUT"
    #: protocol of the device affordance ("mqtt", "opcua", "rest", ...)
    protocol: str = ""
    device_source: "ResolvedSource | None" = None
    #: AAS writeback reference (key values) + SinkId for the value envelope
    writeback_keys: list[str] = field(default_factory=list)
    writeback_sink_id: str = ""
    #: the raw sink ModelReference (the descriptor's AAS-sink builder reads it)
    writeback_ref: object | None = None
    request_blob: str | None = None
    #: reply-direction blob (device ack → effective value envelope); MQTT
    #: acks REQUIRE it (the correlated reply carries the effective value)
    response_blob: str | None = None
    timeout_seconds: float = 30.0
    unresolved: list[str] = field(default_factory=list)


@dataclass
class ResolutionResult:
    """All mappings of an asset, with sources resolved against its AID."""

    asset_id_short: str
    mappings: list[ResolvedMapping] = field(default_factory=list)
    interactions: list[ResolvedInteraction] = field(default_factory=list)
    property_writes: list[ResolvedPropertyWrite] = field(default_factory=list)

    @property
    def all_resolved(self) -> bool:
        return all(not m.unresolved for m in self.mappings) and \
            all(not i.unresolved for i in self.interactions) and \
            all(not w.unresolved for w in self.property_writes)


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


def _schema_pointer(dp, keys: list[str], data_point_idx: int) -> str | None:
    """Derive the fine-granularity JSON Pointer from the reference keys by
    walking the affordance's DECLARED data schema (ADR-020 as revised: no
    authored marker — the AID schema mirror IS the payload contract).

    Walk rules over the trailing keys past the data point:
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
    node = dp
    tokens: list[str] = []
    trailing = keys[data_point_idx + 1:]
    for pos, k in enumerate(trailing):
        container = getattr(node, "properties", None)
        named = dict(getattr(container, "property_name", {}) or {})
        if k in named:
            node = named[k]
            tokens.append(k)
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
    return "$." + ".".join(tokens) if tokens else None


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
    resolved.value_path = _schema_pointer(dp, keys, data_point_idx) if dp is not None else None
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
    # an EXPLICITLY declared IDTA polling interval pulls the affordance — the
    # transport-default push classification yields to the authored cadence
    # (e.g. a retained MQTT topic sampled by the DMP's timer route)
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
        polling_interval=_float(source.PollingInterval),
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
    dp = props.get(data_point) or acts.get(data_point) or evts.get(data_point)
    resolved.value_path = _schema_pointer(dp, keys, data_point_idx) if dp is not None else None
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
    for name, schema in names.items():
        t = getattr(schema, "type", None)
        value = getattr(t, "value", None)
        if value:
            out[name] = str(value).strip()
    return out


def _apply_declared_form_facts(resolved, dp, kind: str) -> None:
    """The DECLARED facts of one affordance instance onto its resolved
    endpoint: topic, the interactive reply topic, the MQTT delivery
    qualifiers (``mqv_qos``/``mqv_retain`` — wired into the compiled runtime
    config verbatim; absence stays absent, WoT defaults apply), the MQTT 5
    request/reply correlation terms (``mqv_request_reply`` /
    ``mqv_response_topic``) and the embedded input/output parameter types."""
    resolved.topic = _form_prop(dp, "href")
    declared_op = (_form_prop(dp, "op") or "").lower()
    if kind == ACTION_AFFORDANCE:
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
    elif kind == PROPERTY_AFFORDANCE and declared_op == "writeproperty":
        # a WRITABLE property may declare a command/reply pair (W2 Shape A:
        # the correlated ack on the declared response topic is the write's
        # ack) — same declared-facts contract as actions
        response_href = _declared_response_form(dp)
        if isinstance(response_href, str) and response_href.startswith("/"):
            resolved.response_topic = response_href.lstrip("/")
        rr = (_form_prop(dp, "mqv_request_reply") or "").strip().lower()
        resolved.request_reply = rr in ("true", "1")
        rt = _form_prop(dp, "mqv_response_topic")
        if isinstance(rt, str) and rt.strip():
            resolved.response_topic = rt.strip().lstrip("/")
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
    write_names: set[str] = set()
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
        for source in mc.Sources.value:
            verify_reference(source.Source.value,
                             context=f"mapping {_mapping_name(mc, index)} source {source.SourceId.value}")
        for sink in mc.Sinks.value:
            verify_reference(sink.Sink.value,
                             context=f"mapping {_mapping_name(mc, index)} sink {sink.SinkId.value}")
        operation = _operation_interaction(mc, aid, aid_for_anchor,
                                           response_order=order.get(_operation_action_name(mc)))
        if operation is not None:
            result.interactions.append(operation)
            continue
        write = _property_write(mc, aid, index, aid_for_anchor, write_names)
        if write is not None:
            write_names.add(write.id_short)
            result.property_writes.append(write)
            continue
        resolved_sources = [_resolve_source(s, aid, aid_for_anchor) for s in mc.Sources.value]
        for source in resolved_sources:
            if source.affordance_kind == ACTION_AFFORDANCE:
                raise ValueError(
                    f"mapping {_mapping_name(mc, index)} source {source.source_id}: "
                    "an action affordance requires an operation or write delegation"
                )
            if source.protocol in ("rest", "http", "modbus", "plc4x", "bacnet") \
                    and source.delivery == "async":
                raise ValueError(
                    f"mapping {_mapping_name(mc, index)} source {source.source_id}: "
                    f"{source.protocol} has no asynchronous event consumer"
                )
        sink_refs = [s.Sink.value for s in mc.Sinks.value]
        resolved_sinks = [_resolve_sink(s, aid, aid_for_anchor) for s in mc.Sinks.value]
        result.mappings.append(
            ResolvedMapping(
                id_short=_mapping_name(mc, index),
                sources=resolved_sources,
                sink_refs=sink_refs,
                sink_ids=[s.SinkId.value for s in mc.Sinks.value],
                resolved_sinks=resolved_sinks,
                transformation=_blob_text(mc.Transformation),
                transformation_engine=transformation_engine(mc),
                default_polling_interval=_float(mc.DefaultPollingInterval),
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


def transformation_engine(mc) -> str:
    """Engine selected by the Transformation blob's semantic id (ADR-015 / §7.6)."""
    tr = getattr(mc, "Transformation", None)
    if tr is None:
        return "LUA"
    sids = [getattr(tr, "semantic_id", None) or ""]
    sids += list(getattr(tr, "supplemental_semantic_ids", None) or [])
    for sid in sids:
        if str(sid).lower().startswith(JSONATA_SEMANTIC_NAMESPACE):
            return "JSONATA"
    return "LUA"


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
                           response_order: list[str] | None = None) -> ResolvedInteraction | None:
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
        # the CCI operation's positional output order (the awaited UA Call
        # zip's contract); absent → the readback/self-describing fallback
        response_order=list(response_order or []),
    )
    if len(operation_keys) > 1:
        resolved.unresolved.append(
            f"{action}: mapping targets {len(operation_keys)} Operation "
            "elements — one mapping per operation (split the sources)")
    if other_keys:
        resolved.unresolved.append(
            f"{action}: mapping mixes Operation-element and affordance sources "
            "— an operation delegation maps exactly one operation invocation")

    sink = _command_sink(mc, aid, aid_for_anchor)
    if sink is None:
        resolved.unresolved.append(
            f"{action}: no native command affordance sink (the sink must be an "
            "ACTION affordance on the AID's own interfaces, never an AAS "
            "property)")
    else:
        resolved.command_sink = sink
        resolved.protocol = sink.protocol
        # the DECLARED contract of the command affordance rides with the
        # interaction: MQTT 5 request/reply correlation, reply topic and the
        # embedded parameter types — the DMP parses values with them
        resolved.request_reply = bool(sink.request_reply)
        resolved.response_topic = sink.response_topic
        resolved.input_types = dict(sink.input_types)
        resolved.output_types = dict(sink.output_types)
        if sink.protocol == "mqtt" and not sink.response_topic:
            # the correlated reply leg rides the DECLARED response form of the
            # command affordance; there is nothing to rewrite, ever
            resolved.unresolved.append(
                f"{action}: command affordance {sink.data_point!r} declares no "
                "response form (set forms.response.href explicitly)")

    if resolved.request_blob is None:
        resolved.unresolved.append(f"{action}: mapping carries no Transformation "
                                   "request blob")

    if resolved.unresolved:
        logger.warning("interaction %s unresolved: %s", action,
                       "; ".join(resolved.unresolved))
    return resolved


def _write_delegation_qualifier(mc):
    """The ``writeDelegation`` ConceptQualifier carried by an AIMC mapping
    (W2 authoring), or ``None`` — its presence classifies the mapping as a
    property write-delegation instead of a data poll."""
    for qualifier in (getattr(mc, "qualifiers", None) or []):
        if (getattr(qualifier, "type_", "") or "").lower() == "writedelegation":
            return qualifier
    return None


def _write_name_from_device(device, index: int, taken: set[str] | None) -> str:
    """The delegation name for a write whose authored list-item idShort did
    not survive serialization: ``{data_point}_{protocol}`` from the device
    reference (the SubmodelElementList round-trip drops element names, AAS
    spec — interactions resolve the same way from their reference keys).
    Collisions (same data point bound twice on one protocol) get a stable
    index suffix."""
    name = f"{device.data_point}_{device.protocol or 'device'}"
    if not taken or name not in taken:
        return name
    candidate = f"{name}_{index}"
    n = index
    while candidate in taken:
        n += 1
        candidate = f"{name}_{n}"
    return candidate


def _property_write(mc, aid: DmpAssetInterfacesDescription, index: int = 0,
                    aid_for_anchor=None, taken: set[str] | None = None) \
        -> ResolvedPropertyWrite | None:
    """Classify a mapping as a property write-delegation (W2, Shape A) and
    resolve its legs.

    Requires the ``writeDelegation`` qualifier on the mapping; the SOURCE is
    the DEVICE affordance to write (any supported protocol — a writable UA
    variable, an MQTT command/reply property pair, an HTTP resource) and the
    SINK is the AAS property receiving the effective value after the device
    acknowledges. Fails loud instead of wiring a wrong endpoint."""
    qualifier = _write_delegation_qualifier(mc)
    if qualifier is None:
        return None

    resolved = ResolvedPropertyWrite(id_short=_mapping_name(mc, index))
    resolved.request_blob = _blob_text(mc.Transformation)
    resolved.response_blob = _blob_text(
        getattr(mc, "ResponseTransformation", None))

    qualifier_value = (getattr(qualifier, "value", "") or "")
    if "{delegation_base}" in qualifier_value:
        tail = qualifier_value.split("{delegation_base}", 1)[1]
        resolved.rest_path = tail if tail.startswith("/") else "/" + tail
    elif qualifier_value.startswith("/"):
        resolved.rest_path = qualifier_value

    # ── device leg: exactly one affordance source ─────────────────────────
    sources = mc.Sources.value
    if len(sources) != 1:
        resolved.unresolved.append(
            f"{resolved.id_short}: a write delegation maps exactly one device affordance "
            f"(found {len(sources)} sources)")
    else:
        device = _resolve_source(sources[0], aid, aid_for_anchor)
        resolved.device_source = device
        resolved.protocol = device.protocol
        # authored list-item names do not survive the SubmodelElementList
        # round-trip (AAS spec: list elements carry no idShort) — derive the
        # delegation name from the device reference
        if resolved.id_short.startswith("mapping_"):
            resolved.id_short = _write_name_from_device(device, index, taken)
        name = resolved.id_short
        if not device.protocol or device.protocol == "aas":
            resolved.unresolved.append(
                f"{name}: source does not resolve to a device affordance "
                f"(keys={reference_values(sources[0].Source.value)})")
        elif device.protocol == "mqtt":
            # the correlated reply leg IS the ack — without the declared
            # response form there is nothing to await
            if not device.response_topic:
                resolved.unresolved.append(
                    f"{name}: MQTT write affordance {device.data_point!r} "
                    "declares no response form (set forms.response.href "
                    "explicitly)")
            if resolved.request_blob is None or resolved.response_blob is None:
                resolved.unresolved.append(
                    f"{name}: MQTT write delegation needs a Transformation "
                    "request blob (command shape) AND a ResponseTransformation "
                    "blob (ack → effective value envelope)")
        elif device.protocol == "opcua":
            if not device.node:
                resolved.unresolved.append(
                    f"{name}: OPC UA write affordance {device.data_point!r} "
                    "carries no node information (uav_browsePath or §6.2 href)")
        elif device.protocol in ("rest", "http"):
            if not device.topic and not device.value_path:
                resolved.unresolved.append(
                    f"{name}: HTTP write affordance {device.data_point!r} "
                    "declares no href")

    name = resolved.id_short
    if resolved.request_blob is None and resolved.protocol != "mqtt":
        resolved.unresolved.append(
            f"{name}: mapping carries no Transformation request blob")

    # ── writeback leg: exactly one AAS-element sink ───────────────────────
    sinks = mc.Sinks.value
    if len(sinks) != 1:
        resolved.unresolved.append(
            f"{name}: a write delegation declares exactly one AAS writeback "
            f"sink (found {len(sinks)})")
    else:
        keys = reference_values(sinks[0].Sink.value)
        if any(k.startswith("interface_") for k in keys):
            resolved.unresolved.append(
                f"{name}: the writeback sink must be the AAS property "
                "(Variables/Parameters submodel), never a device affordance")
        elif not keys:
            resolved.unresolved.append(f"{name}: writeback sink has no reference")
        else:
            resolved.writeback_keys = keys
            resolved.writeback_sink_id = sinks[0].SinkId.value
            resolved.writeback_ref = sinks[0].Sink.value
            if resolved.rest_path is None:
                resolved.rest_path = \
                    f"/properties/{{aas_id_short}}/{keys[-1]}"

    if resolved.unresolved:
        logger.warning("property write %s unresolved: %s", name,
                       "; ".join(resolved.unresolved))
    return resolved


def _command_sink(mc, aid: DmpAssetInterfacesDescription,
                  aid_for_anchor=None):
    """The mapping's native command affordance sink: an ACTION affordance on
    one of the AID's own interfaces, any protocol. AAS-element sinks and
    property/event affordances are not command targets — the interaction
    stays unresolved rather than wiring a wrong endpoint."""
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
        return resolved
    return None
