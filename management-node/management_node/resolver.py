"""Reference resolution — links AIMC source/sink references to AID endpoints.

This is deliberately a SEPARATE step from the parsers: ``parsers.aid`` and
``parsers.aimc`` know nothing about each other; only here do the two models
meet. For every AIMC source the resolver finds the AID interface and data
point it points at and produces the concrete protocol endpoint the DMP needs
(base, topic/href, MQTT options, polling).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from .parsers.aid import AidForm, AidInterface, AidModel, MqttForm
from .parsers.aimc import AimcMapping, AimcModel, AimcReference, AimcSource

logger = logging.getLogger(__name__)


@dataclass
class ResolvedSource:
    """A concrete endpoint a DMP can consume, derived from AID + AIMC."""

    source_id: str
    protocol: str  # e.g. "mqtt", "http"
    interface: str  # AID interface name, e.g. "interface_mqtt"
    base: str | None = None  # endpoint base from AID EndpointMetadata
    topic: str | None = None  # href for MQTT (or path for other protocols)
    mqtt: MqttForm | None = None
    synchronous: bool | None = None
    polling_interval: float | None = None
    data_point: str | None = None  # AID data point name


@dataclass
class ResolvedMapping:
    """An AIMC mapping with its sources resolved against the AID."""

    id_short: str
    sources: list[ResolvedSource] = field(default_factory=list)
    sink_refs: list[AimcReference] = field(default_factory=list)
    sink_ids: list[str] = field(default_factory=list)
    transformation: str | None = None
    default_polling_interval: float | None = None

    @property
    def unresolved(self) -> list[str]:
        return [s.source_id for s in self.sources if not s.protocol]


@dataclass
class ResolvedBidirectionalMapping:
    """A resolved operation-delegation (request/response) mapping.

    REST side: the endpoint the DMP exposes. MQTT side: the native command/
    response topics plus the two directional Lua blobs.
    """

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


def _protocol_from_interface(interface: str) -> str:
    return interface.removeprefix("interface_").split("_")[0]


def _resolve_source(source: AimcSource, aid: AidModel) -> ResolvedSource:
    keys = source.source.as_path()
    # e.g. {aas_id}/submodels/AssetInterfacesDescription / interface_mqtt /
    #       InteractionMetadata / properties / StationState
    interface_name = next((k for k in keys if k.startswith("interface_")), None)
    data_point = keys[-1] if keys else None

    resolved = ResolvedSource(
        source_id=source.source_id,
        protocol="",
        interface=interface_name or "",
        polling_interval=source.polling_interval,
        data_point=data_point,
    )

    if not interface_name:
        logger.warning("source %s: no interface_* key in reference %s",
                       source.source_id, keys)
        return resolved

    interface: AidInterface | None = aid.interfaces.get(interface_name)
    if interface is None:
        logger.warning("source %s: AID interface %r not found",
                       source.source_id, interface_name)
        return resolved

    resolved.base = interface.base
    resolved.protocol = _protocol_from_interface(interface_name)

    # find the addressed data point (property or action)
    entry = None
    if data_point:
        entry = interface.properties.get(data_point) or interface.actions.get(data_point)
    if entry is None:
        logger.warning("source %s: AID data point %r not found in %s",
                       source.source_id, data_point, interface_name)
        return resolved

    resolved.synchronous = entry.synchronous if hasattr(entry, "synchronous") else None
    form = next((f for f in entry.forms if f.href), None)
    if form is not None:
        resolved.topic = form.href
        resolved.mqtt = form.mqtt

    return resolved


def resolve(aimc: AimcModel, aid: AidModel) -> ResolutionResult:
    """Resolve every mapping's sources against the asset's AID."""
    result = ResolutionResult(asset_id_short=aimc.id_short)
    for mapping in aimc.mappings:
        resolved_sources = [_resolve_source(s, aid) for s in mapping.sources]
        result.mappings.append(
            ResolvedMapping(
                id_short=mapping.id_short,
                sources=resolved_sources,
                sink_refs=[s.sink for s in mapping.sinks],
                sink_ids=[s.sink_id for s in mapping.sinks],
                transformation=mapping.transformation,
                default_polling_interval=mapping.default_polling_interval,
            )
        )
    result.bidirectional = _pair_bidirectional(aimc.mappings, aid)
    return result


def _pair_bidirectional(mappings: list[AimcMapping], aid: AidModel) -> list[ResolvedBidirectionalMapping]:
    """Group the directional mappings into request/response operation pairs.

    A skill's operation delegation is modeled as TWO plain IDTA
    MappingConfigurations; the direction is derived from the Source/Sink
    references:

    - request  = source ``interface_rest`` action → sink ``interface_mqtt`` action
    - response = source ``interface_mqtt`` action → sink ``interface_rest`` action

    Mappings with the same action name are paired into one operation.
    """
    requests: dict[str, AimcMapping] = {}
    responses: dict[str, AimcMapping] = {}
    for mapping in mappings:
        if not mapping.sources or not mapping.sinks:
            continue
        src_path = mapping.sources[0].source.as_path()
        snk_path = mapping.sinks[0].sink.as_path()
        if "interface_rest" in src_path and "interface_mqtt" in snk_path:
            action = src_path[-1] if src_path else mapping.id_short
            requests[action] = mapping
        elif "interface_mqtt" in src_path and "interface_rest" in snk_path:
            action = src_path[-1] if src_path else mapping.id_short
            responses[action] = mapping

    result = []
    for action, req in requests.items():
        result.append(_resolve_bidirectional_pair(action, req, responses.get(action), aid))
    return result


def _resolve_bidirectional_pair(
    action: str,
    req: AimcMapping,
    resp: AimcMapping | None,
    aid: AidModel,
) -> ResolvedBidirectionalMapping:
    """Resolve one request/response operation pair against the AID.

    The REST action (interface_rest) provides the exposed endpoint
    (base + path + method); the MQTT action (interface_mqtt) provides the
    native command topic (form href, e.g. /CMD/Halt) and the response topic
    (form response href, e.g. /DATA/Halt).
    """
    resolved = ResolvedBidirectionalMapping(id_short=action)

    rest_interface = aid.interfaces.get("interface_rest")
    mqtt_interface = aid.interfaces.get("interface_mqtt")

    rest_action = None
    mqtt_action = None
    if rest_interface is not None:
        rest_action = rest_interface.actions.get(action)
    if mqtt_interface is not None:
        mqtt_action = mqtt_interface.actions.get(action)

    if rest_interface is None or rest_action is None or not rest_interface.base:
        resolved.unresolved.append(f"{action}: REST action not found in AID")
    else:
        resolved.rest_base = rest_interface.base
        rest_form = next((f for f in rest_action.forms if f.href), None)
        resolved.rest_path = rest_form.href if rest_form else None
        resolved.rest_method = rest_form.method if rest_form and rest_form.method else "POST"

    if mqtt_interface is None or mqtt_action is None or not mqtt_interface.base:
        resolved.unresolved.append(f"{action}: MQTT action not found in AID")
    else:
        resolved.mqtt_base = mqtt_interface.base
        mqtt_form = next((f for f in mqtt_action.forms if f.href), None)
        if mqtt_form is not None:
            _, _, base_path = _split_mqtt_path(mqtt_interface.base)
            resolved.mqtt_command_topic = (base_path + mqtt_form.href).lstrip("/")
            response_href = None
            if mqtt_form.response is not None:
                href = mqtt_form.response.href
                if isinstance(href, str) and href.startswith("/"):
                    response_href = href
            if response_href is None:
                # derive from the command topic (/CMD/ -> /DATA/)
                response_href = mqtt_form.href.replace("/CMD/", "/DATA/")
            resolved.mqtt_response_topic = (base_path + response_href).lstrip("/")
        if not resolved.mqtt_command_topic:
            resolved.unresolved.append(f"{action}: no MQTT command topic")

    resolved.request_blob = req.transformation
    if resp is not None:
        resolved.response_blob = resp.transformation
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
