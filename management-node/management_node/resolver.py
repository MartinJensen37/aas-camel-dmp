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
class ResolutionResult:
    """All mappings of an asset, with sources resolved against its AID."""

    asset_id_short: str
    mappings: list[ResolvedMapping] = field(default_factory=list)

    @property
    def all_resolved(self) -> bool:
        return all(not m.unresolved for m in self.mappings)


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
    return result
