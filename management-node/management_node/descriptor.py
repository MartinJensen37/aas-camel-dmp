"""DMP descriptor compiler (SPEC §6.3, ADR-012).

Turns a :class:`~management_node.resolver.ResolutionResult` (AIMC + resolved
AID) into the config files the per-asset DMP container reads at startup:

- ``routes.json``             — one route per MappingConfiguration
- ``mqttconsumer.json``       — datasource configs (from resolved AID endpoints)
- ``luatransformers.json``    — LUA transformer configs (SPEC §8.5)
- ``queries/<asset>_<mapping>.lua`` — the AIMC transformation blobs
- ``aasserver.json``          — AAS datasink configs (from AIMC Sink refs)

The output is a dict of ``filename -> content``; the orchestrator writes it
into the asset's DMP config directory (mounted at ``/usr/share/config``).
"""

from __future__ import annotations

import base64
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from .resolver import ResolutionResult, ResolvedMapping, ResolvedSource

logger = logging.getLogger(__name__)

#: Default file names consumed by the DataBridge runtime (see databridge.camel-*).
ROUTES_FILE = "routes.json"
MQTT_CONSUMER_FILE = "mqttconsumer.json"
LUA_TRANSFORMERS_FILE = "luatransformers.json"
AAS_SERVER_FILE = "aasserver.json"

_MACRO = re.compile(r"\{[a-zA-Z0-9_:-]+\}")


def _substitute(value: str, aas_id: str, aas_id_short: str) -> str:
    """Resolve the {aas_id}/{aas_id_short} macros used in references."""
    return value.replace("{aas_id}", aas_id).replace("{aas_id_short}", aas_id_short)


def _split_mqtt_base(base: str | None) -> tuple[str | None, int | None, str]:
    """``mqtt://host:port/path`` -> (host, port, base_path)."""
    if not base:
        return None, None, ""
    m = re.match(r"^(?:mqtts?|tcp)://([^:/]+):?(\d*)(.*)$", base)
    if not m:
        return None, None, base
    host = m.group(1)
    port = int(m.group(2)) if m.group(2) else None
    path = m.group(3) or ""
    return host, port, path


def _sink_targets_variables(sink_ref) -> bool:
    """True when the sink reference targets the asset's ``Variables`` submodel
    (a writable data variable), not e.g. the AID action metadata."""
    path = sink_ref.as_path()
    return bool(path) and "/submodels/Variables" in path[0]


@dataclass
class DmpDescriptor:
    """Config files for one DMP container (1 DMP per asset, ADR-012)."""

    asset_id: str
    asset_id_short: str
    files: dict[str, str] = field(default_factory=dict)

    def write_to(self, directory) -> None:
        """Persist all files under the given config directory."""
        import pathlib

        root = pathlib.Path(directory)
        for name, content in self.files.items():
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        logger.info("wrote %d DMP config files to %s", len(self.files), root)


class DescriptorCompiler:
    def __init__(
        self,
        *,
        aas_base_url: str,
        aas_id: str | None = None,
        aas_id_short: str | None = None,
        mqtt_broker: tuple[str, int] | None = None,
    ) -> None:
        self.aas_base_url = aas_base_url.rstrip("/")
        self.aas_id = aas_id or "{aas_id}"
        self.aas_id_short = aas_id_short or "{aas_id_short}"
        # Optional (host, port) override for all MQTT sources: the AID base
        # may point at the physical lab broker (e.g. 192.168.0.104), while the
        # simulated stations publish to the local broker the DMPs can reach.
        self.mqtt_broker = mqtt_broker

    def compile(self, resolution: ResolutionResult) -> DmpDescriptor:
        descriptor = DmpDescriptor(
            asset_id=self.aas_id,
            asset_id_short=self.aas_id_short or resolution.asset_id_short,
        )

        routes: list[dict[str, Any]] = []
        mqtt_sources: list[dict[str, Any]] = []
        lua_transformers: list[dict[str, Any]] = []
        aas_sinks: list[dict[str, Any]] = []

        for mapping in resolution.mappings:
            route = self._compile_route(mapping, mqtt_sources, lua_transformers, aas_sinks, descriptor.files)
            if route:
                routes.append(route)

        if routes:
            descriptor.files[ROUTES_FILE] = json.dumps(routes, indent=2)
        if mqtt_sources:
            descriptor.files[MQTT_CONSUMER_FILE] = json.dumps(mqtt_sources, indent=2)
        if lua_transformers:
            descriptor.files[LUA_TRANSFORMERS_FILE] = json.dumps(lua_transformers, indent=2)
        if aas_sinks:
            descriptor.files[AAS_SERVER_FILE] = json.dumps(aas_sinks, indent=2)

        return descriptor

    # -- mapping -> route -------------------------------------------------- #
    def _compile_route(
        self,
        mapping: ResolvedMapping,
        mqtt_sources: list[dict[str, Any]],
        lua_transformers: list[dict[str, Any]],
        aas_sinks: list[dict[str, Any]],
        files: dict[str, str],
    ) -> dict[str, Any] | None:
        if not mapping.sources:
            logger.warning("mapping %s has no sources; skipping", mapping.id_short)
            return None

        source = mapping.sources[0]
        datasource_id = self._add_mqtt_source(source, mqtt_sources)
        if datasource_id is None:
            logger.warning("mapping %s: no supported datasource (protocol=%s); skipping",
                           mapping.id_short, source.protocol)
            return None

        transformer_id = None
        if mapping.transformation:
            transformer_id = self._add_lua_transformer(mapping, lua_transformers, files)

        # Only sinks on the asset's Variables submodel are writable AAS
        # properties (the DMP data-flow mapping). Skill mappings (REST
        # operation-delegation <-> MQTT action) reference AID action metadata
        # and need REST operation sinks, which are not implemented yet —
        # skip them so the DMP starts cleanly.
        variables_sink_refs = [
            (ref, sid)
            for ref, sid in zip(mapping.sink_refs, mapping.sink_ids)
            if _sink_targets_variables(ref)
        ]
        if len(variables_sink_refs) != len(mapping.sink_refs):
            logger.warning(
                "mapping %s: %d sink(s) target non-Variables submodels (REST "
                "operation-delegation not yet supported); skipping those",
                mapping.id_short, len(mapping.sink_refs) - len(variables_sink_refs))
        if not variables_sink_refs:
            logger.warning("mapping %s: no writable Variables sinks; skipping", mapping.id_short)
            return None

        sink_ids = []
        for sink_ref, sink_id in variables_sink_refs:
            sink_key = self._add_aas_sink(sink_ref, sink_id, aas_sinks)
            if sink_key:
                sink_ids.append(sink_key)

        if not sink_ids:
            logger.warning("mapping %s: no AAS sinks; skipping", mapping.id_short)
            return None

        route: dict[str, Any] = {
            "datasource": datasource_id,
            "transformers": [transformer_id] if transformer_id else [],
            "datasinks": sink_ids,
            # trigger matches the datasource kind: MQTT (and other event-driven
            # sources) use "event"; a real timer/polling source would use
            # "timer" (the AID `synchronous` flag is a REST semantics, not a
            # polling hint — it must not switch an MQTT source to timer).
            "trigger": "timer" if source.protocol == "timer" else "event",
        }
        if transformer_id and len(sink_ids) > 1:
            route["datasinkMappingConfiguration"] = {
                sink: [transformer_id] for sink in sink_ids
            }
        return route

    # -- sources ----------------------------------------------------------- #
    def _add_mqtt_source(
        self, source: ResolvedSource, mqtt_sources: list[dict[str, Any]]
    ) -> str | None:
        if source.protocol != "mqtt":
            return None

        host, port, base_path = _split_mqtt_base(source.base)
        if not host or not port:
            logger.warning("source %s: incomplete MQTT base %r", source.source_id, source.base)
            return None
        if self.mqtt_broker:
            host, port = self.mqtt_broker

        topic = source.topic or ""
        if topic and not topic.startswith("/"):
            topic = "/" + topic
        full_topic = (base_path.rstrip("/") + topic) if base_path else topic
        # MQTT topic of the AID base + href (e.g. NN/.../DATA/State): the base
        # path carries a leading '/' from the mqtt:// URL, but the broker topic
        # must not (the stations publish without one).
        full_topic = full_topic.lstrip("/")

        mqtt_sources.append(
            {
                "uniqueId": source.source_id,
                "serverUrl": host,
                "serverPort": port,
                "topic": full_topic,
            }
        )
        return source.source_id

    # -- transformers ------------------------------------------------------ #
    def _add_lua_transformer(
        self,
        mapping: ResolvedMapping,
        lua_transformers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> str:
        unique_id = f"{self.aas_id_short}_{mapping.id_short}_transformer"
        query_path = f"queries/{self.aas_id_short}_{mapping.id_short}.lua"

        # AIMC blobs address sources by SourceId (e.g. sources.StationState),
        # so the Lua component must key the incoming payload by that id rather
        # than the default "body".
        source_keys = mapping.sources[0].source_id if mapping.sources else "body"

        lua_transformers.append(
            {
                "uniqueId": unique_id,
                "queryLanguage": "LUA",
                "queryPath": query_path,
                "inputType": "JsonString",
                "outputType": "JsonString",
                "sourceKeys": source_keys,
            }
        )

        # the AIMC Transformation blob is the ONLY source of the Lua content
        # (ADR-011): materialize it as the DMP blob file
        if mapping.transformation:
            files[query_path] = mapping.transformation

        return unique_id

    # -- sinks ------------------------------------------------------------- #
    #: BaSyx 2.0 submodel-elements path separator used in idShortPath.
    _VALUE_PROPERTY = "variable"

    def _add_aas_sink(self, sink_ref, sink_id: str, aas_sinks: list[dict[str, Any]]) -> str | None:
        path = sink_ref.as_path()
        if not path:
            return None

        # reference keys: [Submodel=<aas_id>/submodels/<smId>, SMC=<propPath>...]
        submodel_id = _substitute(path[0], self.aas_id, self.aas_id_short)
        # BaSyx AAS 2.0 addresses submodels via UTF8-BASE64-URL-encoded id
        # (verified against the live aas-env: /submodels/<b64url>).
        b64 = base64.urlsafe_b64encode(submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
        submodel_endpoint = f"{self.aas_base_url}/submodels/{b64}"
        id_short_path = "/".join(path[1:]) if len(path) > 1 else path[0]
        # The AIMC SinkId points at a SubmodelElementCollection; the actual
        # writable value lives in its `variable` property (apex model).
        if id_short_path:
            id_short_path = f"{id_short_path}.{self._VALUE_PROPERTY}"

        unique_id = f"{self.aas_id_short}_{sink_id}_sink"
        aas_sinks.append(
            {
                "uniqueId": unique_id,
                "type": "PROPERTY",
                "submodelEndpoint": submodel_endpoint,
                "idShortPath": id_short_path,
                "api": "DotAasV3",
                # the AIMC blob returns {SinkId: value, ...}; each sink writes
                # only its own SinkId value out of that object
                "valuePath": sink_id,
            }
        )
        return unique_id


def compile_descriptor(
    resolution: ResolutionResult,
    *,
    aas_base_url: str,
    aas_id: str | None = None,
    aas_id_short: str | None = None,
    mqtt_broker: tuple[str, int] | None = None,
) -> DmpDescriptor:
    """Convenience factory for :class:`DescriptorCompiler`."""
    return DescriptorCompiler(
        aas_base_url=aas_base_url, aas_id=aas_id, aas_id_short=aas_id_short,
        mqtt_broker=mqtt_broker,
    ).compile(resolution)
