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

from .resolver import ResolutionResult, ResolvedMapping, ResolvedSource, reference_values

logger = logging.getLogger(__name__)

#: Default file names consumed by the DataBridge runtime (see databridge.camel-*).
ROUTES_FILE = "routes.json"
MQTT_CONSUMER_FILE = "mqttconsumer.json"
MQTT_SINK_FILE = "mqttdatasink.json"
LUA_TRANSFORMERS_FILE = "luatransformers.json"
JSONATA_TRANSFORMERS_FILE = "jsonatatransformer.json"
AAS_SERVER_FILE = "aasserver.json"
AAS_POLLING_CONSUMER_FILE = "aaspollingconsumer.json"
OPCUA_CONSUMER_FILE = "opcuaconsumer.json"
PLC4X_CONSUMER_FILE = "plc4xconsumer.json"
HTTP_CONSUMER_FILE = "httpconsumer.json"
TIMER_CONSUMER_FILE = "timerconsumer.json"
REST_SOURCES_FILE = "restsources.json"

_MACRO = re.compile(r"\{[a-zA-Z0-9_:-]+\}")


def _substitute(value: str, aas_id: str, aas_id_short: str) -> str:
    """Resolve the {aas_id}/{aas_id_short} macros used in references."""
    return value.replace("{aas_id}", aas_id).replace("{aas_id_short}", aas_id_short)


def _split_mqtt_base(base: str | None) -> tuple[str | None, int | None, str]:
    """``mqtt://host:port/path`` -> (host, port, base_path)."""
    host, port, path = _parse_endpoint(base)
    return host, port, path


def _parse_endpoint(base: str | None) -> tuple[str | None, int | None, str]:
    """Parses ``<scheme>://host[:port][/path]`` for any scheme (mqtt, tcp,
    opc.tcp, http, https, ...), returning (host, port, path)."""
    if not base:
        return None, None, ""
    m = re.match(r"^(?:[a-zA-Z][a-zA-Z0-9+.-]*)://([^:/]+)(?::(\d+))?(/.*)?$", base)
    if not m:
        return None, None, base
    host = m.group(1)
    port = int(m.group(2)) if m.group(2) else None
    path = m.group(3) or ""
    return host, port, path


def _is_variables_submodel(ref: str) -> bool:
    """True when a submodel reference names the asset's ``Variables`` submodel
    (the apex-model leaf convention uses a ``variable`` property under each
    SubmodelElementCollection)."""
    return "/submodels/Variables" in ref


def _interval_ms(seconds: float | None) -> str:
    """Polling interval in seconds -> millisecond string (OPC UA uses ms)."""
    return str(int((seconds or 5.0) * 1000))


def _is_operation_mapping(mapping: ResolvedMapping) -> bool:
    """True when the mapping is an operation-delegation mapping (REST <-> MQTT
    action) that is compiled as an operation by ``_compile_operations``, not a
    data route. A bidirectional skill is expressed as two directional mappings
    pairing an ``interface_rest`` action with an ``interface_mqtt`` action."""
    if not mapping.sources or not mapping.sink_refs:
        return False
    src_interface = mapping.sources[0].interface
    snk_path = reference_values(mapping.sink_refs[0])
    return (
        (src_interface == "interface_rest" and "interface_mqtt" in snk_path)
        or (src_interface == "interface_mqtt" and "interface_rest" in snk_path)
    )


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
        mqtt_sinks: list[dict[str, Any]] = []
        rest_sources: list[dict[str, Any]] = []
        lua_transformers: list[dict[str, Any]] = []
        jsonata_transformers: list[dict[str, Any]] = []
        aas_sinks: list[dict[str, Any]] = []
        aas_polling_sources: list[dict[str, Any]] = []
        opcua_sources: list[dict[str, Any]] = []
        plc4x_sources: list[dict[str, Any]] = []
        http_sources: list[dict[str, Any]] = []
        timers: list[dict[str, Any]] = []

        for mapping in resolution.mappings:
            route = self._compile_route(
                mapping, mqtt_sources, mqtt_sinks, lua_transformers,
                jsonata_transformers, aas_sinks, aas_polling_sources,
                opcua_sources, plc4x_sources, http_sources, timers,
                descriptor.files)
            if route:
                routes.append(route)

        # Operation-delegation routes: REST endpoint as datasource, native MQTT
        # command as datasink, request/response Lua blobs as transformers. The
        # REST endpoints are sources/sinks of these routes and are asset-
        # dependent — only present when the asset's AIMC declares the operation.
        self._compile_operations(
            resolution, routes, mqtt_sources, mqtt_sinks, rest_sources,
            lua_transformers, descriptor.files)

        if routes:
            descriptor.files[ROUTES_FILE] = json.dumps(routes, indent=2)
        if mqtt_sources:
            descriptor.files[MQTT_CONSUMER_FILE] = json.dumps(mqtt_sources, indent=2)
        if mqtt_sinks:
            descriptor.files[MQTT_SINK_FILE] = json.dumps(mqtt_sinks, indent=2)
        if rest_sources:
            descriptor.files[REST_SOURCES_FILE] = json.dumps(rest_sources, indent=2)
        if lua_transformers:
            descriptor.files[LUA_TRANSFORMERS_FILE] = json.dumps(lua_transformers, indent=2)
        if jsonata_transformers:
            descriptor.files[JSONATA_TRANSFORMERS_FILE] = json.dumps(jsonata_transformers, indent=2)
        if aas_sinks:
            descriptor.files[AAS_SERVER_FILE] = json.dumps(aas_sinks, indent=2)
        if aas_polling_sources:
            descriptor.files[AAS_POLLING_CONSUMER_FILE] = json.dumps(aas_polling_sources, indent=2)
        if opcua_sources:
            descriptor.files[OPCUA_CONSUMER_FILE] = json.dumps(opcua_sources, indent=2)
        if plc4x_sources:
            descriptor.files[PLC4X_CONSUMER_FILE] = json.dumps(plc4x_sources, indent=2)
        if http_sources:
            descriptor.files[HTTP_CONSUMER_FILE] = json.dumps(http_sources, indent=2)
        if timers:
            descriptor.files[TIMER_CONSUMER_FILE] = json.dumps(timers, indent=2)

        return descriptor

    # -- mapping -> route -------------------------------------------------- #
    def _compile_route(
        self,
        mapping: ResolvedMapping,
        mqtt_sources: list[dict[str, Any]],
        mqtt_sinks: list[dict[str, Any]],
        lua_transformers: list[dict[str, Any]],
        jsonata_transformers: list[dict[str, Any]],
        aas_sinks: list[dict[str, Any]],
        aas_polling_sources: list[dict[str, Any]],
        opcua_sources: list[dict[str, Any]],
        plc4x_sources: list[dict[str, Any]],
        http_sources: list[dict[str, Any]],
        timers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> dict[str, Any] | None:
        if not mapping.sources:
            logger.warning("mapping %s has no sources; skipping", mapping.id_short)
            return None

        # An operation-delegation (REST <-> MQTT action) mapping is compiled as
        # an operation by _compile_operations, not as a data route.
        if _is_operation_mapping(mapping):
            logger.debug("mapping %s is an operation-delegation mapping; "
                         "compiled as an operation", mapping.id_short)
            return None

        # Delivery style is classified per source from the AID (protocol-
        # agnostic, SPEC §6.1): "async" sources push events; "sync" sources are
        # polled. This is what the route trigger + fan-in strategy key on.
        sync = [s for s in mapping.sources if s.delivery == "sync"]
        event = [s for s in mapping.sources if s.delivery == "async"]
        unresolved = [s for s in mapping.sources if not s.protocol]

        if unresolved:
            logger.warning("mapping %s: unresolved sources %s; skipping",
                           mapping.id_short, [s.source_id for s in unresolved])
            return None

        transformer_id = self._resolve_transformer(mapping, lua_transformers, jsonata_transformers, files)

        # Sinks: AAS submodel elements (any submodel, use-case 2, R2) are AAS
        # sinks; AID MQTT-interface sinks are protocol data sinks; other
        # protocol sinks are skipped (only MQTT protocol sinks and AAS element
        # sinks are implementable in the data plane today).
        aas_sink_refs: list[tuple[Any, str]] = []
        mqtt_sink_entries: list[tuple[Any, str]] = []
        for sink_ref, sink_id in zip(mapping.sink_refs, mapping.sink_ids):
            resolved = next((s for s in mapping.resolved_sinks if s.sink_id == sink_id), None)
            if resolved is not None and resolved.aas:
                aas_sink_refs.append((sink_ref, sink_id))
            elif resolved is not None and resolved.protocol == "mqtt":
                mqtt_sink_entries.append((resolved, sink_id))
            else:
                logger.debug("mapping %s: sink %s targets an unsupported sink "
                             "(protocol=%s); skipping", mapping.id_short, sink_id,
                             resolved.protocol if resolved else "?")

        if sync:
            # Synchronous source(s) — only supported as a single source per
            # mapping for now (the unified timer-driven fan-in aggregator for
            # multiple / mixed sync+event sources is the next increment).
            if len(sync) != len(mapping.sources):
                logger.warning("mapping %s: mixed sync/event sources for a single "
                               "route are not supported yet; skipping", mapping.id_short)
                return None
            return self._compile_timer_route(
                mapping, sync[0], transformer_id, aas_sink_refs,
                mqtt_sink_entries, aas_sinks, mqtt_sinks,
                aas_polling_sources, opcua_sources, plc4x_sources,
                http_sources, timers)

        # All async sources -> event route (fan-in).
        if not event:
            logger.warning("mapping %s: no supported datasource; skipping", mapping.id_short)
            return None
        sink_ids = self._compile_sinks(
            aas_sink_refs, mqtt_sink_entries, aas_sinks, mqtt_sinks)
        if not sink_ids:
            logger.warning("mapping %s: no sink endpoints; skipping", mapping.id_short)
            return None

        datasource_ids = [self._add_datasource(
            src, mqtt_sources, aas_polling_sources, opcua_sources,
            plc4x_sources, http_sources) for src in event]
        datasource_ids = [d for d in datasource_ids if d]

        route: dict[str, Any] = {
            "transformers": [transformer_id] if transformer_id else [],
            "datasinks": sink_ids,
            "trigger": "event",
        }
        if len(datasource_ids) == 1:
            route["datasource"] = datasource_ids[0]
        else:
            route["datasources"] = datasource_ids
        if transformer_id and len(sink_ids) > 1:
            route["datasinkMappingConfiguration"] = {
                sink: [transformer_id] for sink in sink_ids
            }
        return route

    def _compile_sinks(
        self,
        aas_sink_refs: list[tuple[Any, str]],
        mqtt_sink_entries: list[tuple[Any, str]],
        aas_sinks: list[dict[str, Any]],
        mqtt_sinks: list[dict[str, Any]],
    ) -> list[str]:
        """Compiles AAS-element and MQTT-interface sinks, returning sink ids."""
        sink_ids: list[str] = []
        for sink_ref, sink_id in aas_sink_refs:
            sink_key = self._add_aas_sink(sink_ref, sink_id, aas_sinks)
            if sink_key:
                sink_ids.append(sink_key)
        for resolved, sink_id in mqtt_sink_entries:
            sink_key = self._add_mqtt_datasink(resolved, sink_id, mqtt_sinks)
            if sink_key:
                sink_ids.append(sink_key)
        return sink_ids

    def _resolve_transformer(
        self,
        mapping: ResolvedMapping,
        lua_transformers: list[dict[str, Any]],
        jsonata_transformers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> str | None:
        """Emits the transformer for the mapping: the declared engine when a
        Transformation blob is present, otherwise a passthrough identity Lua
        blob (R3 — AIMC: SourceId links to SinkId when no transformation)."""
        if mapping.transformation:
            return self._add_transformer(
                mapping, lua_transformers, jsonata_transformers, files)
        if mapping.sink_ids:
            return self._passthrough_transformer(mapping, lua_transformers, files)
        return None

    def _compile_timer_route(
        self,
        mapping: ResolvedMapping,
        source: ResolvedSource,
        transformer_id: str | None,
        aas_sink_refs: list[tuple[Any, str]],
        mqtt_sink_entries: list[tuple[Any, str]],
        aas_sinks: list[dict[str, Any]],
        mqtt_sinks: list[dict[str, Any]],
        aas_polling_sources: list[dict[str, Any]],
        opcua_sources: list[dict[str, Any]],
        plc4x_sources: list[dict[str, Any]],
        http_sources: list[dict[str, Any]],
        timers: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """Compiles a single synchronous (polling) source into a timer route.

        The source becomes a polling consumer (aas/opcua/plc4x/http) and the
        route polls it on a timer whose period is the AIMC polling interval
        (DefaultPollingInterval or per-source PollingInterval).
        """
        datasource_id = self._add_datasource(
            source, [], aas_polling_sources, opcua_sources, plc4x_sources,
            http_sources, mapping.default_polling_interval)
        if datasource_id is None:
            logger.warning("mapping %s: unresolvable synchronous source "
                           "(protocol=%s); skipping", mapping.id_short, source.protocol)
            return None

        timer_id = self._add_timer(
            mapping, source.polling_interval or mapping.default_polling_interval, timers)

        sink_ids = self._compile_sinks(aas_sink_refs, mqtt_sink_entries, aas_sinks, mqtt_sinks)
        if not sink_ids:
            logger.warning("mapping %s: no sink endpoints; skipping", mapping.id_short)
            return None

        return {
            "datasource": datasource_id,
            "transformers": [transformer_id] if transformer_id else [],
            "datasinks": sink_ids,
            "trigger": "timer",
            "triggerData": {"timerName": timer_id},
        }

    # -- sources ----------------------------------------------------------- #
    def _add_datasource(
        self,
        source: ResolvedSource,
        mqtt_sources: list[dict[str, Any]],
        aas_polling_sources: list[dict[str, Any]],
        opcua_sources: list[dict[str, Any]],
        plc4x_sources: list[dict[str, Any]],
        http_sources: list[dict[str, Any]],
        default_polling_interval: float | None = None,
    ) -> str | None:
        """Dispatches a resolved source to its per-protocol config builder.

        Protocol-agnostic: the caller routes on ``source.delivery`` (sync vs
        async, SPEC §6.1), not on the protocol string — the same builder serves
        both an event (async) and a timer (sync) route."""
        p = source.protocol
        if p == "mqtt":
            return self._add_mqtt_source(source, mqtt_sources)
        if p == "aas":
            return self._add_aas_source(source, default_polling_interval, aas_polling_sources)
        if p == "opcua":
            return self._add_opcua_source(source, opcua_sources)
        if p in ("modbus", "plc4x"):
            return self._add_plc4x_source(source, plc4x_sources)
        if p == "http":
            return self._add_http_source(source, http_sources)
        logger.warning("source %s: no datasource builder for protocol %r",
                       source.source_id, p)
        return None

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
        if source.value_path:
            # fine source (ADR-014 / SPEC §6.3): the DMP disassembles the
            # payload to this JSON Pointer leaf before placing it under SourceId
            mqtt_sources[-1]["valuePath"] = source.value_path
        return source.source_id

    def _add_timer(
        self,
        mapping: ResolvedMapping,
        interval: float | None,
        timers: list[dict[str, Any]],
    ) -> str:
        """Adds a timer config driving a polling route, with the AIMC polling
        interval (seconds) as the period."""
        period_ms = int((interval or 5.0) * 1000)
        timer_id = f"{self.aas_id_short}_{mapping.id_short}_timer"
        timers.append(
            {
                "uniqueId": timer_id,
                "fixedRate": True,
                "delay": 1000,
                "period": max(period_ms, 1000),
            }
        )
        return timer_id

    def _add_aas_source(
        self,
        source: ResolvedSource,
        default_interval: float | None,
        aas_polling_sources: list[dict[str, Any]],
    ) -> str | None:
        """R1 — an AAS-element source (case 2, AAS->AAS) becomes an AAS polling
        consumer over the referenced submodel element."""
        path = source.ref_path
        if not path:
            return None
        submodel_id = _substitute(path[0], self.aas_id, self.aas_id_short)
        b64 = base64.urlsafe_b64encode(submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
        submodel_endpoint = f"{self.aas_base_url}/submodels/{b64}"
        id_short_path = "/".join(path[1:]) if len(path) > 1 else ""
        # only the asset's Variables submodel uses the apex `.variable` leaf.
        if _is_variables_submodel(path[0]) and id_short_path:
            id_short_path = f"{id_short_path}.{self._VALUE_PROPERTY}"

        unique_id = source.source_id
        aas_polling_sources.append(
            {
                "uniqueId": unique_id,
                "submodelEndpoint": submodel_endpoint,
                "idShortPath": id_short_path,
                "api": "DotAasV3",
            }
        )
        return unique_id

    def _add_opcua_source(
        self,
        source: ResolvedSource,
        opcua_sources: list[dict[str, Any]],
    ) -> str | None:
        """R4 — an OPC UA interface data point becomes an OPC UA polling
        consumer (milo-client)."""
        host, port, base_path = _split_mqtt_base(source.base)
        if not host or not port:
            return None
        entry: dict[str, Any] = {
            "uniqueId": source.source_id,
            "serverUrl": host,
            "serverPort": port,
            "pathToService": base_path or "/opcua/server",
            "nodeInformation": source.node or (source.topic or ""),
            "username": "",
            "password": "",
            "parameters": {
                "requestedPublishingInterval": _interval_ms(source.polling_interval),
            },
        }
        opcua_sources.append(entry)
        return source.source_id

    def _add_plc4x_source(
        self,
        source: ResolvedSource,
        plc4x_sources: list[dict[str, Any]],
    ) -> str | None:
        """R4 — a Modbus/PLC interface data point becomes a PLC4X polling
        consumer."""
        host, port, _ = _split_mqtt_base(source.base)
        if not host or not port:
            return None
        driver = "modbus-tcp" if source.protocol in ("modbus", "plc4x") else source.protocol
        address = source.topic or source.modbus_entity or ""
        entry: dict[str, Any] = {
            "uniqueId": source.source_id,
            "serverUrl": host,
            "serverPort": port,
            "driver": driver,
            "servicePath": "",
            "options": "",
            "tags": [{"name": source.data_point or source.source_id, "value": address}],
        }
        plc4x_sources.append(entry)
        return source.source_id

    def _add_http_source(
        self,
        source: ResolvedSource,
        http_sources: list[dict[str, Any]],
    ) -> str | None:
        """R4 — a synchronous HTTP interface data point becomes an HTTP polling
        consumer (serverUrl = base + href)."""
        if not source.base:
            return None
        url = source.base.rstrip("/") + (source.topic or "")
        entry: dict[str, Any] = {
            "uniqueId": source.source_id,
            "serverUrl": url,
        }
        http_sources.append(entry)
        return source.source_id

    # -- transformers ------------------------------------------------------ #
    def _add_transformer(
        self,
        mapping: ResolvedMapping,
        lua_transformers: list[dict[str, Any]],
        jsonata_transformers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> str | None:
        """Emits the transformer config for the mapping's engine (ADR-015).

        The engine is selected by the AIMC Transformation blob's semantic id
        (SPEC §7.6): the AIMC-compliant Lua path by default, JSONata when the
        blob declares a JSONata semantic id. Both share the same dis/assembly
        pipeline and the same SourceId/SinkId keyed contract (SPEC §8.4 #11);
        the DMP instantiates ``lua:`` or ``jsonata:`` from the config."""
        engine = mapping.transformation_engine or "LUA"
        if engine == "JSONATA":
            return self._add_jsonata_transformer(mapping, jsonata_transformers, files)
        return self._add_lua_transformer(mapping, lua_transformers, files)

    def _add_jsonata_transformer(
        self,
        mapping: ResolvedMapping,
        jsonata_transformers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> str:
        """JSONata engine (ADR-015 / SPEC §7.6): a fast/simple transformation.

        The JSONata expression is the blob content, materialized as a
        ``.jsonata`` file; both engines receive the same incoming body (the
        SourceId-keyed ``sources`` map, or the single payload)."""
        unique_id = f"{self.aas_id_short}_{mapping.id_short}_transformer"
        query_path = f"queries/{self.aas_id_short}_{mapping.id_short}.jsonata"

        jsonata_transformers.append(
            {
                "uniqueId": unique_id,
                "queryLanguage": "JSONATA",
                "queryPath": query_path,
                "inputType": "JsonString",
                "outputType": "JsonString",
            }
        )

        if mapping.transformation:
            files[query_path] = mapping.transformation

        return unique_id

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
        # than the default "body". For a fan-in mapping, every SourceId must be
        # listed (comma-separated) so the aggregated {SourceId: payload} map is
        # disassembled into the aimc_main(sources) table (SPEC §6.4).
        source_keys = ",".join(s.source_id for s in mapping.sources) if mapping.sources else "body"

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

    def _passthrough_transformer(
        self,
        mapping: ResolvedMapping,
        lua_transformers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> str:
        """R3 — passthrough (identity) transform when the mapping declares NO
        Transformation blob.

        The AIMC template: SourceId establishes a relationship to a
        corresponding sink when no transformation is given. Emits a Lua blob
        that copies each SinkId from its matching SourceId (or positionally)."""
        sources = [s.source_id for s in mapping.sources]
        lines = []
        for i, sink_id in enumerate(mapping.sink_ids):
            src = sink_id if sink_id in sources else (sources[i] if i < len(sources) else None)
            if src is None:
                continue
            lines.append(f'        ["{sink_id}"] = sources["{src}"],')

        blob = ("function aimc_main(sources)\n"
                "    return {\n"
                + "\n".join(lines)
                + "\n    }\n"
                "end\n")

        unique_id = f"{self.aas_id_short}_{mapping.id_short}_passthrough"
        query_path = f"queries/{self.aas_id_short}_{mapping.id_short}_passthrough.lua"
        lua_transformers.append(
            {
                "uniqueId": unique_id,
                "queryLanguage": "LUA",
                "queryPath": query_path,
                "inputType": "JsonString",
                "outputType": "JsonString",
                "sourceKeys": ",".join(sources) if sources else "body",
            }
        )
        files[query_path] = blob
        return unique_id

    # -- operations (REST <-> MQTT operation delegation) ------------------- #
    def _compile_operations(
        self,
        resolution: ResolutionResult,
        routes: list[dict[str, Any]],
        mqtt_sources: list[dict[str, Any]],
        mqtt_sinks: list[dict[str, Any]],
        rest_sources: list[dict[str, Any]],
        lua_transformers: list[dict[str, Any]],
        files: dict[str, str],
    ) -> None:
        """Compile the bidirectional (request/response) mappings into routes.

        Each bidirectional AIMC mapping becomes an operation route whose REST
        endpoint is a datasource and whose native MQTT command is a datasink
        (with the request/response Lua blobs as transformers). The DMP exposes
        ``restPath`` via camel-rest and, on invocation, publishes the request
        blob result to the MQTT command topic, waits for the correlated reply
        on the response topic, runs the response blob and answers the HTTP
        request (SPEC §10.x / M7).

        The routes/entities are asset-dependent: only emitted for assets whose
        AIMC declares the corresponding operation mapping."""
        for bidi in resolution.bidirectional:
            if bidi.unresolved:
                logger.warning(
                    "operation %s unresolved (%s); skipping",
                    bidi.id_short, "; ".join(bidi.unresolved))
                continue
            if not bidi.rest_path or not bidi.mqtt_command_topic or not bidi.mqtt_response_topic:
                logger.warning("operation %s: missing endpoint; skipping", bidi.id_short)
                continue

            host, port, _ = _split_mqtt_base(bidi.mqtt_base)
            if not host or not port:
                logger.warning("operation %s: incomplete MQTT base %r", bidi.id_short, bidi.mqtt_base)
                continue
            if self.mqtt_broker:
                host, port = self.mqtt_broker

            action = bidi.id_short
            asset = self.aas_id_short
            method = (bidi.rest_method or "POST").upper()

            rest_source_id = f"{asset}_{action}_rest"
            request_transformer_id = f"{asset}_{action}_request_transformer"
            command_sink_id = f"{asset}_{action}_command_sink"
            response_source_id = f"{asset}_{action}_response_source"
            response_transformer_id = f"{asset}_{action}_response_transformer"
            route_id = f"{asset}_{action}_operation"

            request_path = f"queries/{asset}_{action}_request.lua"
            response_path = f"queries/{asset}_{action}_response.lua"

            # REST datasource (the operation's exposed endpoint).
            rest_sources.append({
                "uniqueId": rest_source_id,
                "restMethod": method,
                "restPath": bidi.rest_path,
                "contentType": "application/json",
            })

            # Request-direction Lua transformer (REST operation variables ->
            # native MQTT command message).
            lua_transformers.append({
                "uniqueId": request_transformer_id,
                "queryLanguage": "LUA",
                "queryPath": request_path,
                "inputType": "JsonString",
                "outputType": "JsonString",
                "sourceKeys": action,
            })
            # Response-direction Lua transformer (native MQTT response -> REST).
            lua_transformers.append({
                "uniqueId": response_transformer_id,
                "queryLanguage": "LUA",
                "queryPath": response_path,
                "inputType": "JsonString",
                "outputType": "JsonString",
                "sourceKeys": action,
            })

            # Native MQTT command topic datasink (paho producer).
            mqtt_sinks.append({
                "uniqueId": command_sink_id,
                "serverUrl": host,
                "serverPort": port,
                "topic": bidi.mqtt_command_topic,
                "clientId": f"dmp-{asset}-{action}-cmd",
                "userName": "",
                "password": "",
            })
            # Native MQTT response topic datasource (paho consumer) for the
            # Uuid-correlated reply.
            mqtt_sources.append({
                "uniqueId": response_source_id,
                "serverUrl": host,
                "serverPort": port,
                "topic": bidi.mqtt_response_topic,
                "clientId": f"dmp-{asset}-{action}-resp",
            })

            # The operation route: REST datasource, request Lua transformer,
            # command datasink; the response half lives in triggerData.
            routes.append({
                "routeId": route_id,
                "trigger": "operation",
                "datasource": rest_source_id,
                "transformers": [request_transformer_id],
                "datasinks": [command_sink_id],
                "triggerData": {
                    "responseDatasource": response_source_id,
                    "responseTransformer": response_transformer_id,
                    "timeoutSeconds": 30,
                },
            })

            if bidi.request_blob:
                files[request_path] = bidi.request_blob
            if bidi.response_blob:
                files[response_path] = bidi.response_blob

    # -- sinks ------------------------------------------------------------- #
    #: BaSyx 2.0 submodel-elements path separator used in idShortPath.
    _VALUE_PROPERTY = "variable"

    def _add_mqtt_datasink(
        self,
        sink,
        sink_id: str,
        mqtt_sinks: list[dict[str, Any]],
    ) -> str | None:
        """Registers an MQTT datasink (paho producer) from a resolved AID sink.

        Coarse (no ``value_path``): the DMP publishes the whole ``SinkId`` value
        from the transform's return object. Fine (ADR-014 / SPEC §6.5.4): the
        DMP constructs the outgoing payload with the value at the pinned JSON
        Pointer (``valuePath``).
        """
        if sink.protocol != "mqtt" or not sink.base:
            return None

        host, port, base_path = _split_mqtt_base(sink.base)
        if not host or not port:
            logger.warning("sink %s: incomplete MQTT base %r", sink_id, sink.base)
            return None
        if self.mqtt_broker:
            host, port = self.mqtt_broker

        topic = sink.topic or ""
        if topic and not topic.startswith("/"):
            topic = "/" + topic
        full_topic = (base_path.rstrip("/") + topic) if base_path else topic
        full_topic = full_topic.lstrip("/")

        unique_id = f"{self.aas_id_short}_{sink_id}_sink"
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "serverUrl": host,
            "serverPort": port,
            "topic": full_topic,
            "clientId": f"dmp-{self.aas_id_short}-{sink_id}",
            "userName": "",
            "password": "",
            # each sink consumes only its own SinkId value out of the return object
            "sinkId": sink_id,
        }
        if sink.value_path:
            entry["valuePath"] = sink.value_path
        mqtt_sinks.append(entry)
        return unique_id

    def _add_aas_sink(self, sink_ref, sink_id: str, aas_sinks: list[dict[str, Any]]) -> str | None:
        path = reference_values(sink_ref)
        if not path:
            return None

        # reference keys: [Submodel=<aas_id>/submodels/<smId>, SMC=<propPath>...]
        submodel_id = _substitute(path[0], self.aas_id, self.aas_id_short)
        # BaSyx AAS 2.0 addresses submodels via UTF8-BASE64-URL-encoded id
        # (verified against the live aas-env: /submodels/<b64url>).
        b64 = base64.urlsafe_b64encode(submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
        submodel_endpoint = f"{self.aas_base_url}/submodels/{b64}"
        id_short_path = "/".join(path[1:]) if len(path) > 1 else path[0]
        # The asset's Variables submodel uses the apex model: each SinkId
        # SubmodelElementCollection holds its writable value in a `variable`
        # property. Other (case-2) submodels are addressed directly.
        if _is_variables_submodel(path[0]) and len(path) > 1:
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
