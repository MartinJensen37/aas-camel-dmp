"""DMP descriptor compiler (SPEC §6.3, ADR-012, ADR-017, ADR-021).

Turns a :class:`~management_node.resolver.ResolutionResult` (AIMC + resolved
AID) into the config files the per-unit DMP container reads at startup:

- ``routes.json``             — one aggregation route per MappingConfiguration
                                plus one bridge route per interaction pairing
- ``mqttdatasource.json``       — MQTT datasource configs (from declared endpoints)
- ``luatransformer.json``    — LUA transformer configs (SPEC §8.5)
- ``queries/<unit>_<mapping>.lua`` — the AIMC transformation blobs
- ``aasdatasink.json``          — AAS datasink configs (from AIMC Sink refs)
- (plus opcua/plc4x/http/aas-polling consumer files when such sources exist)
- ``kafkadatasource.json`` — AAS repository event datasource configs

Every mapping compiles to ONE uniform shape: ``sources[] + sinks[] +
transformer(s)`` — the aggregation route (fixed DMP runtime semantic,
decision 2026-09-15: evaluate on any source update once ALL sources have
delivered; sync sources are polled at their declared IDTA interval).
Interactive mappings instead compile to a ProtocolBridge route. The legacy
singular ``datasource`` key and the timer-consumer file are gone — plural
always.

Identifiers (ADR-017/021): AIMC scopes SourceId/SinkId per MappingConfiguration,
but every DMP consumer resolves entity ids globally. The compiler qualifies all
DMP-internal uniqueIds with ``{unit}_{mapping}_`` where ``{unit}`` derives from
the AIMC SM instance id; each data-source entry carries a stable, distinct
``clientId = dmp-{unit}_{mapping}_{sourceId}`` so per-mapping feeds never
collide at the Camel URI level. The Lua-facing ``sourceKeys`` stay raw
SourceIds (blob contract, SPEC §8.4).
"""

from __future__ import annotations

import base64
import contextlib
import json
import logging
import os
import re
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from .endpoints import parse_endpoint, parse_endpoint_options
from .resolver import (
    ResolutionResult,
    ResolvedInteraction,
    ResolvedMapping,
    ResolvedSink,
    ResolvedSource,
    reference_values,
)

logger = logging.getLogger(__name__)

#: Default file names consumed by the DataBridge runtime (see databridge.camel-*).
ROUTES_FILE = "routes.json"
MQTT_DATASOURCE_FILE = "mqttdatasource.json"
MQTT_DATASINK_FILE = "mqttdatasink.json"
LUA_TRANSFORMER_FILE = "luatransformer.json"
AAS_DATASINK_FILE = "aasdatasink.json"
AAS_POLLING_DATASOURCE_FILE = "aaspollingdatasource.json"
OPCUA_DATASOURCE_FILE = "opcuadatasource.json"
OPCUA_DATASINK_FILE = "opcuadatasink.json"
PLC4X_DATASOURCE_FILE = "plc4xdatasource.json"
PLC4X_DATASINK_FILE = "plc4xdatasink.json"
HTTP_DATASOURCE_FILE = "httpdatasource.json"
HTTP_DATASINK_FILE = "httpdatasink.json"
KAFKA_DATASOURCE_FILE = "kafkadatasource.json"

def _substitute(value: str, aas_id: str, unit_short: str) -> str:
    """Resolve the {aas_id}/{aas_id_short} macros used in references."""
    return value.replace("{aas_id}", aas_id).replace("{aas_id_short}", unit_short)


#: Extracts the URI scheme of an affordance base (parse_endpoint drops it).
_SCHEME_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9+.\-]*)://")


#: Declared-MQTT-qualifier passthrough: an entry gains ``qos``/``retain`` ONLY
#: when the AID declares them (mqv_qos / mqv_retain) — never invented.
def _mqtt_qualifier_fields(entry: dict[str, Any], resolved) -> None:
    if resolved.qos is not None:
        entry["qos"] = resolved.qos
    if resolved.retain is not None:
        entry["retain"] = resolved.retain


def _mqtt_source_qualifiers(entry: dict[str, Any], resolved) -> None:
    # a subscription carries only the QoS — retain is a publisher concern
    if resolved.qos is not None:
        entry["qos"] = resolved.qos


def _device_schema_fields(entry: dict[str, Any], resolved) -> None:
    if resolved.schema is not None:
        entry["schema"] = resolved.schema
    if isinstance(resolved, ResolvedSink):
        if resolved.payload_schema is not None:
            entry["payloadSchema"] = resolved.payload_schema
        if resolved.payload_group:
            entry["payloadGroup"] = resolved.payload_group


@dataclass
class DescriptorContext:
    """Per-compilation accumulator state.

    One list per DMP config entity kind, the materialized blob/query files,
    and the global uniqueId registry backing the duplicate-id guard (ADR-017):
    every DMP consumer resolves entity ids across all files, so a collision
    between two mappings must fail compilation, not silently misroute.
    ``push_feeds`` backs the source-overlap guard: identical push feeds in two
    mappings would compile to byte-identical consumer URIs (Camel refuses a
    second consumer on a non-shared endpoint) — authors are pointed at fan-in
    (RequiredSources) instead."""

    files: dict[str, str] = field(default_factory=dict)
    routes: list[dict[str, Any]] = field(default_factory=list)
    mqtt_sources: list[dict[str, Any]] = field(default_factory=list)
    mqtt_sinks: list[dict[str, Any]] = field(default_factory=list)
    lua_transformers: list[dict[str, Any]] = field(default_factory=list)
    aas_sinks: list[dict[str, Any]] = field(default_factory=list)
    aas_polling_sources: list[dict[str, Any]] = field(default_factory=list)
    kafka_sources: list[dict[str, Any]] = field(default_factory=list)
    opcua_sources: list[dict[str, Any]] = field(default_factory=list)
    plc4x_sources: list[dict[str, Any]] = field(default_factory=list)
    http_sources: list[dict[str, Any]] = field(default_factory=list)
    opcua_sinks: list[dict[str, Any]] = field(default_factory=list)
    plc4x_sinks: list[dict[str, Any]] = field(default_factory=list)
    http_sinks: list[dict[str, Any]] = field(default_factory=list)
    entity_ids: dict[str, str] = field(default_factory=dict)
    #: (broker host, port, topic) -> origin description of registered pushes
    push_feeds: dict[tuple[str, int, str], str] = field(default_factory=dict)

    def register_entity(self, unique_id: str, origin: str) -> str:
        """Registers a global entity id, failing loudly on collisions."""
        previous = self.entity_ids.get(unique_id)
        if previous is not None:
            raise ValueError(
                f"duplicate DMP entity id {unique_id!r}: {origin} collides with "
                f"{previous}. Mapping names and SourceIds/SinkIds must be unique "
                f"within a unit (AIMC scopes ids per mapping, but the DMP "
                f"resolves them globally — ADR-017)")
        self.entity_ids[unique_id] = origin
        return unique_id

    def register_push_feed(self, host: str, port: int, topic: str, origin: str) -> None:
        """Source-overlap guard (ADR-020 W2.7): two mappings subscribing to the
        identical broker+topic feed fail compilation with both origins named."""
        key = (host, port, topic)
        previous = self.push_feeds.get(key)
        if previous is not None:
            raise ValueError(
                f"source-overlap: {origin} consumes the identical push feed "
                f"{topic!r} already consumed by {previous}. Duplicate consumers "
                f"cannot coexist at the Camel URI level — move this feed into "
                f"ONE mapping and merge the targets with RequiredSources fan-in")
        self.push_feeds[key] = origin

    def snapshot(self) -> dict[str, Any]:
        """Shallow-copies every mutable field for rollback when a mapping is
        later skipped — a skipped mapping must not leave orphaned consumer,
        sink or transformer entries behind."""
        import dataclasses

        state: dict[str, Any] = {}
        for f in dataclasses.fields(self):
            value = getattr(self, f.name)
            if isinstance(value, list):
                state[f.name] = list(value)
            elif isinstance(value, dict):
                state[f.name] = dict(value)
            else:
                state[f.name] = value
        return state

    def restore(self, state: dict[str, Any]) -> None:
        import dataclasses

        for f in dataclasses.fields(self):
            value = state[f.name]
            if isinstance(value, list):
                getattr(self, f.name)[:] = value
            elif isinstance(value, dict):
                getattr(self, f.name).clear()
                getattr(self, f.name).update(value)
            else:
                setattr(self, f.name, value)


@dataclass
class DmpDescriptor:
    """Config files for one DMP container (1 DMP per managed unit, ADR-021)."""

    asset_id: str
    asset_id_short: str
    files: dict[str, str] = field(default_factory=dict)

    def write_to(self, directory) -> None:
        """Persist all files under the given config directory.

        Each file is written to a sibling temp file and swapped into place via
        ``os.replace``, so readers never observe partial content; files not in
        the current descriptor are pruned. The directory itself stays in place
        — replacing it would break live bind mounts into running containers."""
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        for name in self.files:
            rel = Path(name)
            if rel.is_absolute() or ".." in rel.parts:
                raise ValueError(f"DMP descriptor path escapes its config directory: {name!r}")
            parent = root
            for part in rel.parts[:-1]:
                parent = parent / part
                if parent.is_symlink():
                    raise ValueError(f"DMP descriptor path traverses a symlink: {name!r}")
        staged: list[tuple[Path, str]] = []
        for name, content in sorted(self.files.items()):
            rel = Path(name)
            target = root / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_name = tempfile.mkstemp(
                dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as tmp_file:
                    tmp_file.write(content)
                os.chmod(tmp_name, 0o644)
                staged.append((target, tmp_name))
            except BaseException:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_name)
                for _, pending in staged:
                    with contextlib.suppress(OSError):
                        os.unlink(pending)
                raise
        backups: dict[Path, tuple[bytes, int] | None] = {}
        replaced: list[Path] = []
        try:
            for target, tmp_name in staged:
                if target.is_symlink():
                    raise ValueError(f"refusing to replace symlink in DMP config: {target}")
                backups[target] = ((target.read_bytes(), target.stat().st_mode & 0o777)
                                   if target.is_file() else None)
                os.replace(tmp_name, target)
                replaced.append(target)
        except BaseException:
            for target in reversed(replaced):
                previous = backups[target]
                if previous is None:
                    with contextlib.suppress(OSError):
                        target.unlink()
                else:
                    old_content, mode = previous
                    fd, rollback_name = tempfile.mkstemp(
                        dir=str(target.parent), prefix=f".{target.name}.", suffix=".rollback")
                    try:
                        with os.fdopen(fd, "wb") as rollback:
                            rollback.write(old_content)
                        os.chmod(rollback_name, mode)
                        os.replace(rollback_name, target)
                    except BaseException:
                        with contextlib.suppress(OSError):
                            os.unlink(rollback_name)
                        raise
            raise
        finally:
            for _, pending in staged:
                with contextlib.suppress(OSError):
                    os.unlink(pending)
        for existing in root.rglob("*"):
            rel = existing.relative_to(root).as_posix()
            if existing.is_file() and not existing.is_symlink() and rel not in self.files:
                existing.unlink()
                logger.info("pruned stale DMP config file %s", existing)
        for dirpath, dirnames, filenames in os.walk(root, topdown=False):
            stale_dir = Path(dirpath)
            if stale_dir != root and not any(stale_dir.iterdir()):
                stale_dir.rmdir()
        logger.info("wrote %d DMP config files to %s", len(self.files), root)


class DescriptorCompiler:
    def __init__(
        self,
        *,
        aas_base_url: str,
        aas_id: str | None = None,
        aas_id_short: str | None = None,
        unit_name: str | None = None,
        mqtt_broker: tuple[str, int] | None = None,
        aas_event_broker: tuple[str, int] | None = None,
        aas_event_topic: str = "submodel-events",
        aas_event_group: str | None = None,
    ) -> None:
        self.aas_base_url = aas_base_url.rstrip("/")
        self.aas_id = aas_id or "{aas_id}"
        self.aas_id_short = aas_id_short
        # identity prefix of all scoped DMP-internal names: derived from the
        # AIMC instance id (ADR-021), falling back to the shell idShort
        self.unit = unit_name or aas_id_short or "{unit}"
        # Optional (host, port) override for all MQTT traffic: the AID base
        # may point at a broker the DMP cannot reach directly.
        self.mqtt_broker = mqtt_broker
        self.aas_event_broker = aas_event_broker
        self.aas_event_topic = aas_event_topic
        self.aas_event_group = aas_event_group or f"dmp-{self.unit}-aas-events"

    # -- scoped identifiers ------------------------------------------------ #
    def _mapping_scope(self, mapping: ResolvedMapping) -> str:
        return f"{self.unit}_{mapping.id_short}"

    def _source_entity_id(self, mapping: ResolvedMapping, source_id: str) -> str:
        return f"{self._mapping_scope(mapping)}_{source_id}"

    def _sink_entity_id(self, mapping: ResolvedMapping, sink_id: str) -> str:
        return f"{self._mapping_scope(mapping)}_{sink_id}_sink"

    def _transformer_id(self, mapping: ResolvedMapping) -> str:
        return f"{self._mapping_scope(mapping)}_transformer"

    def _blob_path(self, mapping: ResolvedMapping, kind: str = "") -> str:
        stem = f"{self._mapping_scope(mapping)}{kind}"
        return f"queries/{stem}.lua"

    def compile(self, resolution: ResolutionResult) -> DmpDescriptor:
        descriptor = DmpDescriptor(
            asset_id=self.aas_id,
            asset_id_short=self.aas_id_short or resolution.asset_id_short,
        )
        ctx = DescriptorContext()

        for entries, compile_one in (
            (resolution.mappings, self._compile_mapping),
            (resolution.interactions, self._compile_interaction),
        ):
            for entry in entries:
                snapshot = ctx.snapshot()
                route_count = len(ctx.routes)
                compile_one(ctx, entry)
                if len(ctx.routes) == route_count:
                    # A rejected leg may already have registered a command
                    # sink, transformer or query blob. Publish none of it.
                    ctx.restore(snapshot)

        self._validate_routes(ctx.routes)

        self._emit_files(descriptor, ctx)
        return descriptor

    @staticmethod
    def _validate_routes(routes: list[dict[str, Any]]) -> None:
        route_ids: set[str] = set()
        rest_bindings: set[tuple[str, str]] = set()
        for route in routes:
            route_id = route.get("routeId")
            if not route_id or route_id in route_ids:
                raise ValueError(f"duplicate or empty DMP route id {route_id!r}")
            route_ids.add(route_id)
            data = route.get("triggerData") or {}
            path = data.get("restPath")
            if path:
                binding = (str(data.get("restMethod", "POST")).upper(), path)
                if binding in rest_bindings:
                    raise ValueError(f"duplicate DMP REST binding {binding[0]} {binding[1]}")
                rest_bindings.add(binding)

    # ------------------------------------------------------------------ #
    # artifact emitters (one function per output file / entity family)
    # ------------------------------------------------------------------ #
    def _emit_files(self, descriptor: DmpDescriptor, ctx: DescriptorContext) -> None:
        descriptors = [
            (ROUTES_FILE, ctx.routes),
            (MQTT_DATASOURCE_FILE, ctx.mqtt_sources),
            (MQTT_DATASINK_FILE, ctx.mqtt_sinks),
            (LUA_TRANSFORMER_FILE, ctx.lua_transformers),
            (AAS_DATASINK_FILE, ctx.aas_sinks),
            (AAS_POLLING_DATASOURCE_FILE, ctx.aas_polling_sources),
            (KAFKA_DATASOURCE_FILE, ctx.kafka_sources),
            (OPCUA_DATASOURCE_FILE, ctx.opcua_sources),
            (PLC4X_DATASOURCE_FILE, ctx.plc4x_sources),
            (HTTP_DATASOURCE_FILE, ctx.http_sources),
            (OPCUA_DATASINK_FILE, ctx.opcua_sinks),
            (PLC4X_DATASINK_FILE, ctx.plc4x_sinks),
            (HTTP_DATASINK_FILE, ctx.http_sinks),
        ]
        descriptor.files.update(ctx.files)  # materialized blob/query files
        for name, entries in descriptors:
            if entries:
                descriptor.files[name] = json.dumps(entries, indent=2)

    # -- mapping -> execution-policy route --------------------------------- #
    def _compile_mapping(self, ctx: DescriptorContext, mapping: ResolvedMapping) -> dict[str, Any]:
        """One MappingConfiguration → one uniform policy-driven route.

        Every source becomes its configured consumer entry; every supported
        sink becomes its sink entry; the transform(s) run once before the
        multicast fan-out (W4 creator semantics). Unresolvable pieces fail
        the MAPPING loudly — partial routes hide authoring errors."""
        if not mapping.sources:
            logger.warning("mapping %s has no sources; skipping", mapping.id_short)
            return {}

        unresolved = [s.source_id for s in mapping.sources if not s.protocol]
        if unresolved:
            logger.warning("mapping %s: unresolved sources %s; skipping",
                           mapping.id_short, unresolved)
            return {}

        # emit with rollback: a mapping skipped later (unsupported sinks)
        # must not leave orphaned consumer/transformer entries
        snapshot = ctx.snapshot()
        if mapping.default_polling_interval is not None and mapping.default_polling_interval <= 0:
            raise ValueError(
                f"mapping {mapping.id_short}: DefaultPollingInterval must be positive"
            )
        transformer_id = self._emit_transformer(ctx, mapping)
        datasource_ids: list[str | None] = []
        for original in mapping.sources:
            src = replace(original)
            if src.polling_interval is not None and src.polling_interval <= 0:
                raise ValueError(
                    f"mapping {mapping.id_short} source {src.source_id}: "
                    "PollingInterval must be positive"
                )
            if src.protocol == "aas":
                effective_interval = (
                    src.polling_interval
                    if src.polling_interval is not None
                    else mapping.default_polling_interval
                )
                if src.aas_property and effective_interval is None:
                    if self.aas_event_broker is None:
                        raise ValueError(
                            f"mapping {mapping.id_short} source {src.source_id}: "
                            "AAS Property has no effective polling interval and no usable event stream"
                        )
                    src.delivery = "async"  # observe exact Property changes
                elif effective_interval is not None and effective_interval > 0:
                    src.delivery = "sync"  # read at the effective polling cadence
                else:
                    raise ValueError(
                        f"mapping {mapping.id_short} source {src.source_id}: "
                        "AAS collection/list/submodel reads require a positive effective polling interval; "
                        "observation capability is not assumed"
                    )
            if src.delivery == "sync":
                # Preserve the existing device connector cadence contract:
                # source interval overrides the mapping default; when both are
                # absent the runtime's legacy device polling default applies.
                # AAS Property sources are resolved separately above: absent
                # an effective interval, they must use repository events.
                effective_interval = src.polling_interval or mapping.default_polling_interval
                src.polling_interval = effective_interval
            datasource_ids.append(self._add_datasource(ctx, mapping, src))
        missing = [i for i, d in enumerate(datasource_ids) if d is None]
        if missing:
            ctx.restore(snapshot)
            logger.warning("mapping %s: %d/%d sources have no builder/endpoint "
                           "(%s); skipping", mapping.id_short, len(missing),
                           len(datasource_ids),
                           [mapping.sources[i].source_id for i in missing])
            return {}

        sink_ids = self._compile_sinks(ctx, mapping)
        if len(sink_ids) != len(mapping.sink_ids) or not sink_ids:
            ctx.restore(snapshot)
            logger.warning("mapping %s: %d/%d sinks have no supported builder/endpoint; "
                           "skipping", mapping.id_short,
                           len(mapping.sink_ids) - len(sink_ids), len(mapping.sink_ids))
            return {}

        route: dict[str, Any] = {
            "routeId": self._mapping_scope(mapping),
            "trigger": "execution-policy",
            "datasources": [d for d in datasource_ids if d],  # plural ALWAYS
            "transformers": [transformer_id] if transformer_id else [],
            "datasinks": sink_ids,
        }
        if mapping.trigger_source_ids is not None:
            route["triggerData"] = {"triggerSourceIds": list(mapping.trigger_source_ids)}
        if transformer_id and len(sink_ids) > 1:
            route["datasinkMappingConfiguration"] = {
                sink: [transformer_id] for sink in sink_ids
            }
        ctx.routes.append(route)
        return route

    # -- interaction -> ProtocolBridge route ------------------------------- #
    def _compile_interaction(self, ctx: DescriptorContext, bidi: ResolvedInteraction) -> None:
        """Compiles one operation delegation into a bridge route.

        The command leg rides the native command affordance's protocol —
        ANY protocol the AID declares an ACTION affordance on:

        - ``mqtt``: ``paho-mqtt5-inout`` over the upstream MQTT 5 transport
          (the reply topic is the affordance's
          DECLARED ``forms.response.href``);
        - ``rest`` (HTTP, and IO-Link over Profinet REST): the camel-http
          producer POSTs the transformed command — the HTTP response IS the
          reply (synchronous, no separate reply consumer);
        - ``opcua``: milo-client node write (synchronous, no reply payload);
        - ``modbus``/``plc4x``: the plc4x producer writes the transformed tag
          values (synchronous, no reply payload).

        The caller-facing REST surface is derived BY CONVENTION from the
        operation path (DMP v3): the route carries ``restPath``/``restMethod``
        + ``protocol`` in its ``triggerData`` and the bridge emits the
        camel-rest consumer from it. One-MC authoring carries no response
        blob — an identity blob passes replies through unchanged."""
        if bidi.unresolved:
            logger.warning("interaction %s unresolved (%s); skipping",
                           bidi.id_short, "; ".join(bidi.unresolved))
            return
        sink = bidi.command_sink
        if sink is None or not bidi.rest_path:
            logger.warning("interaction %s: no command sink/path; skipping",
                           bidi.id_short)
            return

        rest_path = _substitute(bidi.rest_path, self.aas_id,
                                self.aas_id_short or self.unit)
        if "{aas_id_short}" in rest_path or "{aas_id}" in rest_path:
            # the owning-shell lookup failed upstream and the macro stayed
            # literal: camel-rest would treat it as a URI template matching
            # ANY first segment — never deploy that (ADR-022 derived surface)
            logger.warning("interaction %s: unresolved {aas_id_short} macro in "
                           "rest path %r (no owning shell); skipping",
                           bidi.id_short, bidi.rest_path)
            return

        action = bidi.id_short
        scope = f"{self.unit}_{action}"
        # dual-binding delegations (ADR-022): the resolver suffixes the
        # derived path of non-primary bindings (…/{action}/{protocol}) —
        # keep their scoped ids/files apart too
        plain_path = f"/operations/{self.aas_id_short or self.unit}/{action}"
        binding = rest_path[len(plain_path):] if rest_path.startswith(plain_path) else ""
        if binding:
            scope += "".join("_" + seg for seg in binding.split("/") if seg)
        method = (bidi.rest_method or "POST").upper()
        protocol = (bidi.protocol or sink.protocol or "").lower()

        command_sink_ids: list[str] = []
        trigger_data: dict[str, Any] = {}

        if protocol == "mqtt":
            for sink_id, action_sink in bidi.command_sinks or [(bidi.command_sink_id, sink)]:
                endpoint_id = self._add_interaction_mqtt_command(
                    ctx, f"{scope}_{sink_id}", action, action_sink, sink_id)
                if endpoint_id is not None:
                    command_sink_ids.append(endpoint_id)
            if len(command_sink_ids) != len(bidi.command_sinks or [None]):
                return
            trigger_data = {}
            if bidi.request_reply:
                # the DECLARED MQTT 5 request/reply correlation: the
                # paho-mqtt5-inout endpoint owns the reply subscription
                # itself through the upstream paho-mqtt5 consumer.
                trigger_data["requestReply"] = True
                absolute_rt = self._absolute_reply_topic(action, sink)
                if absolute_rt is None:
                    return
                trigger_data["responseTopic"] = absolute_rt
            else:
                logger.warning("interaction %s: MQTT request/reply terms are required", action)
                return
        elif protocol in ("rest", "http"):
            for sink_id, action_sink in bidi.command_sinks or [(bidi.command_sink_id, sink)]:
                endpoint_id = self._add_http_datasink(
                    ctx, f"{scope}_{sink_id}", action_sink,
                    f"command sink {sink_id} of interaction {action}")
                if endpoint_id is not None:
                    command_sink_ids.append(endpoint_id)
            if len(command_sink_ids) != len(bidi.command_sinks or [None]):
                return
            trigger_data = {}
        elif protocol == "opcua":
            # UA Method INVOCATION (OPC-10101 §7.1.3): the Call service needs
            # the owning Object (uav_componentOf) + the Method NodeId — the
            # Call returns its OutputArguments inline (synchronous reply)
            if not sink.method_node:
                logger.warning("interaction %s: OPC UA action href carries no "
                               "NodeId (§6.2 convention /?id=<nodeId>); skipping",
                               action)
                return
            for sink_id, action_sink in bidi.command_sinks or [(bidi.command_sink_id, sink)]:
                endpoint_id = self._add_opcua_datasink(
                    ctx, f"{scope}_{sink_id}", action_sink,
                    f"command sink {sink_id} of interaction {action}",
                    method_node=action_sink.method_node)
                if endpoint_id is not None:
                    command_sink_ids.append(endpoint_id)
            if len(command_sink_ids) != len(bidi.command_sinks or [None]):
                return
            trigger_data = {}
        elif protocol in ("modbus", "plc4x", "bacnet"):
            for sink_id, action_sink in bidi.command_sinks or [(bidi.command_sink_id, sink)]:
                endpoint_id = self._add_plc4x_datasink(
                    ctx, f"{scope}_{sink_id}", action_sink,
                    f"command sink {sink_id} of interaction {action}")
                if endpoint_id is not None:
                    command_sink_ids.append(endpoint_id)
            if len(command_sink_ids) != len(bidi.command_sinks or [None]):
                return
            trigger_data = {}
        else:
            logger.warning("interaction %s: no bridge builder for command "
                           "protocol %r (AID action affordance); skipping",
                           action, protocol)
            return

        target_options: dict[str, Any] = {"timeoutSeconds": int(bidi.timeout_seconds)}
        if protocol == "mqtt":
            target_options.update({
                "requestReply": bool(trigger_data.get("requestReply")),
                "responseTopic": trigger_data.get("responseTopic"),
            })
        elif protocol in ("rest", "http"):
            target_options["httpMethod"] = (sink.http_method or "POST").upper()
        trigger_data.pop("requestReply", None)
        trigger_data.pop("responseTopic", None)
        trigger_data["action"] = action
        if target_options:
            trigger_data["targetOptions"] = target_options

        request_transformer_id = ctx.register_entity(
            f"{scope}_request_transformer", f"request transformer of interaction {action}")
        response_transformer_id = ctx.register_entity(
            f"{scope}_response_transformer", f"response transformer of interaction {action}")
        route_id = f"{scope}_interaction"

        request_path = f"queries/{scope}_request.lua"
        response_path = f"queries/{scope}_response.lua"

        ctx.lua_transformers.append({
            "uniqueId": request_transformer_id,
            "queryPath": request_path,
            "sourceKeys": ",".join(bidi.request_source_ids),
        })
        response_blob = bidi.response_blob or (
            # one-MC authoring: replies flow through unchanged (the bridge
            # unwraps the action key / Uuid envelope client-side)
            "function aimc_main(sources)\n"
            f"    return {{ [\"{action}\"] = sources[\"{action}\"] }}\n"
            "end\n")
        ctx.lua_transformers.append({
            "uniqueId": response_transformer_id,
            "queryPath": response_path,
            "sourceKeys": (",".join(source.source_id for source in bidi.response_sources)
                           if bidi.explicit_response_mapping and bidi.response_sources
                           else (bidi.operation_source_id if bidi.explicit_response_mapping
                                 else action)),
        })

        trigger_data.update({
            # the derived caller-facing operation path (DMP v3): the bridge
            # creates the REST consumer from it, no restsources.json
            "restPath": rest_path,
            "restMethod": method,
            "responseTransformer": response_transformer_id,
            "operationSourceId": bidi.operation_source_id,
            "requestSourceIds": list(bidi.request_source_ids),
            "commandSinkId": bidi.command_sink_id,
            "commandSinkIds": command_sink_ids,
            "commandSinkBindings": [
                {"sinkId": sink_id, "dataSinkId": endpoint_id,
                 "valuePath": action_sink.value_path}
                for (sink_id, action_sink), endpoint_id
                in zip(bidi.command_sinks, command_sink_ids)
            ],
            "callerReplySinkId": (bidi.caller_reply_sink_id or bidi.operation_source_id
                                  if bidi.explicit_response_mapping else action),
            "explicitResponseMapping": bidi.explicit_response_mapping,
        })
        if bidi.trigger_source_ids is not None:
            trigger_data["triggerSourceIds"] = list(bidi.trigger_source_ids)
        response_sink_ids: list[str] = []
        response_bindings: list[dict[str, Any]] = []
        if bidi.explicit_response_mapping:
            for sink_id, resolved_sink, sink_ref in zip(
                    bidi.response_sink_ids, bidi.response_sinks, bidi.response_sink_refs):
                # The caller-reply identity selects the Operation result; it
                # is not a second write destination.
                if sink_id == bidi.caller_reply_sink_id:
                    continue
                if resolved_sink.aas:
                    endpoint_id = self._add_aas_sink(
                        ctx, bidi, sink_ref, sink_id, entity_scope=scope)
                elif resolved_sink.protocol == "mqtt":
                    endpoint_id = self._add_mqtt_datasink(
                        ctx, bidi, resolved_sink, sink_id, entity_scope=scope)
                elif resolved_sink.protocol in ("rest", "http"):
                    endpoint_id = self._add_http_datasink(
                        ctx, f"{scope}_{sink_id}", resolved_sink,
                        f"response sink {sink_id} of interaction {action}")
                elif resolved_sink.protocol == "opcua":
                    endpoint_id = self._add_opcua_datasink(
                        ctx, f"{scope}_{sink_id}", resolved_sink,
                        f"response sink {sink_id} of interaction {action}",
                        method_node=resolved_sink.method_node)
                elif resolved_sink.protocol in ("modbus", "plc4x"):
                    endpoint_id = self._add_plc4x_datasink(
                        ctx, f"{scope}_{sink_id}", resolved_sink,
                        f"response sink {sink_id} of interaction {action}")
                else:
                    endpoint_id = None
                if endpoint_id is None:
                    logger.warning("interaction %s: could not compile ResponseSink %s",
                                   action, sink_id)
                    return
                response_sink_ids.append(endpoint_id)
                response_bindings.append({
                    "sinkId": sink_id,
                    "dataSinkId": endpoint_id,
                    "payloadGroup": resolved_sink.payload_group,
                    "valuePath": resolved_sink.value_path,
                    "payloadSchema": resolved_sink.payload_schema,
                    "schema": resolved_sink.schema,
                    "aas": resolved_sink.aas,
                    "jsonPayload": (not resolved_sink.aas
                                    and resolved_sink.protocol in ("mqtt", "rest", "http")
                                    and bool(resolved_sink.payload_schema or resolved_sink.schema)),
                })
            trigger_data["responseSourceBindings"] = [
                {"sourceId": source.source_id, "valuePath": source.value_path}
                for source in bidi.response_sources
            ]
            if not bidi.response_sources:
                trigger_data["responseSourceBindings"] = [{
                    "sourceId": bidi.operation_source_id, "valuePath": None,
                }]
            trigger_data["responseSinkBindings"] = response_bindings
            trigger_data["extraResponseSinkIds"] = [binding["sinkId"] for binding in response_bindings]
        # DECLARED interaction contract (AID form terms + DataSchemas),
        # consumed by the bridge at instantiation — value parsing and MQTT 5
        # request/reply correlation are data-driven, never name-hardcoded
        if bidi.input_types:
            trigger_data["parameterTypes"] = dict(bidi.input_types)
        if sink.payload_schema and sink.payload_schema.get("required"):
            trigger_data["parameterRequired"] = list(sink.payload_schema["required"])
        if bidi.output_types:
            trigger_data["responseTypes"] = dict(bidi.output_types)
        if sink.output_schema is not None:
            trigger_data["responseSchema"] = sink.output_schema
            if sink.output_schema.get("required"):
                trigger_data["responseRequired"] = list(sink.output_schema["required"])
        if bidi.caller_response_order:
            # Caller OperationVariable[] order, never UA Method argument order.
            trigger_data["callerOutputOrder"] = list(bidi.caller_response_order)
        ctx.routes.append({
            "routeId": route_id,
            "trigger": "bridge",
            "datasources": [],
            "transformers": [request_transformer_id],
            "datasinks": command_sink_ids + response_sink_ids,
            "triggerData": trigger_data,
        })

        ctx.files[request_path] = bidi.request_blob or ""
        ctx.files[response_path] = response_blob

    # -- interaction legs (per protocol) ------------------------------------ #
    def _absolute_reply_topic(self, action: str, sink: ResolvedSink) -> str | None:
        """The ABSOLUTE reply topic (base path + the declared relative
        topic) — the same construction the legacy response consumer uses."""
        if not sink.response_topic:
            logger.warning("interaction %s: command affordance declares no "
                           "response form; skipping", action)
            return None
        rt = f"/{sink.response_topic.lstrip('/')}"
        _, _, base_path = parse_endpoint(sink.base)
        return ((base_path.rstrip("/") + rt) if base_path else rt).lstrip("/")

    def _add_interaction_mqtt_command(self, ctx: DescriptorContext, scope: str,
                                      action: str, sink: ResolvedSink,
                                      sink_id: str) -> str | None:
        host, port, base_path = parse_endpoint(sink.base)
        if not host or not port:
            logger.warning("interaction %s: incomplete MQTT base %r",
                           action, sink.base)
            return None
        if self.mqtt_broker:
            host, port = self.mqtt_broker
        topic = sink.topic or ""
        if topic and not topic.startswith("/"):
            topic = "/" + topic
        full_topic = ((base_path.rstrip("/") + topic) if base_path else topic).lstrip("/")
        if not full_topic:
            logger.warning("interaction %s: empty command topic; skipping", action)
            return None
        command_sink_id = ctx.register_entity(
            f"{scope}_command_sink", f"command sink {sink_id} of interaction {action}")
        cmd_entry: dict[str, Any] = {
            "uniqueId": command_sink_id,
            "sinkId": sink_id,
            "serverUrl": host,
            "serverPort": port,
            "topic": full_topic,
            "clientId": f"dmp-{scope}-cmd",
            "userName": "",
            "password": "",
        }
        _mqtt_qualifier_fields(cmd_entry, sink)
        _device_schema_fields(cmd_entry, sink)
        ctx.mqtt_sinks.append(cmd_entry)
        return command_sink_id

    # -- sinks ------------------------------------------------------------- #
    def _compile_sinks(self, ctx: DescriptorContext, mapping: ResolvedMapping) -> list[str]:
        """Compiles AAS-element and interface sinks, returning sink ids.

        Overlapping SINKS stay legal — independent writes are intentional
        semantics (only the push-feed/source side is guarded)."""
        sink_ids: list[str] = []
        for sink_ref, sink_id in zip(mapping.sink_refs, mapping.sink_ids):
            resolved = next((s for s in mapping.resolved_sinks
                             if s.sink_id == sink_id), None)
            if resolved is None:
                continue
            scope = f"{self._mapping_scope(mapping)}_{sink_id}"
            origin = f"{resolved.protocol} sink {sink_id} of mapping {mapping.id_short}"
            if resolved.aas:
                sink_key = self._add_aas_sink(ctx, mapping, sink_ref, sink_id)
            elif resolved.protocol == "mqtt":
                sink_key = self._add_mqtt_datasink(ctx, mapping, resolved, sink_id)
            elif resolved.protocol in ("rest", "http"):
                sink_key = self._add_http_datasink(ctx, scope, resolved, origin)
            elif resolved.protocol == "opcua":
                sink_key = self._add_opcua_datasink(ctx, scope, resolved, origin)
            elif resolved.protocol in self.PLC4X_DRIVERS:
                sink_key = self._add_plc4x_datasink(ctx, scope, resolved, origin)
            else:
                logger.warning("mapping %s: sink %s targets an unsupported sink "
                               "(protocol=%s); skipping", mapping.id_short, sink_id,
                               resolved.protocol)
                sink_key = None
            if sink_key:
                sink_ids.append(sink_key)
        return sink_ids

    # -- transformers ------------------------------------------------------ #
    def _emit_transformer(self, ctx: DescriptorContext, mapping: ResolvedMapping) -> str | None:
        """Emits the mapping's Lua transformer; passthrough
        identity when no Transformation blob exists (exact SinkId==SourceId
        pairs only)."""
        if mapping.transformation:
            return self._add_lua_transformer(ctx, mapping)
        if mapping.sink_ids:
            return self._passthrough_transformer(ctx, mapping)
        return None

    def _add_lua_transformer(self, ctx: DescriptorContext, mapping: ResolvedMapping) -> str:
        unique_id = ctx.register_entity(
            self._transformer_id(mapping), f"Lua transformer of mapping {mapping.id_short}")
        query_path = self._blob_path(mapping)
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "queryPath": query_path,
            "sourceKeys": ",".join(s.source_id for s in mapping.sources) or "body",
        }
        ctx.lua_transformers.append(entry)
        ctx.files[query_path] = mapping.transformation or ""
        return unique_id

    def _passthrough_transformer(self, ctx: DescriptorContext, mapping: ResolvedMapping) -> str:
        """R3 — identity transform when the mapping declares NO Transformation
        blob. Only exact SinkId==SourceId pairs pass through; unmatched sinks
        are skipped with a warning — positional guessing invents flows the
        model never declared."""
        sources = {s.source_id for s in mapping.sources}
        lines = []
        for sink_id in mapping.sink_ids:
            if sink_id not in sources:
                logger.warning(
                    "passthrough mapping %s: SinkId %r matches no SourceId; "
                    "skipped (passthrough pairs by identity only)",
                    mapping.id_short, sink_id)
                continue
            lines.append(f'        ["{sink_id}"] = sources["{sink_id}"],')
        if not lines:
            logger.warning("passthrough mapping %s: no SinkId matches a SourceId; "
                           "the passthrough blob returns an empty object", mapping.id_short)

        blob = ("function aimc_main(sources)\n"
                "    return {\n"
                + "\n".join(lines)
                + "\n    }\n"
                "end\n")

        unique_id = ctx.register_entity(
            f"{self._mapping_scope(mapping)}_passthrough",
            f"passthrough transformer of mapping {mapping.id_short}")
        query_path = f"queries/{self._mapping_scope(mapping)}_passthrough.lua"
        ctx.lua_transformers.append(
            {
                "uniqueId": unique_id,
                "queryPath": query_path,
                "sourceKeys": ",".join(s.source_id for s in mapping.sources) or "body",
            }
        )
        ctx.files[query_path] = blob
        return unique_id

    # -- sources (per protocol) -------------------------------------------- #
    #: PLC4X driver per AID protocol — BACnet rides the plc4x builders
    #: (``org.apache.plc4x:plc4j-driver-bacnetip`` runtime dep, see ADR-022).
    PLC4X_DRIVERS = {"modbus": "modbus-tcp", "plc4x": "modbus-tcp", "bacnet": "bacnet-ip"}

    def _add_datasource(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                        source: ResolvedSource) -> str | None:
        """Dispatches a resolved source to its per-protocol config builder."""
        p = source.protocol
        if p == "mqtt":
            return self._add_mqtt_source(ctx, mapping, source)
        if p == "aas":
            return self._add_aas_source(ctx, mapping, source)
        if p == "opcua":
            return self._add_opcua_source(ctx, mapping, source)
        if p in self.PLC4X_DRIVERS:
            return self._add_plc4x_source(ctx, mapping, source)
        if p in ("http", "rest"):
            return self._add_http_source(ctx, mapping, source)
        logger.warning("mapping %s: source %s has no datasource builder for "
                       "protocol %r; skipping", mapping.id_short, source.source_id, p)
        return None

    def _add_mqtt_source(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                         source: ResolvedSource) -> str | None:
        host, port, base_path = parse_endpoint(source.base)
        if not host or not port:
            logger.warning("mapping %s: source %s has incomplete MQTT base %r; skipping",
                           mapping.id_short, source.source_id, source.base)
            return None
        if self.mqtt_broker:
            host, port = self.mqtt_broker

        topic = source.topic or ""
        if topic and not topic.startswith("/"):
            topic = "/" + topic
        full_topic = ((base_path.rstrip("/") + topic) if base_path else topic).lstrip("/")
        if not full_topic:
            logger.warning("mapping %s: source %s has empty topic; skipping",
                           mapping.id_short, source.source_id)
            return None
        if source.delivery == "async":
            # guard BEFORE registering: duplicate push feeds fail loudly here
            ctx.register_push_feed(
                host, port, full_topic,
                f"mapping {mapping.id_short} source {source.source_id}")

        unique_id = ctx.register_entity(
            self._source_entity_id(mapping, source.source_id),
            f"mqtt source {source.source_id} of mapping {mapping.id_short}")
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            # raw AIMC SourceId: fan-in aggregation and Lua sourceKeys key on
            # this, independent of the qualified uniqueId (ADR-017)
            "sourceId": source.source_id,
            "delivery": source.delivery,
            "serverUrl": host,
            "serverPort": port,
            "topic": full_topic,
            # stable per-mapping clientId: two feeds can share broker/topic-
            # shaped URI fragments without colliding at the endpoint cache
            "clientId": f"dmp-{self._source_entity_id(mapping, source.source_id)}",
        }
        if source.delivery == "sync" and source.polling_interval:
            # a declared polling cadence on a retained MQTT topic: the DMP's
            # timer route samples the sync surface at the declared interval
            entry["pollingIntervalMs"] = int(source.polling_interval * 1000)
        _mqtt_source_qualifiers(entry, source)
        _device_schema_fields(entry, source)
        if source.value_path:
            # AUTHORED fine granularity (ADR-020): the DMP disassembles the
            # payload to this JSON Pointer leaf before placing it under SourceId
            entry["valuePath"] = source.value_path
        ctx.mqtt_sources.append(entry)
        return unique_id

    def _add_aas_source(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                          source: ResolvedSource) -> str | None:
        """Compile an AAS element source to exact polling or repository-event
        delivery according to the resolved AIMC Property mode."""
        path = source.ref_path
        if not path:
            logger.warning("mapping %s: AAS source %s has no reference path; skipping",
                           mapping.id_short, source.source_id)
            return None
        submodel_id = _substitute(path[0], self.aas_id, self.unit)
        b64 = base64.urlsafe_b64encode(submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
        submodel_endpoint = f"{self.aas_base_url}/submodels/{b64}"
        id_short_path = "/".join(path[1:]) if len(path) > 1 else ""
        # The reference addresses the element directly: a Variables/Parameters
        # entry is a plain Property unless it groups children (then it is an
        # SMC and the authored keys already descend into it), so no legacy
        # '.variable' compatibility suffix is applied.
        unique_id = ctx.register_entity(
            self._source_entity_id(mapping, source.source_id),
            f"aas source {source.source_id} of mapping {mapping.id_short}")
        aas_entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "sourceId": source.source_id,
            "delivery": source.delivery,
            "mode": "poll",
            "elementType": source.aas_element_type or "Submodel",
            "submodelEndpoint": submodel_endpoint,
            "idShortPath": id_short_path,
            "api": "DotAasV3",
        }
        if source.delivery == "sync" and source.polling_interval:
            aas_entry["pollingIntervalMs"] = int(source.polling_interval * 1000)
        event_element = source.aas_property or source.aas_element_type in {
            "SubmodelElementCollection", "SubmodelElementList"
        }
        if event_element and source.delivery == "async":
            if self.aas_event_broker is None:
                raise ValueError(
                    f"mapping {mapping.id_short} source {source.source_id}: "
                    "AAS element source has no usable repository change stream"
                )
            broker_host, broker_port = self.aas_event_broker
            event_path = ".".join(path[1:])
            ctx.kafka_sources.append({
                "uniqueId": unique_id,
                "sourceId": source.source_id,
                "sourceKind": "aas-element-event",
                "elementType": source.aas_element_type or "Property",
                "mode": "event",
                "delivery": "async",
                "serverUrl": broker_host,
                "serverPort": broker_port,
                "topic": self.aas_event_topic,
                # Source-scoped groups preserve correctness until a shared
                # per-DMP event ingress is introduced.
                "groupId": f"{self.aas_event_group}-{unique_id}",
                "maxPollRecords": 500,
                "consumersCount": 1,
                "seekTo": "END",
                "breakOnFirstError": True,
                "eventFilter": {
                    "submodelId": submodel_id,
                    "elementPath": event_path,
                    "modelType": source.aas_element_type or "Property",
                    "readUrl": (
                        f"{submodel_endpoint}/submodel-elements/"
                        f"{id_short_path}/$value"
                    ),
                },
            })
            return unique_id
        ctx.aas_polling_sources.append(aas_entry)
        return unique_id

    def _add_opcua_source(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                          source: ResolvedSource) -> str | None:
        host, port, base_path = parse_endpoint(source.base)
        if not host or not port:
            logger.warning("mapping %s: source %s has incomplete OPC UA base %r; skipping",
                           mapping.id_short, source.source_id, source.base)
            return None
        if not base_path.rstrip("/"):
            # A bare root ("/" or no path at all) carries no service location;
            # an invented "/opcua/server" produced wrong endpoints silently.
            logger.warning("mapping %s: OPC UA source %s declares no service path "
                           "in base %r (e.g. .../opcua/server); skipping",
                           mapping.id_short, source.source_id, source.base)
            return None
        unique_id = ctx.register_entity(
            self._source_entity_id(mapping, source.source_id),
            f"opcua source {source.source_id} of mapping {mapping.id_short}")
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "sourceId": source.source_id,
            "delivery": source.delivery,
            "serverUrl": host,
            "serverPort": port,
            # milo assembles opc.tcp://host:port/<pathToService> itself — a
            # leading slash would double it (deploy/config example: "milo")
            "pathToService": base_path.lstrip("/"),
            "nodeInformation": source.node or (source.topic or ""),
            "username": "",
            "password": "",
        }
        if source.delivery == "sync" and source.polling_interval:
            # the DMP's timer route polls at the declared cadence; the milo
            # subscription publishing interval below is the transport detail
            entry["pollingIntervalMs"] = int(source.polling_interval * 1000)
        if source.polling_interval:
            # per-source reader cadence override (declared); nothing invented
            entry["parameters"] = {
                "requestedPublishingInterval": str(int(source.polling_interval * 1000)),
            }
        if source.value_path:
            entry["valuePath"] = source.value_path
        ctx.opcua_sources.append(entry)
        _device_schema_fields(entry, source)
        return unique_id

    def _add_plc4x_source(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                          source: ResolvedSource) -> str | None:
        host, port, service_path, options = parse_endpoint_options(source.base)
        if not host or not port:
            logger.warning("mapping %s: source %s has incomplete PLC base %r; skipping",
                           mapping.id_short, source.source_id, source.base)
            return None
        driver = self.PLC4X_DRIVERS.get(source.protocol, source.protocol)
        address = source.topic or source.modbus_entity or ""
        unique_id = ctx.register_entity(
            self._source_entity_id(mapping, source.source_id),
            f"plc4x source {source.source_id} of mapping {mapping.id_short}")
        plc_entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "sourceId": source.source_id,
            "delivery": source.delivery,
            "serverUrl": host,
            "serverPort": port,
            "driver": driver,
            "servicePath": service_path,
            "options": options,
            "tags": [{"name": source.data_point or source.source_id, "value": address}],
        }
        if source.delivery == "sync" and source.polling_interval:
            plc_entry["pollingIntervalMs"] = int(source.polling_interval * 1000)
        if source.value_path:
            plc_entry["valuePath"] = source.value_path
        _device_schema_fields(plc_entry, source)
        ctx.plc4x_sources.append(plc_entry)
        return unique_id

    def _add_http_source(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                         source: ResolvedSource) -> str | None:
        """REST-interface properties poll like HTTP GETs; sync sources carry
        their declared polling interval (the evaluation driver)."""
        if not source.base:
            logger.warning("mapping %s: HTTP/REST source %s has no endpoint base; skipping",
                           mapping.id_short, source.source_id)
            return None
        host, _, base_path = parse_endpoint(source.base)
        base = urlsplit(source.base)
        if not host or base.scheme not in ("http", "https"):
            logger.warning("mapping %s: HTTP/REST source %s has invalid base %r; skipping",
                           mapping.id_short, source.source_id, source.base)
            return None
        href = urlsplit(source.topic or "")
        if href.scheme or href.netloc or href.fragment:
            raise ValueError(f"mapping {mapping.id_short} source {source.source_id}: "
                             f"HTTP href {source.topic!r} must be a relative path without fragment")
        if href.query:
            parse_qsl(href.query, keep_blank_values=True, strict_parsing=True)
        path = base_path.rstrip("/")
        if href.path:
            path += "/" + href.path.lstrip("/")
        url = f"{base.scheme}://{base.netloc}{path or '/'}"
        if href.query:
            url += f"?{href.query}"
        unique_id = ctx.register_entity(
            self._source_entity_id(mapping, source.source_id),
            f"http source {source.source_id} of mapping {mapping.id_short}")
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "sourceId": source.source_id,
            "delivery": source.delivery,
            "serverUrl": url,
        }
        if source.delivery == "sync" and source.polling_interval:
            entry["pollingIntervalMs"] = int(source.polling_interval * 1000)
        if source.value_path:
            entry["valuePath"] = source.value_path
        _device_schema_fields(entry, source)
        ctx.http_sources.append(entry)
        return unique_id

    # -- sinks (per protocol) ---------------------------------------------- #
    def _add_mqtt_datasink(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                           sink: ResolvedSink, sink_id: str, *,
                           entity_scope: str | None = None) -> str | None:
        """Registers an MQTT datasink (paho producer) from a resolved AID sink.

        Coarse (no authored ``value_path``): the DMP publishes the whole
        ``SinkId`` value from the transform's return object. Fine (ADR-014 /
        SPEC §6.5.4): the DMP constructs the outgoing payload with the value
        at the pinned JSON Pointer."""
        if not sink.base:
            logger.warning("mapping %s: MQTT sink %s has incomplete endpoint; skipping",
                           mapping.id_short, sink_id)
            return None

        host, port, base_path = parse_endpoint(sink.base)
        if not host or not port:
            logger.warning("mapping %s: sink %s has incomplete MQTT base %r; skipping",
                           mapping.id_short, sink_id, sink.base)
            return None
        if self.mqtt_broker:
            host, port = self.mqtt_broker

        topic = sink.topic or ""
        if topic and not topic.startswith("/"):
            topic = "/" + topic
        full_topic = ((base_path.rstrip("/") + topic) if base_path else topic).lstrip("/")

        entity_id = (f"{entity_scope}_{sink_id}_sink" if entity_scope is not None
                     else self._sink_entity_id(mapping, sink_id))
        unique_id = ctx.register_entity(
            entity_id,
            f"mqtt sink {sink_id} of mapping {mapping.id_short}")
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            # raw AIMC SinkId: extraction keys on this regardless of the
            # qualified uniqueId (ADR-017)
            "sinkId": sink_id,
            "serverUrl": host,
            "serverPort": port,
            "topic": full_topic,
            "clientId": f"dmp-{entity_id}",
            "userName": "",
            "password": "",
        }
        _mqtt_qualifier_fields(entry, sink)
        _device_schema_fields(entry, sink)
        if sink.value_path:
            entry["valuePath"] = sink.value_path
        ctx.mqtt_sinks.append(entry)
        return unique_id

    def _add_aas_sink(self, ctx: DescriptorContext, mapping: ResolvedMapping,
                      sink_ref: Any, sink_id: str, *,
                      entity_scope: str | None = None) -> str | None:
        path = reference_values(sink_ref)
        if len(path) <= 1:
            # Without element keys there is nothing to address inside the
            # submodel; emitting idShortPath=<submodel id> wrote garbage.
            logger.warning("mapping %s: AAS sink %s references no submodel element "
                           "(keys=%s); skipping", mapping.id_short, sink_id, path)
            return None

        submodel_id = _substitute(path[0], self.aas_id, self.unit)
        b64 = base64.urlsafe_b64encode(submodel_id.encode("utf-8")).decode("ascii").rstrip("=")
        submodel_endpoint = f"{self.aas_base_url}/submodels/{b64}"
        # The reference addresses the element directly: a Variables/Parameters
        # entry is a plain Property unless it groups children (then it is an
        # SMC and the authored keys already descend into it), so no legacy
        # '.variable' compatibility suffix is applied.
        id_short_path = "/".join(path[1:])

        entity_id = (f"{entity_scope}_{sink_id}_sink" if entity_scope is not None
                     else self._sink_entity_id(mapping, sink_id))
        unique_id = ctx.register_entity(
            entity_id,
            f"aas sink {sink_id} of mapping {mapping.id_short}")
        ctx.aas_sinks.append(
            {
                "uniqueId": unique_id,
                # raw AIMC SinkId (ADR-017); the AAS sink extracts it via
                # valuePath itself (handlesValuePathInternally)
                "sinkId": sink_id,
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

    def _add_http_datasink(self, ctx: DescriptorContext, scope: str,
                           sink: ResolvedSink, origin: str) -> str | None:
        """Registers a camel-http producer sink (``httpdatasink.json``): the
        scheme/host/port come from the affordance base, the path from base
        path + href. Serves HTTP actions AND IO-Link over Profinet REST."""
        if not sink.base:
            logger.warning("%s: no endpoint base; skipping", origin)
            return None
        m = _SCHEME_RE.match(sink.base)
        scheme = (m.group(1).lower() if m else "http") or "http"
        host, port, base_path = parse_endpoint(sink.base)
        if not host or not port:
            logger.warning("%s: incomplete HTTP base %r; skipping", origin, sink.base)
            return None
        path = sink.topic or ""
        if path and not path.startswith("/"):
            path = "/" + path
        href = urlsplit(path)
        full_path = (base_path.rstrip("/") + href.path) if base_path else href.path
        if not full_path:
            logger.warning("%s: empty request path; skipping", origin)
            return None
        query = href.query
        if query:
            parse_qsl(query, keep_blank_values=True, strict_parsing=True)
        unique_id = ctx.register_entity(f"{scope}_sink", origin)
        entry = {
            "uniqueId": unique_id,
            "sinkId": sink.sink_id,
            "scheme": scheme,
            "serverUrl": host,
            "port": port,
            "path": full_path,
        }
        if query:
            entry["query"] = query
        if sink.value_path:
            entry["valuePath"] = sink.value_path
        _device_schema_fields(entry, sink)
        ctx.http_sinks.append(entry)
        return unique_id

    def _add_opcua_datasink(self, ctx: DescriptorContext, scope: str,
                            sink: ResolvedSink, origin: str,
                            *, method_node: str | None = None) -> str | None:
        """Registers a milo-client sink (``opcuadatasink.json``).

        Default: the producer WRITES the transformed value to the affordance's
        node. With ``method_node`` the sink becomes a UA Method INVOCATION
        (OPC-10101 §7.1.3): the milo-client ``method`` option selects the UA
        ``Call`` service — ``node`` = the owning Object (``uav_componentOf``),
        ``method`` = the Method NodeId; the Call returns its OutputArguments
        inline (synchronous reply, no separate reply consumer)."""
        if not sink.base:
            logger.warning("%s: no endpoint base; skipping", origin)
            return None
        host, port, base_path = parse_endpoint(sink.base)
        if not host or not port:
            logger.warning("%s: incomplete OPC UA base %r; skipping", origin, sink.base)
            return None
        if method_node:
            # UA Method invocation: node = the OWNING OBJECT (uav_componentOf),
            # the Method NodeId rides the `method` endpoint option
            node = sink.component_of
            if not node:
                logger.warning("%s: UA Method invocation requires uav_componentOf "
                               "(OPC-10101 §7.1.3); skipping", origin)
                return None
        else:
            node = sink.node or (sink.topic or "")
        if not node:
            logger.warning("%s: no node information (uav_browsePath or href); "
                           "skipping", origin)
            return None
        unique_id = ctx.register_entity(f"{scope}_sink", origin)
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "sinkId": sink.sink_id,
            "serverUrl": host,
            "serverPort": port,
            "pathToService": base_path.lstrip("/"),
            "nodeInformation": node,
            "username": "",
            "password": "",
        }
        if method_node:
            entry["configuration"] = {"method": f"RAW({method_node})"}
        if sink.value_path:
            entry["valuePath"] = sink.value_path
        _device_schema_fields(entry, sink)
        ctx.opcua_sinks.append(entry)
        return unique_id

    def _add_plc4x_datasink(self, ctx: DescriptorContext, scope: str,
                            sink: ResolvedSink, origin: str) -> str | None:
        """Registers a plc4x write sink (``plc4xdatasink.json``) — the producer
        writes the transformed tag values (Modbus coils/registers via the
        modbus-tcp driver)."""
        if not sink.base:
            logger.warning("%s: no endpoint base; skipping", origin)
            return None
        host, port, service_path, options = parse_endpoint_options(sink.base)
        if not host or not port:
            logger.warning("%s: incomplete PLC base %r; skipping", origin, sink.base)
            return None
        driver = self.PLC4X_DRIVERS.get(sink.protocol, sink.protocol)
        address = sink.topic or sink.modbus_entity or ""
        if not address:
            logger.warning("%s: no tag address (href or modv_entity); skipping", origin)
            return None
        unique_id = ctx.register_entity(f"{scope}_sink", origin)
        entry: dict[str, Any] = {
            "uniqueId": unique_id,
            "sinkId": sink.sink_id,
            "serverUrl": host,
            "serverPort": port,
            "driver": driver,
            "servicePath": service_path,
            "options": options,
            "tags": [{"name": sink.data_point or sink.sink_id, "value": address}],
        }
        if sink.value_path:
            entry["valuePath"] = sink.value_path
        _device_schema_fields(entry, sink)
        ctx.plc4x_sinks.append(entry)
        return unique_id


def compile_descriptor(
    resolution: ResolutionResult,
    *,
    aas_base_url: str,
    aas_id: str | None = None,
    aas_id_short: str | None = None,
    unit_name: str | None = None,
    mqtt_broker: tuple[str, int] | None = None,
    aas_event_broker: tuple[str, int] | None = None,
    aas_event_topic: str = "submodel-events",
    aas_event_group: str | None = None,
) -> DmpDescriptor:
    """Convenience factory for :class:`DescriptorCompiler`."""
    return DescriptorCompiler(
        aas_base_url=aas_base_url, aas_id=aas_id, aas_id_short=aas_id_short,
        unit_name=unit_name, mqtt_broker=mqtt_broker,
        aas_event_broker=aas_event_broker, aas_event_topic=aas_event_topic,
        aas_event_group=aas_event_group,
    ).compile(resolution)
