# aas-camel-dmp — Plan

Companion to [SPEC.md](SPEC.md). This file holds the current state and the open
work only; finished work is described in SPEC and the git history. The previous
plan (milestones M0–M11, workstreams W0–W6, phases R0–R8, the retirement audit
and the P3/P5 work logs) is in the git history.

## Current state (2026-10-18)

- The DMP is one Java process (SPEC §3). The Python management node,
  per-asset containers, standalone file configuration, examples, `--compare`
  mode and BaSyx v1/VAB are removed; the parent workspace's Compose stacks run
  one `dmp` service with the delegation origin `http://dmp:8080`.
- Discovery is need-to-know with an event-only lifecycle (SPEC §4); Kafka and
  MQTT event transports both work; event-sourced AAS Property values use a
  separate value feed. The parent workspace's stacks use Kafka only, BaSyx's
  MQTT eventing is off (2026-10-18; both carry the same submodel events).
- Ordinary and bridge mappings (SPEC §6, §7), Lua, the AAS interaction log
  (SPEC §10) and the AID 1.2 bindings for HTTP (including `longpoll`/`sse`
  observation and static `htv_headers`), MQTT, OPC UA (including
  server-described structures), Modbus and SNMP v1/v2c (SPEC §5.2) are
  implemented as specified. Lua receives AAS elements
  in one typed value form (SPEC §5.3) and OPC UA values without an envelope,
  checked against the AID (SPEC §5.2).
- Build: Java 21, Camel 4.22, AAS4J 2.0.1, Milo 1.1.6, PLC4X 0.13.1,
  LuaJ 3.0.1, Kafka clients 4.3.1, MQTT 5 only, JUnit 5 only. Only
  `dmp.component` ships a logging backend.

### Verification

```bash
mvn -o clean test                  # unit and route tests (about 2.5 min)
mvn -o clean test -Pintegration    # plus real Kafka, SASL/TLS and packaged-process tests (CI)
mvn -o test -pl dmp.component -Pcapacity   # stress test only; size with -Ddmp.stress.capacity
```

The image (`Dockerfile` at the root, built locally as `dmp:java`) builds the
jar from the sources, so `docker compose build dmp` needs no Maven step.
Tests tagged `integration` or `capacity` are skipped by a plain `mvn test`,
also when selected with `-Dtest`; use the profile. All tests start embedded
brokers (KRaft Kafka, MQTT 5) and need no external services.
Do not run Maven while the IDE re-imports POMs: the IDE can rewrite
`target/classes` underneath a running build.

## Planned work

### 1. Physical stations register `aas-model` exports

Every simulated station registers the Resource `aas-model` authors for it
(2026-10-21): `packml_runtime` publishes the export, with the AID base set to
the broker or OPC UA endpoint of the stack, and writes it to
`AASDescriptions/Resource/exports/`. Halt is served by the shared runtime and
only invokes. The physical stations (IEC 61499 PLCs, the Planar controller)
still register the old way; they are to publish a copy of their resource's
export (`aas-model/scripts/export_resource.py`).

### 2. TimeSeries sources and sinks (InfluxDB 2)

Agreed design (2026-10-21); InfluxDB only, Internal/External segments out of
scope.

**Model (`aas-model`).** `Variables` holds the groups `State`, `Measurements`
and `Log`. A group holds TimeSeries SMCs, or plain elements for data that is
not a time series (lists such as a process queue, files, images). A TimeSeries
SMC carries this project's semanticId with the IDTA TimeSeries id as a
supplemental id; everything inside it (`Metadata/Record`, `Segments`,
`LinkedSegment`, `Endpoint`, `Query`) keeps the IDTA 02008 ids. A TimeSeries
groups what is sampled together (one row = one sample), not one variable each.
The LinkedSegment carries a DMP extension `DmpTarget`: `Bucket`,
`Measurement` and `Tags`. The tags name the AAS that owns the series (an AIMC
writing another resource's series targets that resource's segment). Each
Record field carries an explicit `Column`, generated once equal to its idShort
and kept on rename; the DMP never derives a column from a name. `Query` is
generated from the same values for the web UI.

**Measurements.** One measurement per Record schema (`station_state`,
`dispensing_weight`, `operation_occupy`), assets told apart by tag. Tags have
low cardinality (never an invocation id or UUID). The compiler blocks Records
with different columns or types under one measurement; aas-model generating
them from templates is the real guarantee.

**DMP.** A Source or Sink targets a LinkedSegment; the DMP reads its
TimeSeries container (Record, `DmpTarget`), which counts as referenced for
events and fingerprints. Lua sees the row as a table keyed by Record idShort
(groups nested, `Position.X`); a missing field is not written, an unknown one
or a wrong type fails the sink. `Time` is optional: a Lua value is the point's
time, otherwise the evaluation time. Only UtcTime records can be written. A
Sink writes one point per evaluation; a Source is a Read with
`PollingInterval` returning the latest row (`last()` over a lookback range).
The Record never holds a current value.

**Logging.** Only what is authored: `Log` series written by ordinary sinks or
ResponseSinks. aas-model generates one typed series per Operation (Time,
Outcome, DurationMs and the Operation's variables). The response
transformation also runs on failure, with the error in its input; Lua gets a
read-only `meta` table (invocation id, duration, caller, outcome). SPEC §10,
`InfluxAuditLog`, `LineProtocol` and `DMP_AUDIT_INFLUX_*` are removed;
DMP-internal diagnostics go to the application log and `/management/status`.

**Camel.** `dmp-influxdb2:` extends `camel-influxdb2` 4.22 (producer only,
asynchronous `makeWriteApi()`, auto-creates buckets and orgs): writes through
`WriteApiBlocking` so failures fail the exchange, builds typed points from the
Lua row, reads the latest row with Flux, never creates buckets or orgs. One
`InfluxDBClient` per endpoint, credentials from the environment. One request
per row; batching only with per-batch acknowledgement.

**Web UI.** The TimeSeries plugin reads a TimeSeries submodel or SMC (children
from `value`, the group's idShortPath as prefix) and uses `Column` when
present.

**Work, in order:** `aas-model` (Variables groups, TimeSeries SMC,
`DmpTarget`, `Column`, Operation log series); the DMP (discovery of the
container, segment source/sink, `dmp-influxdb2:`, Lua `meta` and failure
responses, removal of §10, SPEC); a compose `influxdb` service; the web UI
plugin; migrating stations, BT_Controller, `stationctl`, the e2e tests and
kg-bridge to the new paths.

### 3. Parent workspace

- The main and e2e stacks (`compose/`) run the `dmp` service; DataBridge
  residue and the `labServer` setup are gone (2026-10-18).
- The Python `OperationDelegation` service is removed (2026-10-21); every
  operation runs through the DMP. ARSO is a side project and out of scope.

### 4. Small fixes

- A failed PLC4X request is reported as "the device could not be reached"
  without its cause.

## Deferred (keep in mind)

- **CAN** (AID 1.2 Annex E.2). Not IP-based: receiving it needs a CAN
  controller on the DMP's host wired to the bus, the bus brought up there, and
  the SocketCAN device handed to the container. That ties the central DMP to
  one machine on one bus; AID 1.2 itself leaves `base` unused for CAN and
  expects gateway or adapter hardware at the bus (§2.24.8). Integrate a CAN
  device through a gateway that bridges it to IP (e.g. MQTT), or later through
  an edge DMP instance on the bus (needs multi-instance, below). Worked out for
  that case (2026-10-18): `canv_offset`/`canv_length` are bits (ranges 0–63 and
  1–64); a field is an unsigned integer in Intel (little-endian) bit order, the
  value its raw integer plus `canv_valueOffset` (the binding shifts values so
  that payloads carry no negative numbers), `canv_enumeratedValues` maps raw
  integers to strings, `canv_payloadMapping` fields form one object; bits the
  AID does not describe are sent as 0. PLC4X 0.13.1 could carry the frames
  (generic CAN driver over SocketCAN, a virtual CAN transport for tests) but
  decodes only whole-byte types, so the bit fields are the DMP's to decode.
- **TimescaleDB** as a second TimeSeries dialect (InfluxDB only for now).
  Advantages: the latest value is an indexed `ORDER BY time DESC LIMIT 1`
  with no lookback window (InfluxDB `last()` needs a `range()`, so a state
  unchanged for longer than the window is not found, and `range(start: 0)`
  slows as history grows); a Record maps one to one onto a hypertable with
  typed columns. Costs: the tables need DDL from the Record, and the web UI
  plugin speaks only Flux (needs a REST layer such as PostgREST).
- **HTTP `websub`.** Rejected: it needs an inbound callback endpoint the
  device posts to. `longpoll`, `sse` and static `htv_headers` are done.
- **BACnet.** Rejected by the compiler: the PLC4X 0.13.1 `bacnet-ip` driver
  has `canRead`/`canWrite` false and only listens for COV notifications.
  Options: ReadProperty/WriteProperty/SubscribeCOV on PLC4X's generated BACnet
  model over UDP, a BACnet stack (BACnet4J: check its GPL licence), or drop
  it. AID addresses a device by instance number (`bacnet://{deviceId}`), so
  Who-Is/I-Am discovery is needed, with an optional address term on
  EndpointMetadata where broadcast does not reach.
- **AAS asynchronous invocation** (`invoke-async` with a handle; BaSyx does
  not implement it yet) for actions longer than a caller's HTTP timeout.
- **Runtime:** recovery from a lost MQTT event session without a restart;
  multiple DMP instances / HA (today `DMP_MANAGED_AIMC_IDS` partitions
  manually); a shared MQTT client per broker; per-mapping Lua memory
  isolation; Lua blob provenance (signing); a credential redaction audit of
  data-plane log lines and caller error details.
- **Verification:** live staging on the lab (Occupy/Halt and granular
  Operations, cross-AAS references, mixed sync+async fan-in); Kafka
  `SASL_SSL` with `PLAIN`; one final performance and capacity evaluation.
- **C5 flaky test:** `ManagedPackagedRuntimeIntegrationTest` once exceeded the
  managed-shutdown deadline after a Kafka outage; the deadline was raised from
  15 s to 45 s (2026-10-18). Reopen only if it recurs.

## Decided, not to be done

| Item | Decision (2026-10-18) |
| --- | --- |
| Acknowledgements written to AAS Properties (old A2) | No. A further Property is written through fan-out: an extra Sink on an ordinary mapping, a ResponseSink after the response transform on a bridge mapping. |
| Operation variables of other element types (A3) | No. Properties, collections and lists (nested) only. |
| Custom-encoded OPC UA datatypes (A4) | No. Structures the server describes are supported (SPEC §5.2, 2026-10-18); pre-1.04 DataTypeDictionaries only if a station needs them. |
| Per-element array fan-out via `valuePath` (A5) | No; Lua loops. |
| PLC4X/Modbus actions (A6) | No; the higher-level control sequences writes and reads (SPEC §7.1). |
| Kafka/ActiveMQ/SQL bindings, Hono (A8) | Dropped 2026-10-17; a protocol needed later returns as an AID binding with tests. |
| Invocation URL validation at registration (D5) | No. `aas-model` authors the final `invocationDelegation` URLs; registration stores the AAS unchanged and does not rewrite or validate them. |
| Deployed-stack cutover (D1) | Not needed; the Python manager is gone. |
| ARSO delegation defaults (D3) | Out of scope (side project). |
| AID checked against Modbus, MQTT or HTTP devices | No (2026-10-18). These devices do not describe themselves; only OPC UA values are checked against the server's DataTypes (SPEC §5.2). Unsupported schema terms are rejected instead. |
| `linux/arm/v7` image | Dropped (2026-10-18); the image is built for `linux/amd64` and `linux/arm64`. Inherited from the DataBridge build; `eclipse-temurin:21-jre` has no arm/v7 variant, the DMP runs on x86-64 servers, and current Raspberry Pis run arm64. |
| Java 25 (E4) | Stay on Java 21 for now (2026-10-18). The full suite, integration tests included, passes compiled for and run on Temurin 25.0.4; the base image exists for amd64 and arm64. On 25, Netty 4.1 (Milo) and the Kafka clients log `sun.misc.Unsafe` deprecation warnings; later JDKs plan to deny that access. Camel 4.22 supports 25; PLC4X 0.13.1 is tested to 24; Milo and LuaJ make no statement. |
| LuaJ replacement (E1) | Stay on LuaJ and work around its defects in `LuaSandbox` (as `6167ad5` did for the shared string metatable). Cobalt promises no API stability; luajava runs native code that can crash the JVM. Revisit only if a defect cannot be worked around. |

## Accepted limitations

These are deliberate and documented in SPEC; listed so they are not mistaken
for open work.

- No periodic repair; a missed event is only corrected by the next relevant
  event or a restart.
- Event-sourced AAS Property sources have no value until their first change.
- Device actions are never retried or replayed; extra response-sink writes are
  best effort.
- Property write-delegation (synchronous PUT with same-Property writeback) is
  retired.
