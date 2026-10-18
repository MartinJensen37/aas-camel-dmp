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
  separate value feed.
- Ordinary and bridge mappings (SPEC §6, §7), Lua, the AAS interaction log
  (SPEC §10) and the AID 1.2 bindings for HTTP (including `longpoll`/`sse`
  observation), MQTT, OPC UA (including server-described structures) and
  Modbus (SPEC §5.2) are implemented as specified. Lua receives AAS elements
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

Tests tagged `integration` or `capacity` are skipped by a plain `mvn test`,
also when selected with `-Dtest`; use the profile. All tests start embedded
brokers (KRaft Kafka, MQTT 5) and need no external services.
Do not run Maven while the IDE re-imports POMs: the IDE can rewrite
`target/classes` underneath a running build.

## Planned work

### 1. HTTP static headers

`htv_headers` is rejected ("not sent yet"); send the form's static headers on
polls, observations, writes and invokes. HTTP observe and events (`longpoll`,
`sse`) are done (SPEC §5.2); `websub` stays rejected until an inbound callback
endpoint is designed.

### 2. SNMP and CAN bindings (AID 1.2)

Implement the AID 1.2 SNMP (`…/AssetInterfacesDescription/1/2/SNMP`) and CAN
(`…/AssetInterfacesDescription/1/2/CAN`) bindings as AID 1.2 defines them
(their EndpointMetadata and form terms), the WoT bindings filling only what AID
leaves open. Today both are rejected with "no supported protocol binding".

- SNMP via `camel-snmp`: Read (GET, polled), Observe (traps), Write (SET).
- CAN via PLC4X's CAN drivers (e.g. CANopen), next to Modbus. Check first that
  the PLC4X 0.13.1 driver reads and writes (the BACnet driver did not).

### 3. Fixtures and models regenerated from `aas-model`

- Regenerate the raw model fixtures in
  `dmp.component/src/test/resources/runtime-model-contracts/` from `aas-model`
  (AID 1.2) instead of migrating them by script. Compile outcomes may change;
  each changed outcome is reviewed, never regenerated blindly.
- Parent workspace: regenerate the AAS descriptions (bases now end with `/`,
  hrefs are relative); this includes
  `AASDescriptions/Resource/configs/syntegonStoppering.json`, which still
  carries a pre-ADR-022 AID block. The consumers of
  `mqv_control_packet`/`mqv_response_topic` follow.

### 4. TimeSeries sinks (InfluxDB)

A Sink referencing a TimeSeries submodel (IDTA 02008) `LinkedSegment` writes
only to InfluxDB (measurement `timeseries`, tags `submodel`, `segment`); a Sink
referencing a Property writes the AAS as today. The segment's
`Endpoint`/`Query` are authored by `aas-model` (the DMP never writes models);
the compiler blocks the mapping if the endpoint does not match the configured
InfluxDB. Work: the sink in the compiler and connector, `aas-model` TimeSeries
generation, a compose `influxdb` service, tests with a containerised InfluxDB.

### 5. Container image platforms

`docker.target.platforms` lists `linux/arm/v7`, but `eclipse-temurin:21-jre`
publishes no `linux/arm/v7` image (only amd64, arm64, ppc64le, s390x). Decide
whether arm/v7 is still needed; drop it, or choose another base for it.

### 6. Parent workspace

- `labServer-compose.yml:260-272` still defines the standalone DataBridge
  image and config mount: migrate it to the `dmp` service.
- The Python `OperationDelegation` service is what the DMP replaces; retire it
  once its operations run through the DMP. ARSO is a side project and out of
  scope.
- Correct stale prose in `Registration_Service/Dockerfile:3-9` and
  `aas-model/tests/test_schema_lockstep.py:36-40`.

### 7. Small fixes

- A failed PLC4X request is reported as "the device could not be reached"
  without its cause.

## Deferred (keep in mind)

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
| Java 25 (E4) | Stay on Java 21 (2026-10-18). Camel 4.22 supports 25; PLC4X 0.13.1 is tested to 24; Milo and LuaJ make no statement. |
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
