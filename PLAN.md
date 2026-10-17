# aas-camel-dmp — Plan

Companion to [SPEC.md](SPEC.md). This file holds the current state and the open
work only. The previous plan (milestones M0–M11, workstreams W0–W6, phases
R0–R8 and their verification logs) is in the git history before `8fe9c46`.

## Current state (2026-10-15)

- The DMP is one Java process (SPEC §3). The Python management node,
  per-asset containers, standalone file configuration, examples, `--compare`
  mode and BaSyx v1/VAB are removed.
- Discovery is need-to-know with an event-only lifecycle (SPEC §4); Kafka and
  MQTT event transports both work; event-sourced AAS Property values use a
  separate value feed.
- Ordinary and bridge mappings, Lua, all AID-reachable connectors and the
  health/status endpoints are implemented as specified.
- Build: Java 21, Camel 4.22, AAS4J 2.0.1, Kafka clients 4.3.1, MQTT 5 only,
  JUnit 5 only. Only `dmp.component` ships a logging backend.
- The parent workspace's main/E2E Compose stacks select one `dmp:java`
  service with the fixed delegation origin `http://dmp:8080`.

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

### Retirement revalidation (2026-10-16, repository-only)

The October 14 retirement-readiness prompt and parent audit are historical, not
instructions to repeat the removal. At audited HEAD `34bba25`, commit `8fe9c46`
already removed all 51 tracked Python management-node files. Later commits removed
file-configured examples/deploy samples (`3cc40c0`), file configuration factories
and loaders, standalone startup and `--compare`. Do not restore those interfaces,
the removed SQL example or legacy compiled Lua trees to satisfy an older checklist.

Current parent main/E2E source definitions select one Java `dmp` service; the chosen
AAS-reachable origin is `http://dmp:8080`. This is source-level configuration, not
evidence of running ownership or migrated persisted qualifiers. Retained Python
authoring, registration, simulations and independent services are not the removed
DMP management plane.

The origin is implemented through authored Operation qualifiers, not a verified
global BaSyx delegator setting. `aas-model/src/aas_model/invocation_authoring.py` finalizes encoded
owner/skill segments; generic ID injection skips invocation qualifiers. All four
current parent station profiles already use the shared origin. Registration rejects
the retired `delegation_base` field but deliberately preserves explicit URL strings;
it does not enforce the shared origin or reject every remaining invocation macro.
Decide that validation policy (D5) before treating registration as an origin guard.

Bounded remaining retirement work:

- Classify `labServer-compose.yml:260-272`, which still defines the old standalone
  DataBridge image and config mount. It is not included by the current root Compose
  entry point; decide whether to migrate or explicitly retire that deployment file
  before using it. Its existence does not establish a running legacy owner.
- Correct stale cross-repository prose in `Registration_Service/Dockerfile:3-9`
  and `aas-model/tests/test_schema_lockstep.py:36-40` in a separately scoped change;
  these references are comments/assertion text, not runnable manager dependencies.
- Resolve fixture maintenance (C4). The manifest explicitly records the deleted
  generator as capture provenance; keep those historical hashes. Decide a forward
  maintenance workflow without regenerating expected outcomes to hide differences.
- Complete separately authorized deployed-state inventory and exclusive cutover
  where necessary (D1); resolve stale/unscoped authoring paths (D2/D3).

No builds, imports, live probes, asset writes or performance evaluations were run
for this revalidation. Existing uncommitted action-phase work is preserved; earlier
green logs do not certify those current edits. **Repository manager removal is
complete; live-cutover readiness is not established by this audit.**

## Planned work (designs agreed 2026-10-15, not implemented)

### P3. Action invocation (rescoped 2026-10-17)

**Scope (SPEC §7.1):** an operation mapping binds its Operation 1:1 to one
affordance: an action whose terminal reply is its result — OPC UA Method,
MQTT 5 request/reply (the first reply carrying the request's correlation
data; no `State: RUNNING` skip), HTTP — or a property it writes, answering
once the connector acknowledged the write. On the AAS side it fans in further
AAS elements and fans the result out through ResponseSinks. Invocations are
correlated by their protocol and run concurrently.

**Rescoped out (2026-10-17):** the status phase (`StatusSources`,
`StatusTransformation`, `StatusTimeout`), parameter sinks, read-once
ResponseSources, device ResponseSinks (cleanup writes) and PLC4X actions,
together with the planned WoT asynchronous actions (`queryaction`,
`cancelaction`, concurrency flags). Simulating actions from writes, polls and
reads (Modbus coils, OPC UA Programs) is left to the higher-level control,
which orchestrates them through ordinary mappings. These parts were built and
tested (commit `250a89b`) and removed again to keep the DMP's scope bounded.

**Kept from the work:** the `dmp-plc4x:` and `dmp-milo-client:` components,
OPC UA per OPC 10101 (observed variables as monitored items, `uav_componentOf`
on the action), OPC UA events (`OpcuaEvent`, event monitored items; tested
end to end as ordinary Sources in `OpcUaSubscriptionSourcesTest`), WoT event
affordances and their builders in `aas-model`, the MQTT reply consumer owned
by its endpoint, connector invocation steps (B6) and the command route behind
a Content Enricher.

**Done (2026-10-17):**

- Operations bound to a property, AAS fan-in and fan-out
  (`OperationPropertyWriteTest` with Milo; `OperationConnectorsTest` for
  Modbus via PLC4X, HTTP including a rejected write, and MQTT). OPC UA writes
  wait for the Write status and convert scalars to the variable's DataType.
- Concurrency: eight parallel calls of one MQTT 5 action, answered in reverse
  order, each get their own reply (`OperationConnectorsTest`).
- No `ProducerTemplate` dispatch is left in operation mappings: per-sink
  transformer chains run as Content Enrichers, and the AAS response writes run
  in a write route behind the bounded Wire Tap
  (`ResponseSinkFanOutProcessor.configureDispatch`/`configureWrite`).
- Found on the way: PLC4X tag addresses kept the href's leading `/`
  (`/coil:1`), which PLC4X silently dropped from the request; superseded by
  the WoT Modbus translation (P5). A failed PLC4X request is still reported as "the device could not
  be reached" without its cause.

**Open:**

- **Future:** AAS asynchronous invocation (`invoke-async` with a handle;
  BaSyx does not implement it yet) for actions longer than a caller's HTTP
  timeout.

### P5. Follow the WoT protocol bindings under the hood (reviewed 2026-10-17)

The AID should be authored as the W3C WoT binding templates (and OPC 10101)
describe; the compiler translates those forms into connector URIs, so
connector syntax (PLC4X tag strings, Camel options) never appears in an AID.
Reviewed against the editor's drafts in `w3c/wot-binding-templates` (HTTP,
MQTT, Modbus, BACnet; CoAP, PROFINET, EtherNet/IP, LoRaWAN are not AID
protocols) and OPC 10101. IO-Link, SNMP and CAN have no W3C binding; the IDTA
vocabulary is the only reference there.

**Modbus** (done 2026-10-17, SPEC §5.2: WoT forms are translated into PLC4X
tags; PLC4X-syntax hrefs and `tcp://` bases are rejected. Remaining: `aas-model`'s
`test_serde` authors a `modbus://` base and a `/Coil,1` href, which are not WoT):

| | WoT Modbus binding | DMP today |
| --- | --- | --- |
| Base | `modbus+tcp://host:port` | `tcp://host:port`, PLC4X options in the query (`?unit-identifier=2`) |
| href | `/{unitID}/{address}?quantity={n}`, protocol address | a PLC4X tag string (`/coil:1`, `holding-register:1:INT`), 1-based (`PROTOCOL_ADDRESS_OFFSET = 1`): off by one against WoT |
| `modv_entity` | Coil, DiscreteInput, HoldingRegister, InputRegister | misused as a PLC4X address |
| `modv_function` | readCoil, readHoldingRegisters, writeSingleCoil, …; ignored with `modv_entity` | rejected |
| Defaults | readproperty → readCoil; writeproperty, invokeaction → writeSingleCoil | none; the address must name the memory area |
| `modv_type`, quantity | xsd payload type; quantity default 1 | rejected; PLC4X type suffix and `[n]` |
| `modv_mostSignificantByte`/`Word` | default true | rejected (PLC4X's big-endian default matches) |
| `modv_pollingTime`, `modv_timeout` | observe polling; timeout, default infinite | rejected; AIMC `PollingInterval` |
| `modv_zeroBasedAddressing` | deprecated, default false | rejected |

The table above describes the state before 2026-10-17.

**MQTT:**

- The IDTA term is `mqv_controlPacket` (W3C semantic id); the DMP and
  `aas-model` use `mqv_control_packet` with a DMP id, so a standard IDTA MQTT
  form is rejected.
- WoT control packets describe the consumer (read/observe/subscribeevent →
  subscribe; write/invoke → publish); ours describe the device (property:
  publish, action: subscribe), inverted. The compiler only checks the value
  set, but the `aas-model` builders author the inverted values.
- `mqv_retain` and `mqv_qos` exist in the IDTA template with W3C ids;
  `aas-model` redefines them with DMP ids.
- WoT allows readproperty as a subscription to the retained message (the form
  should also offer observeproperty); we reject it for sources.
- `publishproperty` is accepted but is not a WoT operation.
- Topic: the newest W3C draft moves it to `mqv:topic`/`mqv:filter` with href
  = broker; IDTA keeps href = topic relative to the base, which we follow.
- Request/reply (`mqv_request_reply`, `mqv_response_topic`) is our extension
  (the binding has none); keep it.

**HTTP:** writeproperty defaults to PUT in WoT, we require POST (a PUT form is
rejected); honour `htv_methodName` with the WoT defaults. observeproperty
means long polling or SSE (`subprotocol` longpoll/sse); we accept it with GET
and poll: reject it until a subprotocol is implemented (A7).

**BACnet** (not usable with conformant forms): WoT href
`bacnet://<device-instance>/<objectType>,<instance>[/<property>]` (default
present-value), `contentType` always `application/octet-stream` (we reject
non-JSON), services per op (ReadProperty, WriteProperty, SubscribeCOV;
commandPriority 16) via `bacv:usesService` (the IDTA template writes
`bacv_useService`); we require an IP base, a PLC4X address and reject every
`bacv_*` term. Check what PLC4X `bacnet-ip` supports before designing.

**OPC UA** (aligned since 2026-10-17): op → service mapping, `/?id=` hrefs,
`application/octet-stream`, `uav_componentOf`. Suspected bug:
`uav_browsePath` is passed to Milo as a NodeId instead of being resolved
(TranslateBrowsePathsToNodeIds).

### P2. AAS interaction log and TimeSeries sinks (InfluxDB)

**Status:** the interaction log is implemented (SPEC §10). TimeSeries sinks
are not. Known gaps accepted for now (2026-10-15): no gap markers or sequence
numbers, write-time timestamps that can collide within a series, no starting
snapshot beyond the first `dmp_mapping` and read points.

Supersedes the parent plan `.kilo/plans/1788502800000-dmp-wire-tap-telemetry-plan.md`
(2026-09-30) where they differ.

**Decisions (2026-10-15):**
- **Complete, never blocking.** Every AAS-side interaction is logged; there is
  no per-Source/Sink opt-out (the planned `Logging` flag is dropped). Points go
  through a bounded in-memory queue and batched writer. During an InfluxDB
  outage the data path keeps working; dropped points are counted and shown in
  `/management/status` (and the last successful write time). No file spool.
- **Per-sink choice of destination.** A Sink that references a TimeSeries
  submodel (IDTA 02008) segment writes only to InfluxDB; a Sink that references
  a Property writes the AAS as today. Both are logged.

**What is logged (one measurement each):**

| Measurement | When |
| --- | --- |
| `aas_read` | AAS Property/collection poll read (value, duration, HTTP status) |
| `aas_event` | Value-feed event consumed by a source |
| `aas_write` | AAS write, including response-sink writes (value, status) |
| `aas_invoke` | Operation call accepted (input variables, caller kind) |
| `aas_reply` | Operation answered (output variables, HTTP status, error code, duration) |
| `dmp_mapping` | Mapping installed, replaced, blocked or removed (fingerprint, category) |

- Tags (low cardinality): `submodel` (ID), `path` (idShortPath), `aimc`,
  `mapping`, `outcome`; `aas` (idShort) only for Operations, taken from the
  delegation path. Shells are never read, so the old `aas` ID tag and the
  `dmp_{aasIdShort}` measurement are gone.
- Fields: `invocation` (correlates `aas_invoke`/`aas_reply`), `durationMs`,
  `httpStatus`, `errorCode`, `semanticId`, and the value in a type-specific
  field (`value_num`, `value_bool`, `value_str`, `value_json` for collections
  and lists), because InfluxDB fixes a field's type per measurement.
- Settings: `DMP_AUDIT_INFLUX_URL`, `…_TOKEN` (secret), `…_ORG`, `…_BUCKET`,
  `…_QUEUE_SIZE`. Without a URL the log is off and status says so.

**TimeSeries sinks:** a Sink referencing a TimeSeries `LinkedSegment` compiles
to an InfluxDB writer (measurement `timeseries`, tags `submodel`, `segment`).
The segment's `Endpoint`/`Query` are authored by `aas-model` (the DMP never
writes the AAS); the compiler blocks the mapping if the endpoint does not match
the configured InfluxDB.

**Work:** an audit writer in `dmp.camel-aas` called at each AAS boundary
(polling consumer, value feed, AAS producer, Operation request/reply, response
writes) and from reconciliation; TimeSeries sink in the compiler and
connector; settings and status; `aas-model` TimeSeries generation; compose
`influxdb` service. Tests with an embedded or containerised InfluxDB.

## Open items to reconsider

Each item was part of the spec or an earlier plan and is **not implemented**.
Decide per item: keep (and schedule), or drop (and delete it from SPEC).

### A. Mapping features

| # | Item | Today |
| --- | --- | --- |
| A1 | **Action enrichment**: extra AAS sources on a bridge mapping, sampled at invocation time and passed to the request Lua | Implemented as P3 fan-in (SPEC §7.1). |
| A2 | **Acknowledged writes to a different AAS Property**: correlated `ResponseSources`/`ResponseSinks` on ordinary mappings (terminal success/failure/timeout, no replay) | Response fields on ordinary mappings are rejected. |
| A3 | **Operation variables of other element types** (Range, MultiLanguageProperty, File, Blob, ReferenceElement) | Properties, collections and lists (nested) are supported (SPEC §7); other types block the mapping. |
| A4 | **Custom/structured OPC UA datatypes** in Method arguments and reads | Basic types and arrays of them only. |
| A5 | **Per-element array fan-out** via `valuePath` (apply a mapping to every array element) | Not expressible; Lua must loop. |
| A6 | **PLC4X/Modbus action invocation** | Out of scope (P3 rescoping): a tag write reports no result; the higher-level control sequences writes and reads. |
| A11 | **Ordinary PLC4X sinks** sent the bare Lua value, which camel-plc4x rejects (it needs `{tag: {address: value}}`). | Fixed by the `dmp-plc4x:` component (SPEC §9), whose producer takes the tag value and acknowledges the write. |
| A7 | **HTTP Observe** acquisition | Rejected at compile time. OPC UA events are implemented (P3). |
| A8 | **AID bindings for Kafka, ActiveMQ, SQL**; finishing the **Hono** consumer | Components exist but the compiler never selects them; Hono only logs messages. Alternative: delete these modules. |
| A9 | **SNMP binding** (AID 1.2 `…/AssetInterfacesDescription/1/2/SNMP`) via Camel's `camel-snmp` component: Read (GET/poll), Observe (traps), Write (SET) | Not recognised: an SNMP interface is rejected with "no supported protocol binding". |
| A10 | **CAN binding** (AID 1.2 `…/AssetInterfacesDescription/1/2/CAN`) via PLC4X's CAN drivers (e.g. CANopen), alongside Modbus and BACnet | Not recognised: a CAN interface is rejected with "no supported protocol binding". |

### B. Runtime and operations

| # | Item | Today |
| --- | --- | --- |
| B1 | **Recover from a lost MQTT event session** (broker reset or expiry) without a restart, e.g. a targeted rescan on reconnect with a clean session | Events in the gap are lost until restart (SPEC §4.2). |
| B2 | **Multiple DMP instances / high availability** (leases, partitioned adoption scopes, edge placement) | Exactly one DMP per set of mappings; `DMP_MANAGED_AIMC_IDS` can partition manually. |
| B3 | **Shared MQTT client per broker** for data-plane sources (one subscription union, async fan-out) | One MQTT client per endpoint. |
| B4 | **Per-mapping memory isolation** for Lua | Heap shared by all mappings. |
| B5 | **Lua blob provenance** (signing or trusted-author checks) | Any Lua in a readable AIMC runs. |
| B6 | **Remove the residual `ProducerTemplate` device dispatch** in the invocation adapters (HTTP, OPC UA, MQTT) in favour of plain route steps | Done (P3): adapters contribute route steps (`configureInvoke`), run in a command route behind a Content Enricher. Per-sink transformer chains still use a `ProducerTemplate`. |
| B7 | **Credential redaction audit** of data-plane log lines and caller error details (connector URIs can carry credentials) | Redaction exists for status, fingerprints and settings; data-plane paths are not audited. |

### C. Verification gaps

| # | Item |
| --- | --- |
| C1 | Live staging on the lab: Occupy/Halt and granular Operations, cross-AAS references, a mixed sync+async fan-in mapping. |
| C2 | Kafka `SASL_SSL` with `PLAIN` is not integration-tested (SCRAM over TLS and PLAIN without TLS are). |
| C3 | Performance and capacity evaluation (mapping count, heap, sockets, latency) was deferred by decision to one final evaluation. |
| C4 | The raw model fixtures in `dmp.component/src/test/resources/runtime-model-contracts/` were generated by the removed Python node. Decide how they are maintained (hand-edited, or generated from `aas-model`). All fixtures are AID 1.1; regenerated ones from the current `aas-model` would be AID 1.2, which the DMP accepts since 2026-10-16. |
| C5 | `ManagedPackagedRuntimeIntegrationTest.brokerOutageDropsCachedHealthAndSameEndpointRecoveryDoesNotReplayActions` is flaky: once in three full runs, managed shutdown exceeded its deadline while the Kafka broker was down (the `dmp-value-feed` executor was force-stopped). Decide whether shutdown during a broker outage should be faster or the deadline longer. |
| C6 | Polled AAS Property sources (and fan-in reads) hand Lua the element JSON (`value` as lexical text); SPEC §5.3 says `GET …/$value`. Decide which is intended and align SPEC or `AASPollingConsumer`. |

### D. Deployment and migration (parent workspace)

| # | Item |
| --- | --- |
| D1 | Cut over any deployed stack still running the Python manager and per-asset DMPs: stop old owners, migrate persisted `invocationDelegation` qualifiers to `http://dmp:8080/...`, start one Java DMP, with a rollback path. |
| D2 | `AASDescriptions/Resource/configs/syntegonStoppering.json` still carries a pre-ADR-022 AID block; stations pin `syntegonStoppering.main.json` instead. Fix or delete the stale file. |
| D3 | ARSO still defaults delegation to `http://registration-service:8087`; the independent Python `OperationDelegation` service has its own routes. Decide whether their assets are in scope; do not delete unrelated Python or rewrite their fixtures merely to remove old host strings. |
| D4 | `labServer-compose.yml:260-272` still defines the legacy standalone DataBridge/config mount. Decide whether this separate deployment file is supported, then migrate or retire it; do not infer it is active from its presence. |
| D5 | Registration preserves explicit authored invocation URLs (including other origins) and does not reject every unresolved qualifier macro. Decide whether intended shared-DMP operations must enforce origin/path/macro constraints; current fixed-origin authoring is not a global registration invariant. |

### E. Library audit

| # | Item |
| --- | --- |
| E1 | `luaj-jse` 3.0.1 (2015) is unmaintained and no LuaJ fork is actively maintained. Its process-wide string metatable let one blob change string methods for all others; fixed in `6167ad5`. Cobalt is ruled out: its README promises no API stability and advises against general use. The only maintained alternative found is **luajava** (real Lua 5.1–5.4/LuaJIT via JNI), where a native crash takes down the whole DMP. Decision for now: stay on LuaJ, work around its defects in `LuaSandbox`, and revisit if a defect cannot be worked around. |
| E2 | `dmp.camel-hono` mixes Hono 1.x and 2.x clients and old test dependencies (only relevant if A8 keeps it). |
| E3 | Minor version lags needing a networked build: mariadb, postgresql, sqlite-jdbc, mysql-connector-j. |
| E4 | Java 25: blocked until Milo, PLC4X, LuaJ and the `linux/arm/v7` image are verified on it. |

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
