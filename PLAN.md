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
mvn -o clean test            # full offline reactor
```

Last full run (2026-10-15): 520 tests, no failures, errors or skips. Integration tests
start embedded brokers (KRaft Kafka, MQTT 5) and need no external services.
Do not run Maven while the IDE re-imports POMs: the IDE can rewrite
`target/classes` underneath a running build.

## Planned work (designs agreed 2026-10-15, not implemented)

### P1. Two-phase actions: trigger, then wait for completion

**Goal:** Operations on devices whose actions are not request/reply: a
Modbus/PLC4X tag write that starts an action, or an OPC UA Method that returns
at once while the action keeps running. The DMP sends the trigger, then watches
a status signal until the action has finished.

**AIMC contract (bridge mappings only)**

- Source: the Operation (unchanged). Sink: the AID action affordance that
  triggers it. For PLC4X/Modbus this is now allowed when completion is declared.
- `ResponseSources` may reference **property affordances of the same AID**
  (status tags/variables), not only the action output. Each is acquired by Read
  at its `PollingInterval` (Modbus, PLC4X, OPC UA Read) or by Observe when the
  affordance supports it (OPC UA monitored item, MQTT).
- `ResponseTransformation` is mandatory. It is called as
  `aimc_main(sources, invocation)`: `sources` holds the current status values
  by SourceId; `invocation` holds `baseline` (the same values read once just
  before the trigger), `elapsed` (seconds) and `inputs` (the caller's
  variables). It returns `nil` while the action runs, a SinkId table when it
  has finished (the caller SinkId becomes the reply), or raises `error()` when
  it failed (502 `DMP_DEVICE_ERROR`). The baseline lets Lua require a change,
  so a "done" bit left over from the previous run does not end a new call.
  Existing one-argument scripts are unaffected.
- New DMP extension property on the mapping, `CompletionTimeout`
  (`xs:double`, seconds), required when completion sources are declared.
  Expiry answers 504 `DMP_DEVICE_TIMEOUT`. Needs the `aas-model`
  `DmpMappingConfiguration` extension and acceptance in `WireProjection`.
- One invocation in flight per action affordance, since tags carry no
  correlation. A concurrent call answers 409 `DMP_ACTION_BUSY`. MQTT
  request/reply keeps its correlation and is unaffected.
- A trigger that fails (write error, Method bad status) fails immediately; no
  completion wait, no retry.

**BaSyx limits (checked in `basyx-java-server-sdk` main, 2026-10-06):** the
repository invoke endpoint is always synchronous; the `async` flag is ignored
(`TODO #566` in `SubmodelRepositoryApiHTTPController`). Delegation is a
blocking `WebClient` POST with no timeout configured, so BaSyx waits as long as
the DMP does; the invoking client's HTTP timeout (e.g. BT_Controller) is the
practical limit and must exceed `CompletionTimeout`.

**Work:** compiler (completion sources, timeout, PLC4X invoke), a completion
loop in `SyncOperationProcessor` (baseline read, poll/observe, Lua per sample,
timeout, busy guard), Lua two-argument call, `aas-model` extension. Tests: a
fake Modbus/PLC4X endpoint and a Milo test server with a long-running Method.

### P2. AAS interaction log and TimeSeries sinks (InfluxDB)

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
| A1 | **Action enrichment**: extra AAS sources on a bridge mapping, sampled at invocation time and passed to the request Lua | A bridge mapping must have exactly one source (the Operation). |
| A2 | **Acknowledged writes to a different AAS Property**: correlated `ResponseSources`/`ResponseSinks` on ordinary mappings (terminal success/failure/timeout, no replay) | Response fields on ordinary mappings are rejected. |
| A3 | **Operation variables of other element types** (Range, MultiLanguageProperty, File, Blob, ReferenceElement) | Properties, collections and lists (nested) are supported (SPEC §7); other types block the mapping. |
| A4 | **Custom/structured OPC UA datatypes** in Method arguments and reads | Basic types and arrays of them only. |
| A5 | **Per-element array fan-out** via `valuePath` (apply a mapping to every array element) | Not expressible; Lua must loop. |
| A6 | **PLC4X/Modbus action invocation** | Planned as P1 (trigger write plus completion polling). |
| A7 | **OPC UA events and HTTP Observe** acquisition | Rejected at compile time. |
| A8 | **AID bindings for Kafka, ActiveMQ, SQL**; finishing the **Hono** consumer | Components exist but the compiler never selects them; Hono only logs messages. Alternative: delete these modules. |

### B. Runtime and operations

| # | Item | Today |
| --- | --- | --- |
| B1 | **Recover from a lost MQTT event session** (broker reset or expiry) without a restart, e.g. a targeted rescan on reconnect with a clean session | Events in the gap are lost until restart (SPEC §4.2). |
| B2 | **Multiple DMP instances / high availability** (leases, partitioned adoption scopes, edge placement) | Exactly one DMP per set of mappings; `DMP_MANAGED_AIMC_IDS` can partition manually. |
| B3 | **Shared MQTT client per broker** for data-plane sources (one subscription union, async fan-out) | One MQTT client per endpoint. |
| B4 | **Per-mapping memory isolation** for Lua | Heap shared by all mappings. |
| B5 | **Lua blob provenance** (signing or trusted-author checks) | Any Lua in a readable AIMC runs. |
| B6 | **Remove the residual `ProducerTemplate` device dispatch** in the invocation adapters (HTTP, OPC UA, MQTT) in favour of plain route steps | Still dispatched from processors. |
| B7 | **Credential redaction audit** of data-plane log lines and caller error details (connector URIs can carry credentials) | Redaction exists for status, fingerprints and settings; data-plane paths are not audited. |

### C. Verification gaps

| # | Item |
| --- | --- |
| C1 | Live staging on the lab: Occupy/Halt and granular Operations, cross-AAS references, a mixed sync+async fan-in mapping. |
| C2 | Kafka `SASL_SSL` with `PLAIN` is not integration-tested (SCRAM over TLS and PLAIN without TLS are). |
| C3 | Performance and capacity evaluation (mapping count, heap, sockets, latency) was deferred by decision to one final evaluation. |
| C4 | The raw model fixtures in `dmp.component/src/test/resources/runtime-model-contracts/` were generated by the removed Python node. Decide how they are maintained (hand-edited, or generated from `aas-model`). |
| C5 | `ManagedPackagedRuntimeIntegrationTest.brokerOutageDropsCachedHealthAndSameEndpointRecoveryDoesNotReplayActions` is flaky: once in three full runs, managed shutdown exceeded its deadline while the Kafka broker was down (the `dmp-value-feed` executor was force-stopped). Decide whether shutdown during a broker outage should be faster or the deadline longer. |

### D. Deployment and migration (parent workspace)

| # | Item |
| --- | --- |
| D1 | Cut over any deployed stack still running the Python manager and per-asset DMPs: stop old owners, migrate persisted `invocationDelegation` qualifiers to `http://dmp:8080/...`, start one Java DMP, with a rollback path. |
| D2 | `AASDescriptions/Resource/configs/syntegonStoppering.json` still carries a pre-ADR-022 AID block; stations pin `syntegonStoppering.main.json` instead. Fix or delete the stale file. |
| D3 | ARSO and `OperationDelegation` author delegation URLs separately; decide whether their assets are in scope. |

### E. Library audit

| # | Item |
| --- | --- |
| E1 | `luaj-jse` 3.0.1 (2015) is unmaintained. Its process-wide string metatable let one blob change string methods for all others; fixed in `6167ad5`. Candidates: **Cobalt** (pure Java, LuaJ-derived, maintained with CC: Tweaked, sandbox-oriented, but no API stability promise) and **luajava** (real Lua 5.1–5.4/LuaJIT via JNI; a native crash takes down the whole DMP). Next step if wanted: run the Lua contract tests against Cobalt. |
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
