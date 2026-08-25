# aas-camel-dmp — Implementation Plan (next work items)

> Companion to `SPEC.md`. Status baseline verified 2026-08-24: M0–M7 committed and
> green (core 8 tests, lua 23 tests on Camel 4.22.0/Java 17-target; management-node 39
> pytest). Remaining milestone work = **M8** (coarse/fine, ADR-014) and **M9** (engine
> selection, ADR-015), plus completing **§6.4 fan-in** in the management node.

---

## 1. Baseline (what is already done, verified)

| Area | Where | Status |
|---|---|---|
| M0 rename, README, provenance | repo root | done |
| M0.5 Camel 4.22.0 / Java 17 | parent pom, all modules | done |
| M1 `databridge.camel-lua` sandbox/timeout/reload/harness | `databridge.camel-lua` | done |
| M2 DMP container config mount, `LUA_ENV_VAR_NAME` | `databridge.component`, `deploy/dmp` | done |
| M3 management node (parser/resolver/descriptor/orchestrator) | `management-node/` | done |
| M4 interface transforms (OPC UA canonical adapter) | `databridge.camel-opcua`, `deploy/interface-transforms` | done |
| M5 decision gate (plain containers) | — | done |
| M6 live integration | `e2e.managed_assets.json`, `deploy/dmp` | done |
| M7 REST operation delegation | `databridge.core/operation/*`, `restsources.json` | done |
| §6.4 fan-in/fan-out **route model** | `RouteConfiguration.datasources[]`, `EventRouteCreator` aggregator, `RouteConfigurationDeserializer`, `DataSourceConfiguration.sourceId` | done (tested) |
| Contract tests §8.4 #1–6 + compile-reuse | `databridge.camel-lua/.../LuaContractTest` | done |
| AAS sink coarse `valuePath` (extract own SinkId key) | `databridge.camel-aas/AASEndpoint.extractValuePath` | done |

Build/test commands:
- Java: `JAVA_HOME=/tmp/jdk17 PATH=/tmp/apache-maven-3.9.11/bin:$PATH mvn -o -pl databridge.core,databridge.camel-lua -am test` (the repo compiles with `--release 17`; the JDK at `/tmp/jdk17` provides the compiler — the system java-21 install is a JRE-only runtime with no `javac`).
- Python: `python3 -m pytest` in `management-node/`.

---

## 2. Open work items

### T1 — M8: Reference granularity, coarse vs fine (ADR-014, §6.5, §8)

Goal: a `Source`/`Sink` may resolve to a whole AID data point (**coarse**, default, current)
or to one nested property (**fine**); the management node pins a JSON Pointer (`valuePath`),
and the DMP applies it at the boundary — **disassembly** pre-transform (source), **assembly**
post-transform (sink). Contract tests §8.4 #7–10 become green.

#### T1.1 Shared JSON Pointer utility (`databridge.core`)
- New `org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer` (RFC 6901):
  object keys (`$.State` / `/State` / `$['State']`) and 0-based array indices
  (`$.WorkOrders[0].id`). `Optional<Object> extract(Object json, String pointer)`;
  missing path → empty (caller decides: coarse fallback vs clear route error per §8.4 #8).
  Also `Object assembleEntry(Map<String,Object> returnObj, String sinkId, String pointer)`
  used by sinks.

#### T1.2 Lua component: `valuePath` URI option (single-source disassembly)
- `LuaEndpoint`: add `@UriParam valuePath` (spec §8.3). Regenerated configurer/uri-factory
  classes are produced by the Camel annotation processor at build time.
- `LuaProducer.buildSources`: when `valuePath` set → `sources[SourceId] = JsonPointer.extract(payload)`.
  Missing path → `IllegalArgumentException` (clear route error, §8.4 #8). Absent → whole payload
  (coarse, current behavior).
- `LuaTransformerConfiguration`: add `valuePath`, emit `&valuePath=` in `getConnectionURI()`.
- `luatransformer.schema.json` + `luatransformers.json` example gain the optional field.

#### T1.3 Fan-in + fine (per-source disassembly) — design point, see §3.1
- Extend `MqttConsumerConfiguration` with optional `valuePath`.
- Apply it in `EventRouteCreator.startRouteDefinition`'s per-source `from()` route *before* the
  aggregator stores `latest[SourceId]`, so fan-in mixes whole-payload (coarse) and leaf (fine)
  values correctly (§8.4 #9). Single-source routes may keep either mechanism; the descriptor
  emits the datasource-level `valuePath` so both modes agree.

#### T1.4 Sink-side assembly
- AAS sink (`AASEndpoint.extractValuePath`): generalize from a bare top-level key to a JSON
  Pointer so a fine sink selects a nested field of the return object to write
  (`AASEndpoint.node.get(valuePath)`, §6.5.4). Keep the existing bare-key behavior working
  (that *is* the coarse sink: write own SinkId value).
- MQTT datasink (`MqttDataSinkConfiguration` → `paho:`): for a fine sink, construct the outgoing
  payload `{ "<leaf>": <SinkId value> }` at the pinned pointer (assembly); coarse = publish the
  `SinkId` value as-is. Verify current datasink body handling first.

#### T1.5 Management node
- `resolver.py`: detect fine vs coarse from the reference shape (a `Source`/`Sink` reference
  whose terminal key is a nested property inside a data point, aided by the AID data point
  `semantic_ids`/datatype); extend `ResolvedSource`/`ResolvedMapping` with an optional
  `value_path` per source **and** per sink. §6.1 interpretation: both share the same endpoint
  resolution; only the delivered payload differs.
- `descriptor.py`: emit `valuePath` on the datasource entry (T1.3) and on data sinks
  (AAS sinks: pointer instead of/in addition to the SinkId key; MQTT sinks: pointer).
  Add management-node tests for both granularities.

#### T1.6 Contract & regression tests
- `LuaContractTest` §8.4 #7 (coarse nested access in-blob), #8 (fine leaf, missing-path error),
  #9 (coarse+fine fan-in → `sources` mixes shapes), #10 (sink assembly).
- `EventRouteFanInFanOutTest` or a new test: per-source `valuePath` disassembly before aggregation.
- `descriptor`/`resolver` pytest for coarse/fine emit.

### T2 — M9: Transformation engine selection by semantic id (ADR-015, §7.6)

Goal: the AIMC `Transformation` blob's semantic id selects Lua (`aimc_main`, default) or
JSONata; the management node dispatches and the DMP instantiates `lua:` vs `jsonata:` in the
same dis/assembly pipeline (§8.4 #11).

- `parsers/aimc.py`: capture the Transformation element's `semanticId` +
  `supplementalSemanticIds` + `contentType` (raw BaSyx REST carries these on the Blob child —
  verified in `syntegon_stoppering_aimc.json`); expose `AimcMapping.transformation_semantic_ids`
  (and keep `transformation` text). Add engine-detection helper
  (JSONata semantic id → JSONata; AIMC default / Lua id → Lua).
- `resolver.py`: carry the engine onto `ResolvedMapping`.
- `descriptor.py`: emit `jsonatatransformer.json` + `queries/*.jsonata` with
  `queryLanguage: JSONATA` when the blob declares the JSONata semantic id, else the existing
  Lua path. JSONata is only emitted where the AAS declares it (never as the default path).
- `databridge.camel-jsonata`: verify `JsonataTransformerConfiguration`/`JsonataDefaultConfigurationFactory`
  are usable as-is for a routing dispatch (they are present), and that `jsonatatransformer.json`
  config loads (level of effort: confirm, patch only if needed).
- Contract test §8.4 #11: a JSONata blob and a Lua blob both run through the same SourceId/SinkId
  keyed contract.
- Management-node pytest: semantic-id dispatch (Lua vs JSONata).

### T3 — §6.4 fan-in compile from the management node (complete end-to-end)

The DMP core supports multi-source aggregation, but `descriptor._compile_route` still compiles
only `mapping.sources[0]` and emits the legacy singular `datasource` key.

- `descriptor.py::_compile_route`: iterate **all** `mapping.sources`; add each MQTT source to
  `mqttconsumer.json`; emit `"datasources": [ids...]` (plural, deserializer already accepts it);
  for multiple sources set the Lua transformer `sourceKeys` to the comma-separated SourceId list
  so `aimc_main` receives the aggregated `sources` map (§6.4 example: combine two sources in one
  math transform). Single-source mappings keep `datasource`/single `sourceKeys` (unchanged output).
- New pytest: two-source mapping → `routes.json` has `datasources:[...]`, `mqttconsumer.json`
  has both entries, `luatransformers.json` `sourceKeys` = `a,b`.

### T4 — Spec/doc hygiene (small, optional)
- `SPEC.md` header: "Status: Draft" + "Companion artifacts … (planned)" are stale now that the
  artifacts exist; README could list M8/M9. **Leave SPEC.md untouched unless asked** (the user has
  an uncommitted edit in the working tree — do not conflict).

---

## 3. Design decisions & open points

### 3.1 Where source-side fine `valuePath` is applied (single-source vs fan-in)
- **Single-source:** Lua producer `valuePath` URI option (exactly as §8.3/§8.5 specify).
- **Fan-in + fine:** the aggregated body is a map of *already-raw* payloads keyed by SourceId, so a
  single `lua:valuePath` cannot disassemble different sources at different paths. **Recommendation:**
  also apply the pointer per-source in the datasource/route layer (`EventRouteCreator`) before
  aggregation, with `valuePath` on `MqttConsumerConfiguration`. This is the only mechanism that
  satisfies §8.4 #9 correctly, and matches the §6.3 table wording ("on the datasource"). Keeps the
  Lua `valuePath` option for single-source/manual `lua:` use.
- *Flagging:* the spec uses both "on the datasource" (§6.3) and "transformer/adapter" (§8.2/8.3).
  The plan follows §6.3 for fan-in correctness and §8.3 for the single-source path. If the user
  prefers a strict single home, the datasource-level option is the superset.

### 3.2 Sink `valuePath` semantics for AAS
Current AAS `valuePath` doubles as (a) the key selector of the SinkId in the return object and
(b) the pointer into the incoming message body. Generalize `extractValuePath` to a JSON Pointer
while preserving bare-key behavior (existing AAS configs emit `valuePath: <SinkId>` and must keep
working).

### 3.3 Out of scope (explicitly deferred in SPEC §12/§6.5.1)
Shared MQTT subscriber (Option A/B), per-element array fan-out via `valuePath`, hard per-script
memory caps beyond `-Xmx`/process isolation, cluster/fleet deployment (M5 resolved plain containers).

---

## 4. Verification

1. Java: `mvn -o -pl databridge.core,databridge.camel-lua,databridge.camel-paho,databridge.camel-aas -am test`
   (all affected modules; full `mvn -o test` before finishing).
2. Python: `python3 -m pytest` in `management-node/` (all suites + new).
3. Manual end-to-end (optional, lab): regenerate `syntegonStopperingSystem` config via the
   management node and check a fine source at `$.State` yields `sources.StationState == payload.State`.
4. Harness: `LuaHarness` re-run on the updated blobs for both engines.

---

## 5. Suggested order

1. T1.1 JSON pointer util + tests (shared foundation).
2. T1.2 Lua `valuePath` + §8.4 #7/#8 tests (single-source fine).
3. T1.3 datasource-level `valuePath` + fan-in+fine test (§8.4 #9).
4. T1.4 sink assembly (AAS nested + MQTT payload construction) + §8.4 #10.
5. T1.5/T1.6 management-node fine detection/emission + pytest.
6. T2 M9 (parser semantic ids → descriptor dispatch → §8.4 #11).
7. T3 §6.4 multi-source compile from the management node.
8. T4 spec/README touch-ups last (explicit request only).

---

## 6. Implementation status (2026-08-24)

All items T1.1–T3 are implemented and green; T4 deliberately skipped (user has an
uncommitted `SPEC.md` edit — do not touch).

| Item | Java | Management node | Notes |
|---|---|---|---|
| T1.1 `JsonPointer` util | `JsonPointerTest` 14 ✓ | — | RFC 6901 + `$.`/`[i]`/dotted syntax |
| T1.2 Lua `valuePath` | `LuaProducerCamelTest` +3 ✓ (26 total) | — | s`lua:?valuePath=`; coarse/fine/missing-path |
| T1.3 datasource-level `valuePath` | `EventRouteFanInFanOutTest` 5 ✓ | — | per-source disassembly in fan-in + single-source |
| T1.4 sink assembly | core tests ✓ | — | `DataSinkConfiguration.valuePath`; AAS `JsonPointer` extraction; generic route-level assembly (opt-in) |
| T1.5 mgmt-node fine | `test_fine_resolution.py` 7 ✓ | `ResolvedSource.value_path`/`ResolvedSink`; descriptor emits datasource `valuePath` + MQTT data sinks (`sinkId`/`valuePath`) | |
| T2 M9 engine dispatch | `JsonataEngineContractTest` ✓ | `test_engine_dispatch.py` 3 ✓ + aimc parser tests | blob semantic id → Lua/JSONata; `jsonatatransformer.json` + `*.jsonata` blobs |
| T3 §6.4 fan-in compile | — | `test_fan_in_compiles_all_sources_and_plural_datasources` ✓ | `datasources[]` plural + comma `sourceKeys`; single-source keeps `datasource` |

**Verification:** `mvn -o test -pl core,lua,aas,jsonata -am` green (core 25, lua 26,
jsonata 1); full reactor `mvn -o test-compile` (40 modules, minus the unreachable-at-HEAD
`aas-jsonata-mqtt` example IT) green; `management-node` pytest 53 ✓.

**Environment note:** the only failing module in a full `mvn test` is
`databridge.examples.aas-jsonata-mqtt` (TestAASUpdater) — it publishes to the public
`broker.mqttdashboard.com` sandbox and fails **identically at clean HEAD** (verified in a
throwaway worktree), so it is a pre-existing external-service IT, not a regression.

**Design decision recorded (§3.1):** source-side fine `valuePath` is applied at the
**datasource/route boundary** (`EventRouteCreator`, per-source) so a fan-in route can
disassemble different sources at different pointers (§8.4 #9); the `lua:valuePath` URI
option remains available for single-source `lua:` endpoints but the management node emits
`valuePath` on the datasource only, avoiding double application. AAS sinks self-handle their
`valuePath` (`handlesValuePathInternally()`) and are not wrapped by the generic sink
processor.
