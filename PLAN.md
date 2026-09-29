> Companion to `SPEC.md`. **Status 2026-09-16: M0–M11 complete AND green end-to-end.
> ADR-020 amendment (2026-09-15): the authorable ExecutionPolicy is REPLACED by a
> fixed execution semantic — one aggregation creator per mapping (evaluate on any
> source update once ALL sources have delivered; sync sources polled at their
> declared IDTA interval), the AIMC template is back to the generated IDTA-02027
> (all policy property classes removed, `submodel_templates/aimc.py` deleted — the
> generated template is used directly), `executionPolicy` blocks in configs are a
> load-time error, ProtocolBridge reply deadline fixed at 30 s. All suites green:
> aas-model 4, management-node 104, core 34, component 13, examples reactor 90.**
>
> Earlier status blocks (§1–§7 below) are historical.

---

## 1. Baseline---

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
| M7 REST operation delegation | `databridge.core/operation/*` | done (v2: `restsources.json`; **superseded by the DMP v3 one-MC operation authoring** — the derived REST surface rides in the bridge route's `triggerData`, no REST datasource file exists) |
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
- `luatransformer.schema.json` + `luatransformer.json` example gain the optional field.

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
  has both entries, `luatransformer.json` `sourceKeys` = `a,b`.

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
| T1.5 mgmt-node fine | — | `test_fine_resolution.py` 7 ✓ | `ResolvedSource.value_path`/`ResolvedSink`; descriptor emits datasource `valuePath` + MQTT data sinks (`sinkId`/`valuePath`) |
| T2 M9 engine dispatch | `JsonataEngineContractTest` ✓ | `test_engine_dispatch.py` 3 ✓ + aimc parser tests | blob semantic id → Lua/JSONata; `jsonatatransformer.json` + `*.jsonata` blobs |
| T3 §6.4 fan-in compile | — | `test_fan_in_compiles_all_sources_and_plural_datasources` ✓ | `datasources[]` plural + comma `sourceKeys`; single-source keeps `datasource` |

**Verification:** `mvn -o test -pl core,lua,aas,jsonata -am` green (core 25, lua 26,
jsonata 1); full reactor `mvn -o test-compile` (40 modules, minus the unreachable-at-HEAD
`aas-jsonata-mqtt` example IT) green; `management-node` pytest 53 ✓.

---

## 7. Gap-closure implementation status (2026-08-26)

Gap-closure plan (`.kilo/plans/1787654893169-dmp-gap-fix-plan.md`) executed:
Phases 1–4 complete. T0.1 was the last open item and closed with the live lab
evaluation in §8.6 (2026-09-28) — it confirmed the shipped assumption that
T1.4's id-based delete fallback is mandatory, because a real `SM_DELETED`
carries no `submodel` and no `semanticId`.

| Item | Scope | Status |
|---|---|---|
| T0.1 real event capture | Kafka payloads from the live broker committed as fixtures; the delete-shape question is settled | done — `tests/data/events/`, `test_captured_events.py` |
| T0.2 green baseline | Java full reactor + pytest 70 | done (only known external-broker IT red) |
| T1.1 scoped identifiers (ADR-017) | descriptor/resolver rewrite; dup-id guard; `mapping_<index>` names; raw `sourceId`/`sinkId` pinned | done — `test_descriptor.py`, `test_resolver.py`, dup-guard test |
| T1.2 atomic config writes + pruning | per-file `os.replace`, stale-file pruning (bind-mount-safe) | done — prune/atomic tests |
| T1.3 `None` model guard + validate docstring | orchestrator guard; ADR-16 docstring corrected | done |
| T1.4 listener hardening | body-less SM_DELETED/SME_DELETED pass filter; pre-anchored pattern tolerance; commit-on-failure documented | done — `test_kafka_listener.py` |
| T1.5 asset lookup | canonical-first, suffix fallback w/ ambiguity warning | done |
| T1.6 reconcile concurrency | per-asset single-thread executor, bounded coalescing queue, `flush()` for tests | done — coalescing test |
| T1.7 deployer hardening | subprocess timeouts, hashed collision-checked health ports 18086–18999, `--restart unless-stopped`, `:ro` mount, explicit pull | done — `test_deploy.py` |
| T1.8 Dockerfile | valid vendored `_aas_model/` COPY + `PIP_EXTRA_INDEX_URL` variant; `docker build` verified | done |
| T1.9 passthrough pairing | SinkId==SourceId exact match only, unmatched skipped w/ warning | done |
| T1.10 polling semantics | unset/non-positive interval warns + defaults 5 s; <1 s clamped loudly | done |
| T1.11 protocol gaps | `rest`→HTTP builder; OPC UA requires explicit service path; AAS sink without element keys skipped | done |
| T1.12 JsonPointer null/escapes | JSON-null leaves distinct from missing (`JSON_NULL`); `~0/~1` unescaped in `$.` syntax; `toString()` escaping round-trips | done |
| T2.1 fan-out unification | transform once → Camel **multicast** to one `direct:fanout-<route>-<i>` branch per sink; per-sink assembly from the shared result; per-sink `datasinkMappingConfiguration` kept | done — `FanOutTest` (the `EventRouteMultiSinkFanOutTest` name is gone) |
| T2.2 fan-in barrier | per-route latest-value cache + all-sources-delivered gate; **no** aggregate EIP (starvation verified on Camel 4.22), **no** timeout disposition, **no** legacy-creator hatch | done — `ExecutionPolicyRouteCreatorTest` |
| T2.3 unified sync fan-in | **superseded by ADR-020 (2026-09-15)**: `UnifiedFanInRouteCreator`, the `timer` trigger, the interval-0 event mode and the fresh-all-sync-per-cycle barrier are all gone. One `ExecutionPolicyRouteCreator` emits one route per mapping with plural `datasources` + per-source `delivery`/`pollingIntervalMs` | done — `test_unified_fan_in.py`, `ExecutionPolicyRouteCreatorTest` |
| T2.4 config loading errors | missing/blank trigger throws with the route id; **unknown trigger is a named load-time ERROR** (ADR-020), not warn+skip; helpers name route + entity on a null map | done — `RoutesConfigurationFactoryErrorTest`, `RouteCreatorHelperTest` |
| T3.1 cross-AAS resolution (ADR-019) | `AidDirectory` keyed by the reference's own anchor id (self-contained, no ownership scan); semanticId-validated parse + TTL cache; resolver prefers the anchored instance | done — `test_cross_aas.py` (corrected after review: registry-scan owner chain removed as unnecessary) |
| T3.2 dependency invalidation | reverse index submodel id → dependent units, rebuilt at startup, **replaced** (not appended) on each reconcile and cleared on decommission; AID events AND body-less deletes of referenced submodels re-reconcile dependents | done — `test_cross_aas.py`, `test_kafka_listener.py` |
| T3.3 JSONata profile | strict prefix match under documented namespace; SPEC §7.6 interop note; boundary test (a semantic id merely *containing* `jsonata` stays Lua) | done — `test_engine_dispatch.py` |
| T3.4 operation conventions | **superseded by ADR-020/022**: the `DMP_MGMT_*` knobs, interface sniffing and the `/CMD/`→`/DATA/` rewrite are gone; the caller REST surface is derived from the Operation-element reference | superseded — not implemented as env knobs |
| T4.1/T4.2 refactors | `DescriptorContext` accumulators; shared `endpoints.py`; **follow-up (2026-09-28)**: schema-derived `valuePath` now also rides the OPC UA/PLC4X/HTTP source **and** sink entries, and `_add_http_source` builds its URL through the shared parser (rejects absolute/fragment hrefs and non-HTTP bases) instead of raw string concatenation | done — `test_fine_resolution.py`, `test_descriptor.py`, `fine-protocols` contract fixture |
| T4.3 dead code | `_MACRO`, positional passthrough, dead `_CREATE_UPDATE` set removed; **follow-up (2026-09-28)**: unused `_all_interfaces` resolver helper and the unreachable `bacnet` sink branch (shadowed by `PLC4X_DRIVERS`) removed | done — `test_descriptor.py` BACnet regression covers the driver map |
| T4.4 regenerate dmp-config | regenerated with the fixed compiler (the git-ignored artifact had drifted: the declared MQTT `qos` was missing); byte-identical to a fresh compile; stale `{aas_id_short}_*.lua` pruned; blob keys unchanged | done |
| T4.5 docs | ADR-017/018/019 in SPEC §15; §6.4 + §7.6 reconciled with ADR-020/022; this table | done |
| T5.1 Variables/Parameters flattening (ADR-023) | leaf entries are plain Properties, groups are SMCs, both in one map; the `.variable` sink/source suffix removed; AIMC trailing key type follows the model (`Property` vs `SubmodelElementCollection`); station configs re-authored flat; `aas_pydantic` dict helpers union-aware both ways | done — verified live: DMP writes `Variables/PackMLState` with **0** `AAS write failed` (was 404 per write) |
| T5.2 integral-number contract (SPEC §8.6) | `LuaJsonCodec.fromLua` emits an integral double as `Long` (2^53−1 bound), so an integer no longer leaves a blob as `1.0` | done — `codecKeepsIntegralNumbersIntegral`; live `OccupationState = [1]` |
| T5.3 station occupation lifecycle | a completed process no longer consumes the occupation (occupation mode); an occupation ends only on Release or abort; both state-machine copies covered by a new parametrized test | done — live Occupy→Stoppering→`[1]`, Release→`[]` |

**Verification (2026-09-28):** management-node pytest **183 ✓** (incl. the
non-MQTT `valuePath` descriptor regressions, the HTTP href-joining cases and the
re-captured event fixtures); databridge.camel-lua **31 ✓** / core **47 ✓** /
camel-aas **2 ✓**; the Java `GeneratedDescriptorContractTest` loads the
regenerated 13-case fixture matrix through the real factories (4/4 ✓,
`fine-protocols` included); aas-model **12 ✓** (incl. the container-shape
regressions); Planar_Controller occupation **8 ✓** (both state-machine copies).
The lab e2e checklist ran against the main stack — see §8.6 for the verified
items and the two that still need authored lab data.

**Environment note:** the only failing module in a full `mvn test` is
`databridge.examples.aas-jsonata-mqtt` (TestAASUpdater) — it publishes to the public
`broker.mqttdashboard.com` sandbox and fails **identically at clean HEAD** (verified in a
throwaway worktree), so it is a pre-existing external-service IT, not a regression.

**Design decision recorded (§3.1):** source-side fine `valuePath` is applied at the
**datasource/route boundary** (`ExecutionPolicyRouteCreator`, per-source) so a fan-in route can
disassemble different sources at different pointers (§8.4 #9); the `lua:valuePath` URI
option remains available for single-source `lua:` endpoints but the management node emits
`valuePath` on the datasource only, avoiding double application. AAS sinks self-handle their
`valuePath` (`handlesValuePathInternally()`) and are not wrapped by the generic sink
processor. Sink-side `valuePath` rides the **sink** entry for every builder that does not
self-handle it (MQTT, OPC UA, PLC4X, HTTP).


---

## 8. Execution-semantics migration + green closure (2026-08-31 → 2026-09-16)

### 8.1 Test-fixing sessions (2026-08-31/09-01)

All documented pre-existing reds turned green: HttpProducer (GET path), Plc4X
(fixture transformer chain), mqtt-jsonata-aas (query prefix), sql-jsonata-aas
(marshal step + prefix), plc4x-jsonata-aas (marshal step), httppolling(+auth)
(core `InputStream` boundary normalization + per-source queries), range_and_mlp
(sink valuePath), activemq (JMS deps), both kafka examples (pom fixes, embedded
broker), opcua-jackson + aas-jsonata-opcua (holistic Milo 1.1.6 upgrade:
`MiloTestServer` built on the 1.1.6 API — new `DefaultApplicationGroup`
certificate stack, None+Basic256Sha256 endpoints, HelloWorld scalars — replacing
the binary-incompatible 0.6.x `ExampleServer`; namespace-collision fix;
role-mapper + write-mask fixes for writes). Test isolation: env-var helper merge/
restore; component module green in one JVM.

### 8.2 ADR-020 amendment: fixed execution semantic (2026-09-15, user decision)

The authorable policy (TriggerKind/RequiredSources/MaxAgeSeconds/
CompletionTimeoutSeconds/OnTimeout/SnapshotPolicy/PeriodSeconds) is REPLACED by
a fixed runtime rule: every source feeds a per-route latest-value cache — async
on arrival, sync polled at its declared interval (`pollingIntervalMs`) — and an
update of ANY source evaluates the mapping once ALL sources have delivered; a
partial sources map is never emitted. Rationale: async delivery is best-effort
(a barrier across it is fragile), the IDTA polling interval is finally honored
for sync reads (no more per-arrival reads), and authoring surface drops to zero.
`ProtocolBridge` reply deadline fixed at 30 s (was CompletionTimeoutSeconds).
An `executionPolicy` block in any config is a named load-time error.

### 8.3 AIMC template = generated IDTA (2026-09-16)

`submodel_templates/aimc.py` DELETED — after the policy strip it was a no-op
re-wrap. `submodel_templates/__init__.py` re-exports the generated
`AssetInterfacesMappingConfiguration`/`MappingConfiguration`/`Transformation`
under the DMP-facing aliases (`Aimc`, ...). Schema snapshot regenerated against
the generated template.

### 8.4 Verification

aas-model 4/4; management-node pytest 104/104; databridge.core 34/34;
databridge.component 13/13 (incl. smoke test on the new semantics); examples
reactor sweep 90 tests green (incl. both OPC UA suites); golden config
(`dmp-config/syntegonStopperingSystem`) byte-verified against the descriptor.

### 8.5 Remaining

Docker builds + full root reactor sweep (`mvn -o test -fn`) to re-confirm the
final gate after the migration; pending as of this writing. Phase 4
(2026-09-28) closed the descriptor gaps described in §7 T4.1–T4.4.

### 8.6 Live lab evaluation (2026-09-28)

Run against the main stack (BaSyx aas-environment 2.0.0-SNAPSHOT-06f8841 with
`BASYX_FEATURE_KAFKA_ENABLED=true`, HiveMQ, kg-kafka, the rebuilt
`databridge:latest` + `dmp-management-node:latest` images, and the
`stoppering_station` simulator).

| Checklist item | Result |
|---|---|
| 1. Redeploy `syntegonStopperingSystem`, data still flows | **pass** — an `SM_UPDATED` on the AIMC drove listener → reconcile → 17 files compiled → `deployed … health=18464`; the DMP writes `IDLE` to `Variables/PackMLStateTimer` in the live repository (address corrected by T5.1/ADR-023) |
| 2. Second asset concurrently, both healthy | **pass** — dispensing on **18838** and stoppering on **18464**, both `state: UP`; distinct stable hashed ports, no clash |
| 3. Delete an AIMC → DMP container removed | **pass** — a real-shaped `SM_DELETED` (`{id, type}` only) logged `AIMC deleted for unit … decommissioning` → `stopped DMP container`; the dispensing unit was untouched |
| 4. Cross-AAS (IDTA Example 3) | **not exercised** — needs a second device AAS registered against the line AAS |
| 5. Unified fan-in: 1 async + 2 sync live | **not exercised** — no authored multi-protocol AIMC on the lab stations; covered by `MixedSourceFanInTest` and `test_unified_fan_in.py` instead |
| 6. Edge host restart → DMPs return | **pass** — `hivemq-broker` was down and `dmp-syntegonstopperingsystemaas` had restarted **200** times; the moment the broker returned it came back on its own, with no new AIMC event |

**T0.1 event capture is now closed.** Real payloads from the running broker are
committed under `management-node/tests/data/events/` and pinned by
`tests/test_captured_events.py`. The captured shapes settle the question the
plan left open: **`SM_DELETED` carries only `id` and `type`** — no `submodel`,
no `semanticId` — so a delete can never be attributed by semantic id and the
id-based fallback (T1.4) is mandatory. `SM_CREATED`/`SM_UPDATED` carry the full
submodel, and `SME_*` events carry `smElement`/`smElementPath` with no submodel
object, which is why they are never treated as AIMC lifecycle events. The
`Variables` fixtures were **re-captured after the flattening** (ADR-023), so
`smElementPath` is the bare element id (`PackMLStateTimer`) and now matches the
descriptor's `elementPath` — the nested shape could never match it.

Three live findings worth keeping:

1. **`databridge.component/target/lib` is only refreshed by the `docker-push`
   profile.** A plain `mvn package` therefore leaves stale dependency jars on
   disk and the image silently ships the previous build — that is how the
   deployed image ended up predating the `HTTPRequest` write-failure check.
   Build with `clean install` plus an explicit
   `dependency:copy-dependencies -DincludeScope=compile` before
   `docker build`.
2. Running the stack with `docker compose -f docker-compose.yml -f
   compose/<other>.yml …` loads the included files **twice** and resolves
   `../Mosquitto/...` against the project root, which silently breaks every
   bind mount in them. Use the plain root `docker compose up -d` (the
   `include:` list already pulls the rest in).
3. **`aas-discovery` runs a pre-Spring-Boot-3 image** whose bundled
   `application.properties` documents the legacy `spring.mongodb.*` keys, so
   `SPRING_DATA_MONGODB_*` is ignored and the driver falls back to
   `localhost:27017` — health hangs 30 s and answers 503 DOWN, which also breaks
   the web UI (it needs `aas-discovery` for lookups). Fixed in
   `compose/docker-compose.basyx.yml` by using `SPRING_MONGODB_*` for that one
   service; the registries are a different (Boot-3) image family and keep
   `SPRING_DATA_MONGODB_*`.

### 8.7 Remaining

Checklist items 4 and 5 still need an authored multi-asset / multi-protocol
AIMC on the lab; the full root reactor sweep (`mvn -o test -fn`) is still
outstanding. Also open: `AASDescriptions/Resource/configs/syntegonStoppering.json`
(the station's built-in default) still carries a pre-ADR-022 AID block — retired
`interface_mqtt`/`interface_rest` — and is rejected by the current templates, so
the station compose pins `STOPPERING_AAS_CONFIG: syntegonStoppering.main.json`
(the mechanism the e2e stack already used). The stale file itself is still there.
