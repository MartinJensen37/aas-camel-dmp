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
> W4a amendment (2026-10-09): Lua is now the only transform engine and the
> internal transform boundary is decoded Java values. The JSONata engine
> selection, module, config, examples and tests described below are retired;
> see `.kilo/plans/1791539605000-dmp-correctness-camel-native-plan.md` for the
> migration and validation gates. This note does not assert that W4a is green.

## Implementation Handover (2026-10-14)

This is the current implementation ledger. The sections below it include older
verification snapshots and must not be read as the status of the latest tree.
The source-of-truth scope remains the parent workspace's
`.kilo/plans/1791749067102-dmp-java-runtime-reconciliation-plan.md`.

All implementation changes are uncommitted. DMP HEAD remains `15cdde4`.
Preserve all existing worktree changes, including changes made outside this
session. The pre-existing modified SQL source database remains byte-identical:
`481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.

### Single Java Unit, Need-to-Know Discovery And Library Audit (2026-10-15)

Newest entry; supersedes conflicting statements below. All changes uncommitted.
Full offline reactor: **520 tests, zero failures/errors/skips** (Kafka 4.3.1).

**Done**

- Python management node, standalone/file configuration, examples, `--compare`,
  BaSyx v1/VAB removed; AAS metamodel is AAS4J 2.0.1; fingerprints are Java values.
- Discovery is need-to-know: one `GET /submodels?semanticId=<AIMC>` listing, then
  each referenced submodel read once (AID whole; others `$metadata` + referenced
  subtrees, whole beyond eight paths). Shells, owners, `{aas_id}` expansion,
  owner scopes and local-AID pins are gone; REST paths come from the Operation's
  `invocationDelegation` URL. No periodic repair, no shutdown scan; failed passes
  retry with backoff (`DMP_MANAGED_RETRY_INTERVAL_MS`).
- Events: relevance filter (referenced paths, value-only Property updates and
  unrelated submodels dropped), lazy parsing, targeted refresh. Transport flag
  `DMP_MANAGED_EVENT_TRANSPORT=kafka|mqtt` (compose `DMP_EVENT_TRANSPORT`); BaSyx
  publishes submodel events to both. MQTT uses Camel `paho-mqtt5` with two fixed
  persistent subscriptions; a dropped broker session loses events until restart.
- Event-sourced AAS Property sources (no PollingInterval) read a separate value
  feed (Kafka: per-process group at latest offset, no commits; MQTT: clean
  session), started only while such sources exist, so no backlog is replayed.
- All data-plane MQTT on `paho-mqtt5`; MQTT credentials bound per bundle in the
  Camel registry (`BoundCredential`) and referenced by name in endpoint URIs.
- Library alignment: `kafka-clients` 4.3.1 (was pinned 3.4.0) with a KRaft test
  broker (`KraftTestBroker`) and `StandardAuthorizer`; readiness probe checks
  topics before the group (Kafka 4 reports memberless groups as GroupIdNotFound).
  HttpClient 4 and commons-io removed (JDK HTTP client, HttpCore 5 `URIBuilder`;
  only difference: spaces in query values encode as `%20` instead of `+`).
  gson 2.14.0, slf4j-simple 2.0.17, awaitility 4.3.0 managed in the root POM.
  moquette and `camel-paho` (MQTT 3.1.1) removed; shared `Mqtt5TestBroker` and
  `FakeAasRepository`/`PublishedModels` are exported as test-jars. All tests JUnit 5.

**Open (library audit, not blocking)**

- `luaj-jse` 3.0.1 (2015) is unmaintained; no maintained drop-in sandboxed Lua
  for Java exists. Keep; the sandbox exposes none of its library modules.
- `dmp.camel-hono` (unreachable, kept by decision) mixes Hono 1.x/2.x clients
  (`hono-client-application-amqp` 1.12.3, `-kafka` 2.4.0, `hono-demo-certs` 1.7.4)
  and old test dependencies (logback 1.4.11).
- Minor lags needing a networked build: SQL drivers (mariadb 3.4.0 vs 3.5.x,
  postgresql 42.7.3, sqlite-jdbc 3.46.0.0, mysql-connector-j 8.4.0),
  `log4j-slf4j-impl` 2.23.0, which should also become `log4j-slf4j2-impl` (SLF4J 2).
- Library modules bundle `slf4j-simple` at compile scope; only the executable
  (`dmp.component`) should choose the logging backend.
- Resolved: `compose/e2e/test_java_resource_compatibility.py` no longer calls the
  removed `--compare` mode; its Java eligibility test was dropped (the export
  test remains; eligibility is covered by the DMP contract-fixture tests).
- Resolved: the IDE (m2e) builds into `target-ide/` via the `m2e` profile in the
  root POM, so it no longer overwrites Maven's `target/` classes.

### Current Scope Amendment

User direction on 2026-10-14: defer all performance, scale/ceiling, latency/throughput
and memory-usage evaluation to one final feature-complete terminal evaluation,
outside this plan. Existing measurements below are historical evidence, not current
work or retirement blockers. Do not run more capacity tiers or expand profiling.
Keep correctness tests for cleanup, bounded admission, deadlines and ownership.

Only current correctness/compatibility gaps remain implementation work here. HTTP
native-reply/SinkId-envelope ambiguity is resolved without dropping authored
response Lua or weakening sequential-owner parity. Continue affected correctness
regressions and qualification; do not add another performance campaign.

R8 now also retires per-asset Operation delegation URL injection. There will be one
shared DMP: select one fixed AAS-side invocation origin and preserve exactly
`/operations/{aasIdShort}/{skill}` with current case/encoding, not per-unit DMP hosts
or per-resource bases. A fixed origin has not yet been configured in the checked-in
AAS stack. Verify whether the chosen AAS supports global delegation construction;
otherwise author fixed-origin qualifiers without dynamic deployment-time injection.
Do not remove delegation itself or unrelated ID/reference injection. Authoring,
registration callers and persisted assets must migrate together in the separately
authorized retirement/cutover, not via Java reconciler repository writes.

Deployments, images, commits, registered-asset mutations and immediate Python
deletion remain excluded. Prior final checks passed: SQL plus all 15 Lua fixture
hashes unchanged, `git diff --check` clean, local MQTT test peer/port removed, and no
test/build process left running. Older open-checklist statements below are superseded.

### Java Stack Switch (2026-10-15, Repository Verified)

The user subsequently authorized the repository switch, selected the single AAS
origin `http://dmp:8080`, and approved the existing scalar `Uuid` InOut compatibility,
both control-event topics and removal of obsolete Stoppering property-write qualifiers.
This supersedes the earlier audit-only scope for those specific source/config changes.
It does not authorize Python deletion, changing resource formats/Operation directions,
rewriting Python simulations, deployed-asset mutation, image builds, deployment,
dependency installation, commits or deferred performance evaluation.

Main/E2E Compose now select one `dmp:java` service on their bridge network, with no
Docker socket, Python manager or generated descriptor mount. Empty adoption scope
discovers all repository AIMCs; unsupported mappings remain visible and blocked.
Host access is loopback 18085/main and 18086/E2E; AAS qualifiers use internal 8080.
BaSyx Kafka publication is enabled for the selected stack. Bootstrap packages current
`dmp.*` modules without Maven installation, preserves authored resource files and
requires convergence rather than only `/health`. Main/E2E config variants are now
authored inputs exposed by `.gitignore`, not regenerated ignored artifacts.

Java accepts a bounded literal topic list in `DMP_MANAGED_CONTROL_TOPIC`; selected
stacks consume `aas-events,submodel-events`, and readiness requires this instance's
assignment on each. Only the single scalar `Uuid` Property (`xs:string`) InOut is
newly supported; non-`Uuid`, collection/custom InOut and ordinary response extensions
remain fail-closed. Capture JSON/Lua bytes are preserved; only the explicitly approved
capability expectation for `stoppering-new-build` changed. Python authoring finalizes
fixed qualifiers, registration validates complete URLs, and generic IDs/references
remain independent. Python manager source/snapshots remain for recovery disposition.

The later user choices remove both MQTT `StationStateTimer` mappings and select
only OPC UA for E2E skill Operations on canonical paths, without `/opcua` Operation
aliases. E2E measurement event omits its invalid zero polling interval, its native
UA URI uses the URI-valid `stoppering-station` bridge alias, and skill/interface
references point to the existing UA actions. Main retains its MQTT skill backend.

The user subsequently confirmed that **synchronous property PUT with writeback to
the same AAS Property can be ignored**. Remove only the four legacy proxy mappings:
main `ParameterWriteMqtt`, E2E `ParameterWriteMqtt`/`ParameterWriteOpcua`, and
Dispensing `ParameterWriteOpcua`. Keep Parameter variables, native writable AID
forms, schemas and MQTT/UA sink capabilities. The attempted new synchronous caller
mode is canceled, including its SPI/adapter additions; no replacement PUT API or
implicit event/poll-triggered device write is introduced. Final selected profile
counts are main 5, E2E 7, Dispensing 8. Native parameter-write tests use the native
protocols; old DMP proxy paths are negatives, not successful same-property writeback.

**Future idea, not implementation scope:** response writeback to a different AAS
Property requires explicit AIMC `ResponseSources` identifying the validated native
acknowledgement and `ResponseSinks` identifying that distinct typed target. Specify
request/reply correlation, terminal success/failure/timeout, one-write/no-replay,
authored response Lua/per-SinkId projection, ownership and bounded lifetime before
enabling it. Do not infer the response destination, echo a proposed value as an ack,
or turn ordinary property events into a self-feedback loop. Ordinary response
extensions stay explicitly unsupported until that separate contract is approved.

**Final repository verification:** clean offline dependency-reactor correctness
passed **109 tests, zero failures/errors/skips**, including standalone loading/Lua
path compatibility, strict protocol outcomes, caller values/envelopes, raw fixture
integrity, lifecycle, both control topics and missing/denied-topic readiness. Test
temp root was isolated at `/tmp/kilo/dmp-java-switch.ofUHab`; no capacity evaluation.
After precise cancellation cleanup, the clean gate caught and corrected request
envelope restoration: direct/BaSyx same-name objects, siblings, scalars, arrays and
null retain exactly one authored SourceId envelope. Existing assertions were not
weakened. No callerMode/property-write/prepareCommand feature remains in Java source.

The selected current reactor then packaged/copied runtime dependencies offline with
`docker.skip=true`, no install/image goal. Actual snapshot-JAR acceptance passed
**25 tests, zero failures/errors/skips**. The packaged JAR SHA-256 is
`c4a8347120e86e8c2bd5ccd9390282e73edb22bfe7f38af32bd65e8baaa52617`.
Parent offline configuration/actual-profile tests passed **17**: each 5/7/8-mapping
profile is completely eligible, and both main assets are discovered/compiled together
as **two AIMCs/13 mappings** in one GET-only loopback repository. Comparisons activate
no routes and permit no repository writes; Python-side external sockets/DNS are denied.
Retained Python model tests passed **32**, registration Operation tests **12**.
Both Compose configs, bootstrap/Python syntax and per-repository whitespace checks
pass. SQL hash `481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`
and all 15 golden Lua hashes match the audit baseline. No source goldens were regenerated.

This verifies the checked-in Java selection, not a fresh full 40-project test run,
image, deployed server, registered-asset migration or live protocol staging. No
running service was switched/restarted, no deployment/image/installation/commit or
performance evaluation occurred, and Python manager source/snapshots remain. Live
inventory, persisted qualifiers, exclusive ownership and rollback still require
their separate approval before applying this wiring to an active stack.

### Current Correctness Work

The HTTP sequential-owner reply mismatch is fixed at the final caller boundary.
`OperationReplyProcessor` now distinguishes native normalized device replies (no
response Lua, no explicit mapping) from transformed SinkId envelopes. Native objects
whose fields match the action retain their whole object/array/null values and
siblings. Authored response Lua and explicit response mappings still select exactly
one caller SinkId; no response-source projection or Lua behavior was removed.

The first focused verification passed **42 tests, zero failures/errors/skips** in
`/tmp/kilo/runtime-correctness-http-parity.log`. The later clean focused run passed
**56 tests, zero failures/errors/skips** in
`/tmp/kilo/runtime-correctness-ua-types-verified.log`: reply selection/route tests (22),
OPC argument and adapter tests (10), compiler configuration tests (18), strict
HTTP/MQTT/OPC sequential-owner plus typed OPC output tests (4), and raw-model device /
authored-response-Lua tests (2). Native null/action-named BaSyx outputs are covered.

The real typed-output simulator also found and closed two wiring/response gaps:
compiler argument types now preserve array item type/rank and typed Property
`valueType` schemas; native UA output values must match the declared basic type and
rank, independently of optional affordance validation. Incompatible results are
remote-response errors (502), not successful coerced replies or configuration errors.
The simulator verifies whole native arrays/nulls, scalar caller projection/order,
BaSyx output variables, five Calls for five explicit invocations, no retry/replay and
owned route/endpoint cleanup. Custom/complex UA datatypes and array-valued CCI caller
variables are not claimed by this fixture.

Real loopback SCRAM-SHA-256 and SCRAM-SHA-512 over SASL_SSL now pass active-consumer /
Admin readiness, wrong-password rejection and untrusted-certificate rejection.
Both TLS cases and both existing PLAIN/ACL cases passed in
`/tmp/kilo/runtime-correctness-typed-security-retry.log`; that run still failed its
then-unfixed OPC test and is not an all-suite green log. Certificates/truststores /
broker storage are temporary and removed; JVM trust properties are restored and
keytool passwords are passed via a child environment, not printed command arguments.
Fatal native consumer access denial intentionally requires permission repair and
runtime recreation; there is no new automatic fatal-consumer retry subsystem.

Final correctness verification is green:

* All **40 projects passed** the clean reactor with normal default test-name
  patterns and explicit `!ManagedRuntimeCapacityTest` exclusion, both engines,
  explicit generated-contract properties and a new loopback MQTT5 peer. Evidence:
  `/tmp/kilo/runtime-correctness-clean-reactor.log`. No capacity/performance evaluation
  was run; the exclusion is deliberate scope deferral, not a reported stress pass.
* All **40 projects packaged successfully**, tests skipped and `docker.skip=true`,
  followed by runtime dependency copies; no install/deploy/image goal was invoked.
  Evidence: `/tmp/kilo/runtime-correctness-snapshot-package.log`.
* **25 actual snapshot-JAR acceptance tests passed, zero failures/errors/skips**:
  `/tmp/kilo/runtime-correctness-snapshot-acceptance.log`. Both runtime and read-only
  comparison paths used `java -jar`, plus lifecycle/health/control-readiness checks.
* Retained Python oracle: **206 passed**. SQL source DB and all 15 golden Lua files
  remain byte-identical; whitespace checks pass. The session-created loopback MQTT
  peer was stopped and its container/port removed; no deployed broker was used.

The local artifact remains a mutable development snapshot at
`dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar`, not a release or deployment.
SHA-256: `206878c4e2b071c0f5fe36320e044546593e2357c1335b7cd5689b834517d253`.
The packaged changed runtime libraries are:

* `dmp.camel-aas`: `becc448750d5df6c9704d9cef32d6569fa0f4c6aec22dd105ea24c3817de69e7`
* `dmp.camel-opcua`: `4c7fd5c4102ff10fe10d69f4a7aa40311a5031342b73c81f029c987c27a91a1a`
* `dmp.management`: `562dd16d19b5eeea1087acff1b6785c47f5756dd91f5c364a790afa011a9c519`

Remaining planned work is the separately authorized external/live compatibility and
exclusive cutover/migration gates, then R8 Python plus per-asset URL-injection
retirement with the selected fixed shared AAS-side origin. Do not revive historical
HTTP failure or final-cleanup checklists below: they are superseded by these results.

R8 authoring/registration audit targets: `aas-model` constants, builder and
`id_injector` delegation-base/host expansion; Registration_Service config parsing
and delegation tests; resource configs carrying per-asset overrides. Preserve the
Operation template suffix and unrelated ID/reference injection. The chosen AAS
mechanism and fixed shared origin must be established before removing that wiring;
no parent deployment/config or registered-asset change has been made here.

### Phase Ledger

| Phase | Implemented | Remaining Gate |
|---|---|---|
| R0 | Repeatable Python/Java baseline; reopened value, assembly, response isolation and MQTT ownership fixes; isolated SQL writes and local test brokers | Baseline is established, not proof that every parent correctness/rollout gate is closed |
| R1 | Finite Java wire projections; 13 Python authoring descriptors; raw fixtures for relevant Stoppering submodels plus an in-memory station-builder candidate; real loopback HTTP operation/polling behavior and explicit negative contracts | `fullR1Validation=true` for the clarified submodel-only scope. Current station AIMC/Variables contain retired `writeDelegation` declarations and are explicitly rejected; the new builder candidate has Property references but its CCI InOut operation mappings are explicitly unsupported. No owner shell is inferred and nothing is pushed. |
| R2 | Ordinary and bridge install/remove/re-add plus staged replace/restore, generation fencing, admission/drain, exact route/REST/endpoint inventories, derived HTTP/MQTT invocation endpoint ownership, bounded processor/MQTT cleanup | Final clean reactor exercises replacement/restore, stale work, failed cleanup and churn; no remaining specific R2 blocker identified in this run, but do not infer lossless handover or exactly-once device execution |
| R3 | Immutable precompiled Lua bytes, opaque registry binding, limits, shared binding refcounts, bundle-owned release and standalone compatibility; real managed Lua replacement and cache correctness regressions pass | No identified script-lifecycle blocker; performance/memory evaluation moved outside this plan |
| R4 | Read-only repository client, pagination/auth/failure outcomes, exact references/owners, per-mapping desired outcomes/dependencies and secret-safe fingerprints for valid and rejected mappings, exact local-AID discovery/pins, capability preflight and runtime bridge checks | Unsupported ordinary response extensions and CCI InOut operations are rejected explicitly rather than silently dropped; no remaining specific R4 blocker identified under the accepted contract |
| R5 | Bounded cached dependency-graph repair, per-compilation AID/anchor reuse and compact fingerprints; targeted/full repair, exact ownership and transient failure regressions pass | No identified reconciliation-correctness blocker; further read-sharing optimization and fleet sizing are outside this plan |
| R6 | Safe cached readiness/native task health; real SASL/PLAIN ACL plus both SCRAM mechanisms over TLS; fatal consumer denial requires permission repair/runtime recreation; clean correctness and actual snapshot-JAR acceptance green | PLAIN-over-TLS combination not separately integrated; further socket/heap evaluation is deferred outside this plan |
| R7 | Strict sequential HTTP/MQTT/OPC outcomes and real declared typed OPC outputs; whole native reply boundary, array signatures and incompatible-result handling fixed; clean correctness/JAR acceptance green | Representative live Occupy/Halt/granular-operation staging and exclusive cutover still require authorization; no arbitrary custom-UA/array-CCI claim |
| R8 | Offline inventory complete; parent source selects shared Java, fixed model-authored origin and approved compatibility; final local verification green | Live inventory/persisted-qualifier/exclusive-owner migration remains separate. Python source removal and the five legacy generated-descriptor test-gate replacements are not performed by this switch; preserve recovery evidence until those applicable gates close. |

### Offline Retirement Readiness And R8 Removal Plan (2026-10-14)

The offline audit is complete. The full evidence inventory, exact fixture hashes,
per-repository implementation sequence, correctness commands and conditional cutover
gates are in the parent plan's **Offline Retirement Audit And Bounded R8 Plan**:
`.kilo/plans/1791749067102-dmp-java-runtime-reconciliation-plan.md`.
This audit changes only the two existing plans. It does not authorize Python deletion,
authoring/production config changes, images, deployment, assets, live actions,
dependency installation, commits or performance evaluation. Existing dirty work was
preserved, including after the session pause. The SQL hash and all 15 golden
model-contract Lua files remain unchanged; no fixtures were regenerated.

#### Management-Only Replacement Clarification

User clarification on 2026-10-14: keep existing resource-description formats,
Python authoring/registration and Python station/simulation implementations. Replace
only the Python management plane: Java reads all in-scope existing AIMCs and relevant
source/sink descriptions at startup, constructs routes in memory and maintains them
from repository events, with periodic repair. This is already implemented in the
uncommitted managed path (`DmpExecutable:51-59`, `ManagedDmpRuntime:44-74,113-117`),
but managed mode is opt-in and the parent Compose wiring still selects Python.

"Java preparation" referred only to static Java test evidence replacing generated
test descriptors, not new production descriptions or rewritten simulations. This
clarification is not approval to implement that fixture change or to rewire production.
The separately gated fixed-origin qualifier proposal concerns invocation addresses
only; it does not authorize a resource/model-format migration or a port of Python
authoring and simulated devices to Java.

#### Evidence-Based Inventory

Paths below are relative to the named owning Git repository, not all to this DMP.
Source line references refer to the audited source before this documentation edit.

| Class | Owner / Proposed Item | Reason And Replacement |
| --- | --- | --- |
| Remove after gates | DMP `management-node/management_node/` including entry points, client/locator/listener, parsers/resolver/endpoints/execution policy, descriptor, orchestrator and Docker deployer | `main.py:47-100` starts the management plane; `orchestrator.py:552-566` writes descriptors/deploys; `deploy.py:99-149` recreates/stops per-unit containers. Replace with existing Java read-only discovery/compiler/reconciler and bundle lifecycle. No external manager imports were found in searched production source. |
| Remove after gates | DMP tracked `management-node/{Dockerfile,pyproject.toml,README.md,.gitignore,e2e.managed_assets.json,example.managed_assets.json}` and tracked `tests/` | 51 tracked files total. Transfer relevant station/event/source evidence and static tests before retiring generator/oracle. Exact AIMC IDs and required example AID pin must migrate to Java settings. Never recursively delete ignored `_aas_model`, environments, caches or generated config; preserve dirty patches/recovery evidence. |
| Replace | Parent `compose/docker-compose.services.yml:269-305`, `compose/docker-compose.e2e.yml:281-312`, `compose/e2e/bootstrap.sh:25-29,44-56,60-80,112-159` | Both stacks still reference Python image/context, Docker socket/config-root and managed seed. One shared Java service replaces them only under parent integration approval. Remove manager vendor/start/wait/hashed-port logic, retain Registration Service's vendor copy; repair bootstrap's stale `databridge.*` module names. Do not execute bootstrap. |
| Replace discovery; retain outcomes | Parent `compose/e2e/test_e2e.py:38-65,126-130,260-265,381-429`; simulated-stations Compose comment | Direct DMP callers need one approved test endpoint, not per-unit discovery. Audit `/opcua`/timeout legacy variants separately: they are not the canonical Java managed path. Preserve assertions or explicitly approve their standalone/migration disposition. |
| Replace URL injection only | `aas-model` `src/aas_model/{constants,builder,id_injector}.py`, `resource_template/control_component_instance.py:72-116` | Retire `DELEGATION_BASE`, per-resource builder metadata/arguments and `{dmp_host}`/`{delegation_base}` resolution. Author fixed-origin Operation qualifiers; keep IDs, `{aas_id}`/`{aas_id_short}` references, specific-asset IDs, schema/model tooling and Operation variables. Update serde/variable tests and add fixed-qualifier/identity regressions. |
| Replace parsing/tests; retain service | Registration_Service `src/config_parser.py:36,47-69`, `tests/test_skill_mappings_and_operations.py:85-123,147-157` | Reject retired overrides/macros rather than rewrite URLs before publication. Registration/model/schema processing is independent Python still needed. Preserve `src/registration_service.py` publisher; persisted changes are separately authorized. |
| Replace current dependency prose only | `aas-model/pyproject.toml:8`, Registration_Service `tests/conftest.py:1-6` | Stop naming the removed manager as a current consumer; retain Python package dependencies and the registration tests' sibling model path. |
| Replace approved authoring | Parent `AASDescriptions/Resource/configs/{syntegonStoppering.json,syntegonStoppering.main.json,syntegonStoppering.e2e.json,syntegonDispensing.json}` | Stoppering macros and Dispensing's explicit base/six skill qualifiers need the selected shared origin. Include default Halt/Occupy/Release, not just explicit overrides. Preserve skill case/path. Existing dirty removal of Dispensing property-write qualifiers is unrelated work, not new audit implementation. |
| Replace tests, retain goldens | DMP `GeneratedDescriptorContractTest.java:55-66,96-100,265-268,327-330,353-356` | Five tests assume four generated descriptor directories. Make existing compiled classpath fixtures mandatory; preserve typed values, Lua, addresses, order, errors/send counts and standalone compatibility. The missing legacy Stoppering capture is a concrete prerequisite. |
| Retain / disposition decision | Parent `compose/dmp-config-main/`, `compose/e2e/dmp-config/`; DMP ignored generated config | Route/client/Lua snapshots are inventory/rollback evidence, not running ownership. Do not delete with Python; retention/archive/removal requires verified consumers and rollback disposition. |
| Retain | Parent BT descriptions and `BT_Controller/src/backends/aas/{action_backend,aas_client}.cpp` | Calls AAS Operation `/invoke`, not a hard-coded DMP origin. Persisted qualifier migration changes its routing; preserve BT behavior. Empty `onHalted()` is not a Halt call. |
| Retain, separate caller decision | Parent `OperationDelegation/operation_delegation_service.py:809-830`; ARSO `Transformation/AAS_Builder/AAS_generation/{core/generate_aas.py,submodels/skills_builder.py}` | Independent service has no current Compose/import caller found, not proof of no external use. ARSO separately authors URLs with `DELEGATION_SERVICE_URL` and system IDs; decide whether its published assets are in scope before migrating that sibling path. Neither belongs to manager deletion by inference. |
| Retain | Java standalone loader/factories/examples, raw model matrix/manifest/provenance, existing compiled JSON/Lua; all unrelated Python tooling | Managed startup has no Python/file-loader requirement, but standalone file mode remains a supported concrete consumer. Historical generator/source hashes stay capture provenance after retirement. Update current README/SPEC architecture, not every historical reference. |

#### Fixed Shared Delegation Decision

The checked-in AAS server is overrideable
`eclipsebasyx/aas-environment:${BASYX_VERSION:-2.0.0-SNAPSHOT}`
(`compose/docker-compose.basyx.yml:3`), not a resolved artifact/digest. Its environment
has no authoritative global delegation property. AAS v3 authoring/templates and
Python SDK dependencies do not prove server delegator behavior; no matching local
server source/config evidence was found for global path construction.

The bounded plan selects **model-authored fixed-origin `invocationDelegation`
qualifiers**, unless authoritative evidence for the actual intended AAS artifact
establishes an equivalent global delegator. Verify that artifact's qualifier/HTTP
contract before implementation; global support remains unverified, not disproved.
**The concrete scheme/host-or-IP/port is still a required decision.** No current
per-unit base, hashed port or host-gateway comment is promoted to a shared default.

Complete URLs must be authored before publication, not injected by registration or
deployment. Retire top-level `delegation_base` and URL macros with explicit rejection;
retain generic identity/reference substitution without URL encoding, but do not
rewrite any identity macros inside invocation URLs. Existing configs
depending on default macro qualifiers need complete fixed qualifiers authored first.
The Java factory builds `/operations/` with separately UTF-8-encoded exact owner
idShort and skill (`ConnectorConfigurationFactory:278-279,610-612`), replacing `+`
with `%20`. Current Python authoring interpolates without encoding. Add case/encoding/
single-encoding regressions and keep the exact public suffix; never lowercase paths,
substitute shell IDs or rename collisions. Java repository reconciliation stays GET-only.
Model authoring and separately approved persisted-qualifier migration must agree;
bulk registration is not a safe URL-only migration.

#### Python-Free Test Gate

Reuse the 12 existing static compiled cases under `runtime-model-contracts/` without
changing their bytes. Keep all 15 raw cases, expected files, resource SHA checks and
15 golden Lua files. Remove `dmp.contract.dir`, `dmp.descriptor.dir`,
`dmp.opcua.descriptor.dir`, `dmp.http.descriptor.dir` assumptions from all five
`GeneratedDescriptorContractTest` cases. No runtime Python generator or external
descriptor root may remain in final Java correctness commands.

The 13th legacy descriptor case, Stoppering, has no static `compiled/` snapshot.
The raw current Stoppering case rejects `writeDelegation`; the new builder candidate
rejects CCI InOut. Neither replaces old standalone descriptor compatibility. The
recommended decision is an approved byte-preserving capture of the five existing
`/tmp/kilo/runtime-reconciliation-r0-contracts/stoppering/` JSON/Lua files into a
separate `dmp.component/src/test/resources/standalone-descriptor-contracts/stoppering-legacy/`
resource set with provenance/hashes recorded in the parent plan. Capture approval is
pending; no files were copied. Preserve the original two station inputs/seven event
payloads before deleting their only copies. Do not generate new expected results or
change raw negative fixtures to make this legacy fixture pass.

Retain comparison, strict sequential HTTP/MQTT/OPC outcome, native typed/lexical
string/null/order, exact 500/502/504 and send-count/no-replay, Lua/bridge/ordinary
lifecycle, parser/capability and standalone loader tests. The loss of Python's
continuous authoring-regeneration comparison is an explicit tradeoff, not equivalent
to Java resource hash validation. Relative query paths must resolve from static
resources; file-loader tests may copy approved bytes to disposable Java test output,
not depend on Python-generated temporary descriptors. Isolate JVM temp roots because
`RoutesConfigurationLoader` resets `java.io.tmpdir/dataBridge`.

#### Implementation And Gates

1. After explicit implementation approval, preserve/capture legacy station/event
   provenance and convert the five descriptor tests. Verify this DMP-only preparation
   while the Python oracle/recovery path remains intact.
2. Select one AAS-reachable origin and resolve artifact/delegator evidence; decide
   exact adoption IDs/pins, required model capabilities, ARSO consumers and legacy
   E2E path variants. Do not infer deployed state from seeds or descriptors.
3. Separately approve coordinated `aas-model`, Registration_Service and resource
   authoring changes, then main/E2E Compose/bootstrap/caller integration. Keep one
   managed service, no socket/config-root mount, dedicated control/data identities,
   existing limits and exact paths. No deployment or publication follows implicitly.
4. If an authorized deployed inventory establishes active Python ownership, complete
   exclusive-owner migration and secure independently restorable dirty-source/image/
   descriptor/qualifier/scope/pin evidence before deleting its recovery path. If no
   deployed dependency is established, record it rather than inventing a cutover.
5. Remove only the reviewed tracked manager set after those applicable gates. Retain
   snapshots, ignored local data, independent Python and standalone configuration.
   Close correctness/JAR/import/startup/doc gates; do not run deferred evaluation.

Future commands from the DMP, after static-contract conversion, include:

```bash
mvn -o -B -Ddocker.skip=true -Ddatabridge.health.port=0 -pl dmp.component -am \
  -Dtest=GeneratedDescriptorContractTest,RawModelContractFixtureTest,WireFixtureCompatibilityTest,ConnectorConfigurationFactoryTest,AasControlEventTest,ManagedPlanComparisonTest,ManagedProtocolOutcomeParityTest,ManagedBridgeLifecycleTest,ManagedModelBehaviorTest,ManagedLuaReconciliationTest,TestRoutesConfigurationLoader,LuaDefaultConfigurationFactoryTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

DMP_MQTT5_URI=tcp://127.0.0.1:28883 mvn -o -B \
  -Ddocker.skip=true -Ddatabridge.health.port=0 clean test -fae \
  '-Dtest=Test*,*Test,*Tests,*TestCase,!ManagedRuntimeCapacityTest'
```

Use isolated disposable temp roots and an approved loopback MQTT5 test peer, never a
deployed broker. Parent plan lists package/dependency-copy and actual `java -jar`
acceptance commands with `docker.skip=true`, plus retained authoring/registration
tests. No install/image/deploy goal or descriptor-dir properties. Require five
descriptor cases to run rather than skip; retain both engines and report the Hono
skip separately. None of these commands was executed for this audit.

Old clients/groups/pins/URLs are source inventory only. A live inventory must verify
loaded trees, ownership, DNS/qualifiers/callers, MQTT sessions and Kafka offsets.
New Java identities do not inherit Python broker sessions or source-group END seeks.
For an established active dependency, stop caller admission and old manager first,
then old DMP owners, verify absence of consumers/listeners, apply approved URL/config
changes and activate one Java owner. Rollback stops/cleans Java ownership before
restoring old URLs/DMPs and finally manager; record unknown actions without replay.
Representative required live Operations and rollback rehearsal need their own approval.

Current `writeDelegation`, CCI InOut, ordinary response extensions and node-less OPC
read are migration blockers only if required by the chosen adoption scope. Retiring
property-write behavior is not removing Operation invocation. No new protocol feature,
automatic fatal-consumer retry or performance prerequisite is introduced.

**Readiness:** bounded repository preparation is ready to implement after approval;
complete Python/shared-origin removal is blocked on the concrete fixture/origin/
mechanism/integration decisions above. **Live cutover is not ready or authorized.**

### Earlier Targeted Repair And Readiness Evidence

This continuation kept all original exclusions and preserved the existing dirty
implementation. No deployment/bootstrap/registered-asset changes, commits, image
builds/pushes or Python retirement occurred. The only Docker activity created and
removed an isolated loopback MQTT5 test peer from the already available image;
all other services were embedded or new loopback fixtures, never deployed brokers.

Targeted discovery now retains a bounded desired graph (maximum 8192 mappings).
Known dependency hints identify affected AIMCs; exact header and owner reads plus
bounded parallel compilation replace only that portion of desired state, retaining
unaffected candidate/compiled-resource identity. Shared dependencies repair all
dependants. Exact removal and transient/auth failures retain the original fences.
Uncertain owner/reference changes, unknown resources, failed/unavailable cache,
scope/pin changes or cache overflow fall back to full authoritative discovery.
Startup, periodic and invalidation-overflow repair are full; a continuous stream
of known hints still encounters the full-repair deadline. This cache is neither
durable nor an ownership lease. Regressions assert unrelated AIMCs are not fetched,
exact-owner authority, shared dependency coverage, removal/recreation/failure
fallback, and actual scheduled targeted versus unknown/periodic full repair.

Control readiness is now more than “route started”. Every instance has a unique
control client ID while preserving its configured group/data-plane identities.
One bounded daemon performs read-only Admin topic/group checks and requires this
instance's stable assigned member with a live topic leader, supplemented by native
consumer task readiness. Probe I/O has a two-second budget and one-second delay;
evidence ages from before I/O and expires at five seconds. Health/status requests
read only cached state, not network or lifecycle locks. Missing topics, other/stale
members, broker outages, unauthorized/stale probes and unavailable consumers are
unready. Idempotent shutdown closes the observer/Admin/executor, handles partial
startup/linkage failure, and fences late completion. ACL/SASL classification is
deterministically tested; no actual authenticated broker integration is claimed.

The real outage test found `/health` returning 500 because native Kafka Throwable
details were sent to Gson. Managed health now bypasses native DTO invocation and
returns a fixed safe cached 200/503 projection; standalone behavior is unchanged.
No raw connector URI, group/client identifier or Throwable is exposed. Real
source/JAR tests verify missing-topic recovery, broker stop/restart on the same
endpoint, 503 then 200 health, new-member ownership after crash, bounded observer
cleanup and **no ACTION replay** through readiness recovery.

Normal bounded stress now runs 32 desired/16 owned and 256 desired/128 owned cases.
The explicit larger run also passed at **1024 desired/512 owned**, 1000 queued
invalidations, three replacement waves, constant ownership and zero final tracked
resources after confirmed removal; **zero ACTION sends**. It took 393,978 ms on
the local JVM. Initial/final sampled process heap was 1,028,852,080 /
1,025,332,392 bytes, including the test harness/embedded broker, not per-asset
memory or a GC/heap-isolation guarantee. Process status separately exposes heap
used/committed/max and live/daemon thread totals without enumerating stacks. The
8192-owner configuration ceiling remains unproved; parameterized isolated runs
can use explicit `dmp.stress.capacity` / `dmp.stress.desiredMappings`, not silent skips.

Larger stress evidence:

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-targeted-larger-stress.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 -pl dmp.component -am \
  -Dtest=ManagedRuntimeCapacityTest -Ddmp.stress.capacity=512 \
  -Ddmp.stress.desiredMappings=1024 -Dsurefire.failIfNoSpecifiedTests=false test
```

Final verification: **BUILD SUCCESS, all 40 projects, 28 test-bearing projects,
545 reported tests: 544 passed, zero failures/errors, one existing Hono skip**.
Both Vintage/JUnit4 and Jupiter ran. Raw model-contract fixtures remain independent
of Python; all generated-descriptor and isolated MQTT5 gates were explicitly set.
Previously generated `/tmp/kilo` descriptors were reused without golden regeneration.
The initial focused full-discovery timeout coincided with overlapping builds; an
isolated rerun and the final clean reactor passed without weakening its deadline.

```bash
DMP_MQTT5_URI=tcp://127.0.0.1:28883 mvn -B \
  -l /tmp/kilo/runtime-reconciliation-targeted-readiness-clean.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 clean test -fae \
  -Ddmp.contract.dir=/tmp/kilo/runtime-reconciliation-r0-contracts \
  -Ddmp.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/stoppering \
  -Ddmp.opcua.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/opcua-method \
  -Ddmp.http.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/http-operation
```

Packaging: **BUILD SUCCESS, all 40 projects**, image builds/pushes disabled.

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-targeted-readiness-package.log \
  -Ddocker.skip=true package -DskipTests dependency:copy-dependencies \
  -DincludeScope=runtime -DoutputDirectory=target/lib
```

Actual-JAR post-package acceptance: **25 passed**, zero failures/errors/skips,
including three subprocess lifecycle/outage cases, executable comparison,
seven lifecycle tests, six cached observer tests and three health tests. The
subprocess cases invoke `java -jar` with adjacent runtime dependencies on the local
JVM, not a built/deployed application image.

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-targeted-readiness-packaged.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 -pl dmp.component -am \
  -Dtest=ManagedPlanComparisonTest,ManagedPackagedRuntimeIntegrationTest,ManagedRuntimeLifecycleTest,ManagedControlReadinessTest,HealthCheckTest \
  -Ddmp.comparison.jar=/home/tristan/repositories/AP2030-UNS/aas-camel-dmp/dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar \
  -Ddmp.runtime.jar=/home/tristan/repositories/AP2030-UNS/aas-camel-dmp/dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Python migration oracle: `PYTHONPATH=../../aas-model/src:. python3 -m pytest` from
`management-node`: **206 passed**. All 15 golden Lua hashes are unchanged; protected
dirty SQL SHA-256 remains
`481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.

Remaining: maximum-fleet/8192-owner stress and performance/heap isolation;
per-protocol socket/accounting and authenticated broker integration; mixed-protocol
simulator/fleet values/replies/send-count parity; approved live Occupy/Halt/granular
Operation staging; actual deployment/registered-URL/pin/client-group migration and
exclusive cutover/rollback. Those gates still block R8. Python has **not** been
retired, and local evidence does not authorize deployment or deletion.

### Local Continuation 2026-10-14

The continuation preserved the pre-existing implementation and protected artifacts.
The retained Python oracle passed again (**206 passed**). Before this continuation's
new membership projection optimization, focused compiler/repository regressions
passed (33 and 6 tests respectively); those earlier counts do not validate the
subsequently added decoded-page projection overloads. The protected SQL source DB
and all 15 golden Lua files were re-hashed and remain at their recorded values.

Real loopback embedded Kafka authentication was added for the managed control plane.
The active Kafka consumer and cached readiness Admin share validated control-only
SASL settings; no credentials are placed in endpoint URIs or exported status. PLAIN
over loopback SASL_PLAINTEXT passed assignment, wrong-password rejection, own-member
readiness, DESCRIBE deny/grant recovery, actual control-consumer topic/group denial,
durable manual commit while repository apply is held, restart without acknowledged
event/ACTION replay, and cleanup. **25 focused tests passed** in
`/tmp/kilo/runtime-sasl-focused-final.log`; credential-material scan was empty.
This proves neither SASL_SSL/TLS nor SCRAM interoperability. Native consumer READ /
GROUP denial recovery was tested after runtime recreation, not as automatic recovery
of a fatally failed consumer; DESCRIBE denial recovery was same-runtime.

Compilation now reuses the immutable local AID projection fetched for discovery or
pin resolution, plus successful and failed exact anchors, within one compilation.
The per-compilation cache expires before a later repair. Membership header and shell
projection paths were also changed to consume already decoded page values rather
than serialize and parse each result again. No shared cross-AIMC durable cache or
production limit increase was introduced. A new repository-client test confirms
oversized membership response rejection is fail-closed. These latest changes still
require a post-edit focused and clean-reactor run.

Stress instrumentation now reports membership-page/model-read counts, phase
latencies, process heap/thread samples, and model fixture bytes. The synthetic
repository fixture honors pagination, and the parameterized stress test has a
20-minute test deadline. The initial 1536-desired/768-admitted attempt failed at the
initial install assertion because the old fixture did not paginate; its discarded
failure categories mean body-limit exhaustion versus timeout was not established.
No larger-tier pass is claimed from that attempt. The retry and resource-accounting
suite remain gates; use bounded JVM resources and preserve the 4096 default / 8192
configured admission limits.

The first real outcome parity suite initially failed three cases. The HTTP reply
and MQTT body failures were incorrect first-owner expectations, not yet
legacy-vs-Java comparison evidence: HTTP assumed a device-echo shape instead of
comparing the actual static descriptor outcome, and MQTT assumed a decoded payload
instead of collecting the legacy descriptor's actual JSON value. Those unsupported
expectations were removed. The tests still require exact command/write order,
send/write counts and exact caller replies/AAS write lists across sequential static
and Java owners. In the corrected run the HTTP request bytes, send counts and
failure replies match, but successful caller replies differ: the static descriptor
returns the full echoed JSON object (including sibling fields), while the Java plan
returns the Lua-projected `Restart` object/array. This is a real remaining parity
failure, retained without weakening equality. MQTT latest-value/type/order AAS
writes match exactly across both owners; the OPC bad-status/one-call/no-replay case
also passes after the native result repair below.

Follow-up recovered the original suite outcome from retained Surefire XML: **9
tests, 6 passed, 3 failed** (two unsupported first-owner expectations and the OPC
case). The corrected isolated rerun is **3 tests, 2 passed, 1 failed**; only the
HTTP static-vs-Java successful reply projection remains failing. OPC result loss
was repaired by performing one `callAsync` through the configured endpoint's
existing acquired Camel-owned Milo client connection, then decoding the actual
`CallResponse` status and `Variant` outputs before releasing the connection. A real
loopback Method simulator now returns 502 for `Bad_InternalError`, records six
calls across sequential legacy/Java owners, observes no replay, and verifies route /
endpoint cleanup. Focused OPC verification passed **16 tests, zero failures/errors/
skips** in `/tmp/kilo/r7-opcua-repair-focused-verified.log`. The simulator declares
no output arguments/schema, so arbitrary typed output parity remains unproved.
An attempted removal of the implicit response-source projection to force the
static reply shape made the generated response-Lua regression return 500 instead
of the authored typed reply (`ManagedModelBehaviorTest`), so that production change
was reverted. Do not resolve the remaining HTTP parity finding by dropping response
Lua behavior or relaxing either test; reconcile the older static descriptor/runtime
semantics with the declared response source mapping before external rollout.

Per-protocol accounting now distinguishes identity-deduplicated owned resources,
instrumented plain-TCP MQTT sockets, unknown socket transports, and exact retained
Lua source bytes. Per-bundle resource tokens contain no raw owner/endpoint IDs.
Bytes are retained source payload lengths, not object/heap estimates; shared source
bytes are counted per referenced owner and process-wide JVM totals remain separate.

The current integrated focused run passed dependency/repository (37), OPC adapter
(15), MQTT resource accounting (6), Kafka control/auth (5 combined), lifecycle (7),
capacity (2), resource ownership/status (9), and MQTT parity; the single HTTP reply
parity failure remains in `ManagedProtocolOutcomeParityTest`. Capacity observed the
normal-size 32/16 and 256/128 tiers and all report zero ACTIONs. The bounded
final-code 768-desired/384-admitted tier passed all phases in **730.1 seconds**:
initial 57.0 s, invalidation-burst repair 20.7 s, replacement waves 185.0/185.1/
185.1 s, and removal 83.5 s. It recorded 12,288 model reads, 89 membership pages,
zero ACTIONs, a sampled heap-use maximum of 1,053 MiB of the 1,536 MiB fork, 887 MiB
post-removal, and zero remaining owned resources. The retained Python oracle was
rerun at **206 passed**; the 13 temporary generated fixture directories are under
`/tmp/kilo/runtime-reconciliation-r0-contracts`.

The first clean-reactor attempt exposed a deterministic transient-AIMC scan deadline:
the healthy mapping was classified as `aimc-scan-timeout` after 20 seconds. A thread
snapshot located its discovery worker spending 20.7 CPU seconds in Gson during
`EffectiveFingerprint.ofDesired`; fingerprints were recursively serializing full
resolved submodels and both the same parsed list children and `childListValue`.
Fingerprints now use a canonical compact mapping projection and each selected
resolved target element/schema once. The 20-second test deadline remains unchanged.
After this fix, focused compiler/factory/repository tests passed **38/38**; the
previously timing-out independent-healthy-AIMC case passed in **1.836 seconds**.

The first clean run predates that fix and is not final-tree evidence: one management
project failed its 20-second scan test, so `-fae` skipped dependent component and
delegation modules (37 successes, 1 failure, 2 skipped). The second clean-reactor
run, after compact fingerprinting, completed all 40 projects: **38 successful, one
failed and one skipped**. The 111-test `dmp.component` suite had exactly one failure,
the retained HTTP successful-reply parity mismatch; its dependent delegation module
was skipped. All other module tests, including management, repository discovery,
capacity, broker authentication, MQTT resource ownership, OPC UA, and ordinary
response-Lua behavior, passed. The verified outputs were old static replies
`{"flag":true,"text":"001","nil":null,"Restart":{"x":1}}` and
`{"Restart":[false,2,"002",null]}`, versus Java-plan replies `{"x":1}` and
`[false,2,"002",null]`; both returned the same two 502 failure categories and had
matching request/send counts.

On that final clean run the normal 32/16 capacity tier completed in 10.615 s; the
256-desired/128-admitted tier completed in 103.743 s with three replacement waves,
burst repair, removal, 4,096 model reads, 34 membership pages and zero ACTIONs.
The largest sampled process heap was 1,091 MiB and post-removal was 744 MiB in the
default 8 GiB test JVM.

Packaging then succeeded for all 40 reactor projects with tests skipped and
`docker.skip=true` in **57.5 seconds** (`/tmp/kilo/runtime-remaining-gates-package.log`).
Post-package acceptance ran both runtime and comparison tests against the actual
`dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar`, which forks with `java -jar`:
**25 passed, zero failures/errors/skips** in
`/tmp/kilo/runtime-remaining-gates-packaged-acceptance.log`. This includes 3
packaged lifecycle/recovery tests, 7 lifecycle tests, 3 health, 6 readiness and 6
comparison tests. No image goal was invoked.

Remaining local work: final whitespace/hash/local-process checks. Retain the HTTP
reply-parity result as an explicit blocker unless reconciled without weakening either
response-Lua semantics or the static/Java outcome comparison. External live staging,
asset migration, deployment, image builds/pushes, commits and R8 remain excluded.

### Earlier Local Gate Continuation

The subsequent `continue` requests retained the original exclusions. Existing
dirty work was preserved. Delegated sessions reached their usage limits without
saving implementation; the remaining local changes and verification were completed
in the main session. No commits, image builds/pushes, application deployments,
registered-asset/parent-bootstrap changes or Python retirement occurred. Only
new loopback/embedded fixtures were used, and temporary MQTT5 containers using the
already available image were stopped/removed after each full acceptance run.

Local gates closed:

- Exact old-shell reads now confirm owner/reference disappearance rather than
  interpreting paginated omission as deletion. 401/403/503 and malformed identity
  retain installed ownership as unconfirmed. Exact old references also prevent
  false owner transfer/ambiguity resolution. The service passes installed owner
  identities into discovery, not merely AIMC IDs, preserving evidence across a
  superseded scan. A new owner remains blocked as `prior-owner-cleanup-pending`
  until all old ownership for that AIMC is retired; unrelated AIMCs still progress.
- `DMP_MANAGED_AIMC_IDS` is an optional nonempty JSON array of up to 8192 exact IDs,
  intersected with owner scope in both active runtime and comparison. IDs are not
  trimmed or case-folded; commas are supported. Unset/blank allows all; explicit
  `[]`, malformed, duplicate/non-string/blank IDs are rejected rather than silently
  broadening adoption. Excluded AIMCs are not individually fetched/compiled for
  adoption; membership pages and selected dependencies remain repository reads.
  Selected future creation/deletion/recreation is covered.
- An embedded broker with topic auto-creation disabled proves subscription to a
  later-created topic and real two-member rebalance/resubscription. The regression
  exposed `latest` resetting past an event arriving during assignment. The dedicated
  control consumer now explicitly uses `earliest` only without committed offsets;
  data-plane offsets and device execution policy are unchanged.
- `ManagedPackagedRuntimeIntegrationTest` executes production main in a separate
  JVM, never another runtime engine. It hosts two valid AIMCs plus an invalid
  sibling on one listener, verifies decoded caller values, actual Lua request/reply
  behavior and one-send counts, stops/releases listener and consumer ownership,
  and reconstructs on restart without startup actions. A second case durably
  acknowledges an event while a real repository fetch blocks application, SIGKILLs
  the process, restarts from updated repository state without event/command replay,
  and repairs a later eventless change periodically without actions.
- Crash recovery waits for a **different Kafka member identity** from the dead
  process; an old timed-out member is not evidence of new ownership. The initial
  fixture had falsely accepted that stale member and could stop during rejoin.
- Actual JAR acceptance found a real dependency-closure bug: direct test-scoped
  `kafka-clients:3.4.0` shadowed the transitive runtime client, leaving the packaged
  application without `org.apache.kafka.clients.consumer.Consumer`. The client is
  now runtime-required, preserving the verified version; broker/server libraries
  stay test-only. Startup catches linkage failures for cleanup and FAILED status,
  rather than orphaning a starting listener after main-thread failure. A lifecycle
  regression verifies the failure cause and partial-context cleanup.
- Resource SPI metrics and immutable lifecycle snapshots report owned bundles,
  stages, routes, REST reservations, endpoint ledger entries and resources plus
  fences, Lua bindings, ingress inflight and pending/rejected/failed extra writes.
  Fixed metric keys expose no identifiers or transport settings; reads do not
  take the lifecycle lock. Real old-request drain tests prove nonblocking access.
  Counters describe currently owned generations, not durable audit totals or every
  lazy connection/thread/socket. `repairPending` prevents convergence from becoming
  true while an accepted update still waits for application.
- Bounded local stress uses 32 desired real typed/Lua mappings with capacity 16,
  a 1000-event burst, three replacement waves and confirmed removal. Admission and
  tracking remain bounded; resource counts return to the same baseline and then
  zero, with **zero ACTION sends**. This is not an 8192-mapping/heap-isolation proof.

Final verification for this local-gate continuation:

- Combined focused run: **67 passed**, no failures/errors/skips, in
  `/tmp/kilo/runtime-reconciliation-local-gates-focused-final.log`. Subsequent
  transfer-fencing and linkage-cleanup regressions are covered by the final reactor.
- Final clean reactor: **BUILD SUCCESS, 40 projects, 28 test-bearing projects,
  531 tests reported: 530 passed, zero failures/errors, one existing Hono skip**.
  Vintage/JUnit4, independent raw Java model fixtures, generated descriptor gates
  and the explicit isolated MQTT5 gate all ran. Descriptors were reused from
  `/tmp/kilo`; no checked-in fixture regeneration occurred.

```bash
DMP_MQTT5_URI=tcp://127.0.0.1:28883 mvn -B \
  -l /tmp/kilo/runtime-reconciliation-local-gates-clean-closure.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 clean test -fae \
  -Ddmp.contract.dir=/tmp/kilo/runtime-reconciliation-r0-contracts \
  -Ddmp.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/stoppering \
  -Ddmp.opcua.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/opcua-method \
  -Ddmp.http.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/http-operation
```

Packaging: **BUILD SUCCESS, all 40 projects**, Docker profiles not enabled and
image builds/pushes disabled; runtime client/server scope is verified through the
actual artifact rather than inferred from test-classpath startup.

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-local-gates-package-verified.log \
  -Ddocker.skip=true package -DskipTests dependency:copy-dependencies \
  -DincludeScope=runtime -DoutputDirectory=target/lib
```

Final post-package acceptance: **14 passed**, zero failures/errors/skips. The two
active subprocess tests and executable comparison case use `java -jar` with the
real adjacent runtime dependency directory. Remaining library comparison and six
lifecycle cases include initial event fencing and startup linkage cleanup. This
uses the local JVM and isolated fixtures, not a built/deployed application image.

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-local-gates-packaged-verified.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 -pl dmp.component -am \
  -Dtest=ManagedPlanComparisonTest,ManagedPackagedRuntimeIntegrationTest,ManagedRuntimeLifecycleTest \
  -Ddmp.comparison.jar=/home/tristan/repositories/AP2030-UNS/aas-camel-dmp/dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar \
  -Ddmp.runtime.jar=/home/tristan/repositories/AP2030-UNS/aas-camel-dmp/dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Python oracle: `PYTHONPATH=../../aas-model/src:. python3 -m pytest` from
`management-node`: **206 passed**. All 15 golden Lua checksums remain unchanged,
as does the protected dirty SQL fixture at
`481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.
`git diff --check` passed. Earlier verification snapshots below remain historical.

Remaining work is narrower: targeted discovery/invalidation-graph fetching,
broker-outage/assignment readiness beyond route-start state, maximum-fleet stress
and per-protocol/heap observability, mixed-protocol simulator/fleet semantic and
send-count parity, and separately approved live staging/cutover. R8 remains
unauthorized and blocked; no local build, JAR proof or counter authorizes retirement.

### Earlier Lua/R7 Continuation Evidence

This section supersedes earlier verification snapshots below. The continuation
preserved the existing uncommitted implementation. No commits, image builds/pushes,
application deployment, registered-asset changes, parent deployment/bootstrap edits
or Python retirement occurred. A new test-only `eclipse-mosquitto:2` container used
the existing local image and loopback `127.0.0.1:28883`; it was stopped/removed after
verification. All other repository/device peers and Kafka tests were loopback or
embedded fixtures. Existing/deployed brokers were not used.

`ManagedLuaReconciliationTest` adds eight real repository/compiler/sandbox/staged
lifecycle proofs: unchanged route/binding/source/prototype identity; old request
plus old response scripts during drain with a live sibling; stale compile versus
new update/removal; newer invalidation during old-request drain; removal/recreation
without retired ownership; invalid request/response Lua retaining last good without
replay; and steady route/endpoint/Lua bindings after repeated changes/removal.

Those tests found discovery recompiling unchanged Lua and stale retired `Active`
ownership after superseded drain. The compiler now reuses only immutable sources,
not evaluation state, through a 65,536-slot weak cache with per-source in-flight
futures. Independent sources compile outside the cache monitor. Retired sources
are collectible; live entries are never evicted, and exhausted live capacity
rejects a new preflight without disturbing old valid scripts. Two tests prove
concurrent same-source reuse and capacity rejection without live-source eviction.
This metadata-capacity test is not a maximum-fleet runtime/heap proof.

The R5/R6 review fixed concrete defects rather than trusting phase labels:

- Overflow admission/status tracking stays bounded across repeated scans.
- Failed/superseded replacement refreshes actual bundle ownership; missing handles
  allow fresh installation, and inactive survivors finish cleanup first.
- Failed removals remain visible until cleanup succeeds.
- Accepted events plus repository failure respect retry deadlines/backoff.
- Dependency/AIMC fences let unrelated work progress; unknown/overflow events retain
  full repair, and every mid-scan event prevents premature global convergence.
- List absence/semantic loss of known or installed AIMCs receives exact confirmation.
  Transient/auth failure preserves last good; installed IDs remain protected across
  superseded deletion/recreation even after a previous confirmed-404 discovery.
- Named mapping equality excludes list position while diagnostics remain current.
- Only complete-metadata-matching value-only updates to known installed direct
  Properties suppress structural repair. Partial/unknown events remain conservative;
  control events do not replace the existing data-plane engine.
- Control starts before initial repository reads, and a startup mid-scan invalidation
  is repaired before readiness. Dependency transport/auth or unconfirmed-membership
  failures no longer report READY.

Thirteen new service/discovery regressions cover these repairs. Shared status adds
tokenized per-mapping state, generation, desired/installed fingerprints, convergence,
accepted invalidations and scan-failure/overflow metrics. Raw sensitive URIs and
authorization are not exported; malformed repository URI diagnostics are sanitized.

`DmpExecutable --compare` performs one-shot repository GETs and real model/Lua
preflight before any runtime/context or legacy loader is constructed. It needs
read settings/scope/pins but not Kafka, Python, Docker or descriptors. It creates no
device subscribers/pollers/actions/writes or Lua registry bindings. It reports
`activated=false`, completeness/failures, safe fingerprints, connector/delivery/
polling summaries, authored/response/trigger ID tokens, dependencies and Operation
binding conflicts. `eligible` is not `installed` or evidence of device permission.
Five tests cover static Python-oracle authored Lua bytes, repeatable credential-safe
output, invalid/conflicting candidates, repository failure, zero HTTP polling/action/
write traffic, zero MQTT connections, and the one-shot executable bypassing active
managed mode and legacy files. The executable test is rerun against the actual JAR.

Intentional descriptor difference: Python emits a synthetic response-passthrough Lua
script when no response Blob was authored. Java compiles only authored Blobs and
uses native reply stages for that default. Compare authored bytes and reply outcomes,
not synthetic-script counts; golden fixtures were not changed to hide this difference.

The README records runtime/comparison settings and limitations; checked-in pins,
delegation paths/bases and retired property declarations; ID-injection/registration
publication; stored main/E2E routes/client identities; direct E2E and AAS-backed BT
callers; validation/polling/MQTT/Kafka delivery implications; and separately scoped
exclusive-owner cutover/rollback. This is source/local-artifact inventory, not live
repository, loaded-tree, container, broker-client or consumer-group verification.

Final focused run: **49 passed, zero failures/errors/skips** (cache 2, discovery 6,
service 14, control 3, lifecycle 5, settings 4, status 2, comparison 5, real Lua 8).

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-continuation-focused-final.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 -pl dmp.component -am \
  -Dtest=ManagedLuaReconciliationTest,ManagedLuaCompilationReuseTest,MappingReconciliationServiceTest,RepositoryDiscoveryTest,AasControlEventTest,ManagedPlanComparisonTest,ManagedRuntimeSettingsTest,ManagedRuntimeStatusTest,ManagedRuntimeLifecycleTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Final clean reactor: **BUILD SUCCESS, 40 projects, 28 test-bearing projects,
520 tests reported, 519 passed, zero failures/errors, one existing Hono skip**.
Vintage/JUnit4 ran alongside Jupiter; raw Java model contracts ran independently
of Python. Generated descriptor and MQTT5 gates were explicitly enabled using the
previously generated `/tmp/kilo` descriptors; checked-in fixtures were not regenerated.

```bash
DMP_MQTT5_URI=tcp://127.0.0.1:28883 mvn -B \
  -l /tmp/kilo/runtime-reconciliation-continuation-clean.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 clean test -fae \
  -Ddmp.contract.dir=/tmp/kilo/runtime-reconciliation-r0-contracts \
  -Ddmp.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/stoppering \
  -Ddmp.opcua.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/opcua-method \
  -Ddmp.http.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/http-operation
```

Packaging: **BUILD SUCCESS, all 40 projects**, image builds/pushes disabled. Runtime
dependencies were populated without enabling Docker profiles.

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-continuation-package.log \
  -Ddocker.skip=true package -DskipTests dependency:copy-dependencies \
  -DincludeScope=runtime -DoutputDirectory=target/lib
```

Packaged-JAR comparison: **5 passed, zero failures/errors/skips**. This validates
the actual comparison entry point/dependency closure, not active Kafka/device lifecycle.

```bash
mvn -B -l /tmp/kilo/runtime-reconciliation-continuation-packaged-comparison.log \
  -Ddocker.skip=true -Ddatabridge.health.port=0 -pl dmp.component -am \
  -Dtest=ManagedPlanComparisonTest \
  -Ddmp.comparison.jar=/home/tristan/repositories/AP2030-UNS/aas-camel-dmp/dmp.component/target/dmp.component-1.0.0-SNAPSHOT.jar \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Python oracle: from `management-node`,
`PYTHONPATH=../../aas-model/src:. python3 -m pytest`: **206 passed**.
All 15 model-contract Lua hashes match pre-run values; the SQL fixture remains at
`481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`, preserving
its pre-existing dirty state. `git diff --check` passed.

Explicit open gates:

- Unfiltered `owner-not-found` retains stale mappings. Confirmed old-owner loss needs
  an exact shell/reference recheck; list absence alone is not deletion authority.
- Discovery still scans/fetches all AIMCs despite independent application fences.
  Exact AIMC-ID partition filtering is absent; shell scope is not the old allowlist.
- Partial/unknown Property payloads conservatively trigger discovery.
- Active packaged-JAR lifecycle and absent-topic/rebalance/crash-after-commit-before-
  apply proof are absent. Graceful-close empty-repository Kafka recovery does not
  establish them or exactly-once device execution.
- Maximum-fleet resource/heap stress, complete resource/rejected-write observability,
  mixed-protocol simulator outcome/send-count parity and authorized live gates remain.
- External R7 cutover remains gated. R8 Python removal and parent deployment/asset
  migration remain separately scoped and unauthorized.

### Current Code Boundaries

- `dmp.management/model/WireModel.java` and `WireProjection.java`: immutable,
  finite wire records; AIMC 2/0 and AID 1/1; typed ordered references, presence,
  forms, schemas, operation variables and Blob validation.
- `dmp.management/repository/AasRepositoryClient.java` and
  `AasReferenceResolver.java`: read-only bounded HTTP, Base64URL IDs, exact
  anchors/typed traversal, deterministic rejection of ambiguous shell ownership.
- `dmp.management/compiler/MappingPlanCompiler.java`: ordered resolved plans
  with owner context and per-compilation fetch reuse. `compileDesired` retains
  isolated valid/rejected mapping outcomes, dependencies and effective identity;
  `WireProjection.aimcIsolated` preserves valid siblings of malformed mappings.
- `dmp.management/compiler/ConnectorConfigurationFactory.java`: typed runtime
  configuration and isolated bundle output for ordinary AAS, HTTP, MQTT, OPC UA,
  Modbus/PLC4X, BACnet/PLC4X and IO-Link/HTTP forms. Operation fixtures emit HTTP,
  OPC UA Method and grouped MQTT 5 bridge routes with managed Lua. These are
  configuration/lifecycle proofs, not complete real-producer/device parity.
- `dmp.core/component/DmpComponent.java`, `DmpRouteBuilder.java`,
   `ExecutionPolicyRouteCreator.java` and `ProtocolBridgeRouteCreator.java`:
  exact managed resource inventories, staged install/replace/remove, generation
  fencing, bounded drain and previous-definition restoration. Replacement is
  stop/start, not a lossless transaction; do not infer ownership from
  whole-context route scans.
- `LuaCompiledSource.java`, `LuaTransformerConfiguration.java` and
  `ManagedRouteResource.java`: immutable in-memory transforms and explicit
  bind/close ownership. Do not use `RoutesConfigurationLoader` for managed
  multi-AIMC startup; it still resets one shared temporary directory.

### Explicit Fixture Outcomes

- The refreshed Stoppering fixture uses relevant submodels from the checked
  station config: Property-typed Variables and matching typed references. It is
  an explicit negative for retired `writeDelegation` declarations. The separate
  new-builder candidate has unsupported CCI InOut mappings. Neither fixture
  implies a deployed owner shell or authorizes station registration; the older
  captured SMC-to-Property mismatch remains historical/negative evidence.
- `opcua-read` has no usable node href/browsePath. The compiler rejects it rather
  than accepting the old descriptor's empty `nodeInformation` as runtime parity.
- `projection-extensions` verifies dual binding, type-less schema, response
  references, qualifiers and trigger preservation; its response-extension route
  contract is still explicitly unsupported by the runtime factory.
- Other `route-configured` cases have passing typed configuration assertions.
  The three operation cases also pass generated-route install/remove/re-add
  tests without Python or generated descriptor loading. Selected HTTP operation
  and polling cases additionally exercise values/send counts at real loopback
  transport boundaries; this is not exhaustive mixed-protocol live staging.

### Earlier Verification At Handover

- Latest current-tree focused check: **53 tests passed, zero failures/errors or
  skips**, after fixing the new submodel-header helper's compile error.
  Log: `/tmp/kilo/runtime-reconciliation-handover-tests-final.log`.
  Command from the DMP repository:
  `mvn -B -l /tmp/kilo/runtime-reconciliation-handover-tests-final.log -Ddatabridge.health.port=0 -pl dmp.component -am -Dtest=WireProjectionTest,RawModelContractFixtureTest,ConnectorConfigurationFactoryTest,MappingPlanCompilerTest,AasRepositoryClientTest,AasReferenceResolverTest,ManagedBridgeLifecycleTest,ManagedOrdinaryBundleLifecycleTest,LuaCompiledSourceCamelTest,ResponseSinkFanOutProcessorTest,NativeValueBoundaryTest -Dsurefire.failIfNoSpecifiedTests=false test`.
- Latest Python authoring/fixture run: **206 passed**. Run from
  `management-node`: `PYTHONPATH=../../aas-model/src:. python3 -m pytest`.
- Earlier clean full reactor: **40/40 modules passed, 389 tests reported,
  388 passed and one pre-existing Hono skip**. Log:
  `/tmp/kilo/runtime-reconciliation-r4r3-clean-retry.log`.
  That run predates the latest connector factory, operation compilation and
  header helper changes; it is not a current-tree full-reactor certification.
- Earlier 40-module packaging passed:
  `/tmp/kilo/runtime-reconciliation-r4r3-package.log`. Package the latest tree
  again after the new session's full verification.
- Local MQTT 5 Operation Delegation previously passed against an isolated
  loopback Mosquitto broker. That test container has been stopped for handover.
  Recreate it for full acceptance; do not point tests at deployed assets.
- `git diff --check` and the SQL checksum passed at handover. No commit, image
  build/push, parent deployment edit, deployed-asset mutation or Python retirement
  was performed.

### Verification On 2026-10-13

- Re-ran the clean 40-module reactor successfully with the generated runtime
  contract directory and an isolated loopback-only Mosquitto broker on port
  28883. The module summaries report **464 tests, 463 passed, zero failures or
  errors, and one existing Hono skip**. Log:
  `/tmp/kilo/runtime-reconciliation-20261013-clean-final3.log`.
- Re-ran 40-module packaging successfully with tests skipped and Docker image
  builds disabled. Log:
  `/tmp/kilo/runtime-reconciliation-20261013-package-final.log`.
- Generated HTTP Operation and HTTP polling acceptance tests use local in-memory
  HTTP peers and exercise authored Lua, one device send, response failure
  isolation, decoded numeric-looking text and AAS PATCH. MQTT deadline tests use
  a packet-level loopback peer and pass bounded SUBACK/PUBACK/PUBREC/PUBCOMP,
  no-replay, reconnect and client-thread retirement checks.
- The protected SQL fixture remains at SHA-256
  `481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.
  Python remains in place. No commit, image build/push, parent deployment edit,
  deployed-asset mutation or Python retirement was performed.
- This was the pre-clarification state: R1 was recorded as blocked by synthetic
  owner evidence. The user then narrowed R1 to the relevant station submodels;
  see **Relevant Station Submodels** below. That submodel-only contract now
  records explicit outcomes for retired declarations and unsupported CCI InOut
  mappings without claiming deployed state. R2 lifecycle and R4 failure
  isolation/fingerprint coverage remain green. R5 still requires the latest
  full clean/package rerun; R6 remains gated on R5.

### Projection Isolation Continuation

- `WireProjection.aimcIsolated` validates the AIMC root and mapping-list
  structure globally, then projects each mapping independently. A malformed
  entry is retained as a rejected mapping with its identity/index, safe pointer,
  and parseable Source/Sink references; valid siblings continue to resolve.
- `MappingPlanCompiler.compileDesired` carries rejected mappings into desired
  outcomes and dependency sets. `createDesiredBundles` gives blocked mappings
  secret-safe effective fingerprints; credential-sensitive internal identity
  remains distinct while exported digests redact tokens.
- Focused projection/compiler/repository tests pass, including a valid sibling
  beside a malformed mapping and dependency/fingerprint assertions. The latest
  clean reactor reports 466 tests across 27 test-bearing modules, zero failures
  or errors, and the existing Hono skip. Package verification also passes.
- Latest validation after fixture-manifest regeneration: clean reactor log
  `/tmp/kilo/runtime-reconciliation-20261013-clean-continuation.log`, package log
  `/tmp/kilo/runtime-reconciliation-20261013-package-continuation.log`, Python
  oracle **206 passed**, and `git diff --check` passed. SQL fixture SHA-256 remains
  `481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.
- Earlier focused logs: `/tmp/kilo/r1-r4-isolation-fingerprint-final.log` and
  `/tmp/kilo/r1-r4-acceptance-focused-final.log`.

### Relevant Station Submodels

- Following the clarified scope, the Stoppering raw fixture is built from
  `AASDescriptions/Resource/configs/syntegonStoppering.main.json` with
  `aas-model`'s `merge_instance_config`, `ResourceTypeAAS` validation, ID
  injection and BaSyx serialization. Only serialized submodels are retained;
  no shell is synthesized or used to infer ownership. `Variables/PackMLState`
  and `Variables/OccupationState` are Properties and the AIMC keys are typed
  accordingly.
- The current station configuration includes retired `writeDelegation`
  declarations in AIMC and Variables. Java rejects them explicitly. A separate
  `build_resource_type_aas` candidate is created in memory; its optional
  nonpositive default polling interval is omitted in the fixture, but its
  connector plan still reports CCI InOut Operation inputs as unsupported. The
  candidate is therefore not push-ready and was not registered.
- `Registration_Service` exposes `register-config`; its conflict path deletes
  and recreates existing shell/submodel data. It was inspected only. No register,
  POST, delete, deployment or asset mutation was performed.
- Focused evidence: `/tmp/kilo/r1-submodel-only-java-tests-green.log` (raw
  matrix) and `/tmp/kilo/r1-r4-submodels-focused.log` (R1/R4 selection).

### R5 Reconciliation Verification

- `RepositoryDiscovery` paginates semantic submodel headers, identifies AIMCs by
  semantic ID, resolves exact shell ownership, then processes AIMCs with bounded
  parallelism and per-AIMC failure preservation. `MappingReconciliationService`
  performs full-scan repair, reverse dependency invalidation, no-op fingerprint
  reuse, fresh staged replacement, last-good retention on transient failures,
  removal on confirmed 404/deletion, duplicate REST binding preflight, and
  bounded exponential retry after full-scan failures.
- `KafkaControlEventRouteBuilder` uses a dedicated single-consumer group, manual
  commits only after bounded invalidation acceptance, and maps malformed or
  unrecognized envelopes to full reconciliation. It is wired by the managed
  executable and tested through an advised direct source, including commit
  ordering; no Kafka broker was started.
- Focused R5 tests pass: `/tmp/kilo/r5-current-focused-final.log`,
  `/tmp/kilo/r5-parallel-repair-final.log`, and
  `/tmp/kilo/r5-kafka-route-commit-retry.log`.
- The final combined R5/R6 clean reactor reports **486 tests across 27
  test-bearing modules, zero failures or errors, one existing Hono skip**. Log:
  `/tmp/kilo/runtime-reconciliation-20261013-r6-final-clean-pass.log`.
- Package: **BUILD SUCCESS**, tests skipped, Docker image builds disabled. Log:
  `/tmp/kilo/runtime-reconciliation-20261013-r6-final-package.log`. Python
  remains at **206 passed**. The SQL fixture hash remains
  `481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.

### R6 Managed Startup

- `DMP_MANAGED_RECONCILIATION` is an explicit opt-in and defaults to false. In
  managed mode `DmpExecutable` bypasses `RoutesConfigurationLoader` and legacy
  route/config environment loading; standalone launch behavior is preserved.
- Managed settings validate the repository URI/auth header, DOT_AAS_V3 API,
  separate control topic/group, owner filter, local-AID pins, page/worker/scan
  limits, queue/mapping capacities and repair interval. Authorization is
  redacted from settings text and never included in status.
- Startup sets readiness to starting, starts the empty DMP context and health
  surface, performs an initial repository reconciliation, adds the control
  route, then starts periodic repair. `/health` remains unavailable until the
  initial scan and control route are ready. `/management/status` is only added
  in managed mode and exposes counts/categories without raw IDs or credentials.
- Shutdown marks readiness down, performs a final full scan, stops the repair
  service, then stops Camel routes so staged/managed bundles are retired before
  context teardown. Tests verify successful and failing repository startup,
  control-route ordering, readiness transitions, status route gating and actual
  Camel context shutdown.
- `ManagedRuntimeIntegrationTest` starts the actual DMP Camel context against
  loopback repository/device HTTP peers, discovers two simulated AIMCs, serves
  their Operations on one configured port, preserves the valid mapping beside
  one malformed sibling, then stops and reconstructs routes from repository
  state on restart.
- Kafka listener behavior is tested with an advised direct source and manual
  commit stub, plus an embedded local Kafka/ZooKeeper broker. The acknowledged
  event is not replayed after consumer restart; no deployed broker was contacted.

### Final R6 Validation

- Clean reactor: **488 tests across 27 test-bearing modules, zero failures or
  errors, one existing Hono skip**. Log:
  `/tmp/kilo/runtime-reconciliation-20261013-final-verified-clean.log`.
- Package: **BUILD SUCCESS**, tests skipped and Docker image builds disabled.
  Log: `/tmp/kilo/runtime-reconciliation-20261013-final-verified-package.log`.
- Python manager: **206 passed**. `git diff --check` passed. Protected SQL
  fixture remains SHA-256
  `481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.
- Embedded Kafka tests verify manual commit and offset persistence. The two-AIMC
  Operation acceptance uses loopback AAS/device peers and a direct control-route
  fixture. No image, deployed asset, deployed broker or parent deployment was
  changed.

### Continuation Agreement (2026-10-13)

The user accepts that the remaining Lua replacement proof and later cutover/
retirement phases are not yet done. Leave them explicitly open; this agreement
does not authorize skipping the tests, changing deployed assets or retiring
Python. "Concurrent Lua replacement" means a script/configuration update while
old requests or reconciliation work are in flight, not parallel script execution.
Generic staged replacement and fingerprint no-op tests already pass; the missing
proof must exercise actual Lua scripts/resources across those transitions.

### Next Session After This Continuation

1. Read the source-of-truth plan and this handover first. Review the existing
   uncommitted implementation before building further on it; do not regenerate
   or repeat completed phases blindly.
2. Continue maximum-fleet/performance, per-protocol socket/heap and authenticated
   broker integration gates. Targeted graph repair, broker outage/own-assignment
   readiness, ownership/filtering, local crash repair and actual JAR proofs now pass;
   do not redo them blindly or infer fleet acceptance from local evidence.
3. Use the implemented R7 comparison/migration inventory to prepare separately
   approved simulator/live parity gates. Do not dual-run active owners to compare;
   keep external rollout and R8 retirement gated.
4. Preserve every existing worktree change and SQL fixture bytes. Do not commit,
   register/push a station, build/push an image, edit parent deployment files,
   mutate deployed assets or retire Python. Use isolated local test services.
5. Re-run focused regressions, the full clean reactor with explicit generated
   contract properties and isolated MQTT, and packaging with image builds
   disabled after implementation changes. Update this ledger with exact evidence.

Next-session prompt:
`.kilo/plans/1791749067102-dmp-java-runtime-reconciliation-continuation-prompt.md`.

## Correctness Review at Reconciliation Entry (2026-10-11)

This section records the review state before the Java runtime reconciliation
implementation below. It is not the current completion ledger. The
implementation spans W0-W6 of the parent workspace's
`.kilo/plans/1791539605000-dmp-correctness-camel-native-plan.md`. Its previously
recorded build successes are not proof that all acceptance gates are complete.
The review and commit preparation corrected distinct caller Reply SinkId
selection, rejection of retired write-delegation and action JSONata authoring,
HTTP client automatic retries, legacy UA JSON-String null encoding, and
component JUnit 4 discovery alongside Jupiter tests. The management suite now
passes 205 tests; the focused HTTP and UA adapter selection passes six tests.

The final full-reactor rerun enabled both test engines and explicit generated
descriptor fixtures. Component discovery (three tests), generated contracts
(five), health, Lua smoke, HTTP, MQTT and SQL regressions passed. The component
reports contain 24 tests with six failures and one error: ActiveMQ (including
its environment-variable variant), multiple MQTT sinks, the mixed-source failed
read gate, two AAS polling assertions, and PLC4X. The polling/PLC4X assertions
still expect quoted JSON text; the other failures include missing propagation
and failed-read acceptance. These require investigation and contract-aligned
regressions, not disabling Vintage or weakening assertions solely to pass. The
full reactor gate is not green; earlier successful runs omitted these JUnit 4
tests. W0/W6 local regression closure is therefore reopened before live staging.

Remaining source/acceptance follow-ups include:

- W1/W4: compatible whole-plus-member action/response assembly and a single
  shared assembler; request/reply payload members matching authored IDs must
  not cause an additional implicit envelope unwrap. Disjoint array members must
  not conflict through padding nulls, and a missing ordinary group must not
  prevent unrelated destinations from being sent.
- W4/W4a: deep-copy isolation for detached response job bodies and normalized
  MQTT scalar preservation through fan-in; BaSyx decoded Strings must not lose
  authored surrounding quotes.
- W1/W4a: controlled null markers must reach schema validation as null; response
  projection must distinguish a missing pointer from valid null and detect
  duplicate SourceIds even if the first value is null.
- W4a: numeric-looking object keys must stay object keys through Lua; explicit
  Lua `json.decode` must reject malformed JSON rather than accept bare text.
- W2/W3: producer-chain HTTP/UA timeout and invalid-response outcome coverage,
  native UA Read/Call integration tests, and removal of residual nested template
  device dispatch with explicit route-scoped no-redelivery policy.
- W4/W5: MQTT correlation registration/lifecycle failures, route stop/reload,
  outstanding response jobs, and latest-value cache lifecycle acceptance.
- W3/W6: sanitize transport URIs and error details before logging or returning
  them to callers; connector URIs can contain credentials.
- W6: management image dependency closure must use a model package containing
  `schema_from_datapoint`; the existing ignored `_aas_model` copy is stale, while
  pytest uses the sibling checkout. External model/assets are not migrated by
  the DMP commits.
- W6: approved golden Occupy/Halt and granular Operation staging, required AID
  qualifiers, deployed-authoring migration, and final image runtime checks.
- W0/W6: the SQL example test writes timestamps into a tracked source SQLite
  fixture. Preserve its original bytes in the rename commit and move future
  test database writes to isolated generated output instead of source resources.

Commits group the actual compiler, runtime, connector, fixture/packaging and
documentation changes. They are a dependent series, not separately verified
historical snapshots of each workstream. No deployment or image push is part of
this commit preparation.

### Java Runtime Reconciliation Baseline (2026-10-11)

Implementation of the parent workspace's
`.kilo/plans/1791749067102-dmp-java-runtime-reconciliation-plan.md` started from
`15cdde4`. The first full collection used `mvn clean test -fn` with all four
generated-contract directory properties explicitly supplied. It reported 38 of
39 modules successful; component tests had five failures and one error. The
`-fn` exit code was not treated as success. SQL isolation and the mixed-source
fixture correction were already included in that collection, so it is not an
untouched-HEAD oracle.

R0 prerequisite repairs now cover strict JSON decoding and numeric-looking Lua
object keys, native fan-in scalars, controlled-null schema validation, shared
whole/member and array assembly, independent ordinary sink failures, response
pointer presence/duplicate IDs, select-once replies, detached payload-tree
isolation, BaSyx decoded String quotes, and ordinary AAS connector-local
selectors. MQTT reply ownership rejects duplicate channel correlations, fences
reuse for 60 seconds with a bounded 8,192-reservation capacity, waits for
subscription readiness before publication, and retires last-owner reply routes.
Injectable-transport lifecycle tests are not real MQTT 5 broker acceptance.

The reopened component regressions now have contract-aligned fixtures and
assertions, without disabling Vintage or Jupiter. AAS-to-MQTT and component AAS
polling tests use local brokers and propagate connection failures. The SQL
example writes only to a generated database copy; the existing modified source
`values.db` remained byte-identical before and after both reactor runs:
`481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.

Repeatable local verification (OpenJDK/javac 21.0.12.1, Maven 3.8.7, Java 17
release compilation, Python 3.12.7):

- `PYTHONPATH=../../aas-model/src:. python3 -m pytest` in `management-node`:
  206 passed, including raw fixture regeneration/comparison.
- `PYTHONPATH=../../aas-model/src:. python3 tests/test_runtime_contract_fixtures.py /tmp/kilo/runtime-reconciliation-r0-contracts`
  in `management-node`: all thirteen descriptor cases materialized.
- `mvn -B -l /tmp/kilo/runtime-reconciliation-r0-clean-reactor.log clean test -fae -Ddmp.contract.dir=/tmp/kilo/runtime-reconciliation-r0-contracts -Ddmp.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/stoppering -Ddmp.opcua.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/opcua-method -Ddmp.http.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/http-operation`:
  39/39 modules successful, 349 tests reported, 347 passed, two skipped. Core 92,
  Lua 41, AAS 57, Paho 32 and component 26 passed. All five generated-descriptor
  tests, both mandatory raw-model tests and both SQL example tests ran.
- `mvn -B -l /tmp/kilo/runtime-reconciliation-r0-package.log package -DskipTests dependency:tree '-Dincludes=org.eclipse.basyx:*,com.fasterxml.jackson.*:*,org.springframework:*'`:
  39/39 modules packaged successfully. This is JAR/dependency verification, not
  image inspection or packaged managed-runtime acceptance.

The two skips remain explicit acceptance gaps: the Hono example's existing
unexplained `@Ignore`, and the Operation delegation example's missing
`DMP_MQTT5_URI` assumption. Neither is counted as an exercised connector test.
The executable dependency closure still mixes SDK 1.2.0 with SDK tests 1.4.0,
Jackson 2.14.x/2.18.3, and Spring Framework 7.0.8 with web 6.2.11. No replacement
AAS SDK or dependency-line migration was added.

R1 includes a Java 17 `dmp.management` module with immutable, finite AIMC, AID,
shell, reference, affordance, form, recursive schema, qualifier, operation
variable, protocol binding and Blob projections. It strictly enforces AIMC 2/0
and AID 1/1, preserves ordered typed keys and absent/null values, and fails
malformed consumed fields without adding an AAS SDK. The refreshed raw matrix
uses only relevant submodels for the current Stoppering station and a fresh
in-memory station-builder candidate; it does not synthesize or parse an owner
shell for those cases. The current station is explicitly rejected for retired
`writeDelegation` in its AIMC/Variables submodels. The new builder candidate
uses Property-typed Variables references, but connector compilation explicitly
rejects its CCI InOut Operation inputs. Both negative outcomes are recorded in
the manifest. The separate model-type mismatch test remains a strict negative;
there is no inference from the obsolete captured AIMC to current deployed state.
The manifest's `fullR1Validation` now means complete validation within this
relevant-submodel contract boundary; the external Python event-broker option is
explicitly not an AIMC/AID submodel declaration. Java tests still compare
element paths/identities, ordered references, protocol families, source/sink
identity, optional mapping fields, response/trigger/qualifier data and Blob
bytes. Local HTTP tests exercise runtime values and send counts; no station was
registered or pushed.

R2 now has strict per-mapping route inventories, collector-first ordinary route
definitions, ordinary and Operation install/remove/re-add handles, exact REST
path/method reservations, exact REST consumer endpoint cleanup, candidate
rollback, and bounded stop/drain before resources are retired. Operation
resources own producer templates and unique per-route response executors;
response writes stop admission, drain for a bounded interval, then cancel queued
work or retain the failed-cleanup handle. FanIn cache state clears on bundle
retirement. `stopComponent()` retires its managed bundles before stopping the
context. The REST lifecycle regression keeps a second Operation active while
removing/re-adding the first. This is not yet an atomic generation replace: the
candidate is dynamically installed before old bundle removal, failed drains can
retain the old bundle, and an in-flight REST action/route interruption test is
still required.

R3's immutable, eagerly compiled Lua source is connected to this bundle resource
inventory. Opaque registry references contain no script bytes; size, UTF-8 and
syntax checks happen at authoring/bind time, and per-configuration reference
counts prevent one bundle from unbinding a source still used by a sibling.
Retirement unbinds only after route shutdown/drain. Standalone file/resource Lua
continues through its original loading path. Compile-once, version isolation,
shared-source refcounts and no-file managed execution are covered.

Final local verification (OpenJDK 21.0.12.1, Maven 3.8.7):

- `DMP_MQTT5_URI=tcp://127.0.0.1:28883 mvn -B -l /tmp/kilo/runtime-reconciliation-final-clean.log -Ddatabridge.health.port=0 clean test -fae -Ddmp.contract.dir=/tmp/kilo/runtime-reconciliation-r0-contracts -Ddmp.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/stoppering -Ddmp.opcua.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/opcua-method -Ddmp.http.descriptor.dir=/tmp/kilo/runtime-reconciliation-r0-contracts/http-operation`:
  39/39 reactor modules successful; 361 tests, 360 passed, zero failures/errors,
  and one skip. All five generated descriptor cases, both raw-model cases, all
  SQL/component regressions, managed ordinary lifecycle, and Operation
  Delegation ran. The sole skip is the pre-existing `@Ignore` Hono example. The
  local broker on port 28883 was an isolated Mosquitto container; no external
  or deployed MQTT broker was used for Operation Delegation.
- `mvn -B -l /tmp/kilo/runtime-reconciliation-final-package.log package -DskipTests`:
  all 39 modules packaged successfully. This remains JAR/package verification,
  not a container/image or packaged managed-runtime test.
- Management pytest: 206 passed. `values.db` retained the pre-existing checksum
  `481d23c0feeb93450cca91f4ea676c62eb1c44270ce48edde422736defe2faf4`.

The initial verification above had two skips because Operation Delegation did
not yet receive a broker URI; the final run supplied the isolated local broker,
so only Hono remains skipped.

R4 now has a read-only bounded Java HTTP repository client with Base64URL path
identifiers, authorization without credential-bearing diagnostics, paginated
shell/reference reads and explicit not-found/unauthorized/transient/invalid
outcomes. `AasReferenceResolver` fetches an exact anchored AID, traverses typed
ordered AAS keys, follows only explicit AID schema wrappers, and rejects
ambiguous shell ownership rather than selecting the first match. This is a
tested resolver base, not the Java AIMC/AID-to-runtime compiler. R4's full
connector capability/fingerprint compilation, R5 event/reconciliation loop,
R6 managed executable/cutover and R7-R8 rollout/docs are not implemented.
Remaining gates include validated target-resolution rejection for the
Stoppering type mismatch, native HTTP/UA caller-chain timeout/invalid-response
outcomes,
route-scoped no-redelivery/device-dispatch closure, credential-safe diagnostics,
cache/resource reset, bounded response-job drain and MQTT mapping-removal/
ProducerCache integration. Real MQTT 5 ACL/reconnect/retained/QoS and client
thread-cleanup acceptance, DNS-alias correlation ambiguity and replies later
than the finite fence are not proven. No commit, image push, parent deployment
edit or deployed-asset mutation was performed.

---

## 1. Baseline---

## 1. Baseline (what is already done, verified)

| Area | Where | Status |
|---|---|---|
| M0 rename, README, provenance | repo root | done |
| M0.5 Camel 4.22.0 / Java 17 | parent pom, all modules | done |
| M1 `dmp.camel-lua` sandbox/timeout/reload/harness | `dmp.camel-lua` | done |
| M2 DMP container config mount, `LUA_ENV_VAR_NAME` | `dmp.component`, `deploy/dmp` | done |
| M3 management node (parser/resolver/descriptor/orchestrator) | `management-node/` | done |
| M4 interface transforms (OPC UA canonical adapter) | `dmp.camel-opcua`, `deploy/interface-transforms` | done |
| M5 decision gate (plain containers) | — | done |
| M6 live integration | `e2e.managed_assets.json`, `deploy/dmp` | done |
| M7 REST operation delegation | `dmp.core/operation/*` | done (v2: `restsources.json`; **superseded by the DMP v3 one-MC operation authoring** — the derived REST surface rides in the bridge route's `triggerData`, no REST datasource file exists) |
| §6.4 fan-in/fan-out **route model** | `RouteConfiguration.datasources[]`, `EventRouteCreator` aggregator, `RouteConfigurationDeserializer`, `DataSourceConfiguration.sourceId` | done (tested) |
| Contract tests §8.4 #1–6 + compile-reuse | `dmp.camel-lua/.../LuaContractTest` | done |
| AAS sink coarse `valuePath` (extract own SinkId key) | `dmp.camel-aas/AASEndpoint.extractValuePath` | done |

Build/test commands:
- Java: `mvn -o -pl dmp.core,dmp.camel-lua -am test` (the repo compiles with `--release 21`; requires a JDK 21+).
- Python: `python3 -m pytest` in `management-node/`.

---

## 2. Open work items

### T1 — M8: Reference granularity, coarse vs fine (ADR-014, §6.5, §8)

Goal: a `Source`/`Sink` may resolve to a whole AID data point (**coarse**, default, current)
or to one nested property (**fine**); the management node pins a JSON Pointer (`valuePath`),
and the DMP applies it at the boundary — **disassembly** pre-transform (source), **assembly**
post-transform (sink). Contract tests §8.4 #7–10 become green.

#### T1.1 Shared JSON Pointer utility (`dmp.core`)
- New `org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer` (RFC 6901):
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
- `dmp.camel-jsonata`: verify `JsonataTransformerConfiguration`/`JsonataDefaultConfigurationFactory`
  are usable as-is for a routing dispatch (they are present), and that `jsonatatransformer.json`
  config loads (level of effort: confirm, patch only if needed).
- Contract test §8.4 #11: a JSONata blob and a Lua blob both run through the same SourceId/SinkId
  keyed contract.
- Management-node pytest: semantic-id dispatch (Lua vs JSONata).

### T3 — §6.4 fan-in compile from the management node (complete end-to-end)

The DMP core supports multi-source aggregation, but `descriptor._compile_route` still compiles
only `mapping.sources[0]` and emits the legacy singular `datasource` key.

- `descriptor.py::_compile_route`: iterate **all** `mapping.sources`; add each MQTT source to
  `mqttdatasource.json`; emit `"datasources": [ids...]` (plural, deserializer already accepts it);
  for multiple sources set the Lua transformer `sourceKeys` to the comma-separated SourceId list
  so `aimc_main` receives the aggregated `sources` map (§6.4 example: combine two sources in one
  math transform). Single-source mappings keep `datasource`/single `sourceKeys` (unchanged output).
- New pytest: two-source mapping → `routes.json` has `datasources:[...]`, `mqttdatasource.json`
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

1. Java: `mvn -o -pl dmp.core,dmp.camel-lua,dmp.camel-paho,dmp.camel-aas -am test`
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
re-captured event fixtures); dmp.camel-lua **31 ✓** / core **47 ✓** /
camel-aas **2 ✓**; the Java `GeneratedDescriptorContractTest` loads the
regenerated 13-case fixture matrix through the real factories (4/4 ✓,
`fine-protocols` included); aas-model **12 ✓** (incl. the container-shape
regressions); Planar_Controller occupation **8 ✓** (both state-machine copies).
The lab e2e checklist ran against the main stack — see §8.6 for the verified
items and the two that still need authored lab data.

**Environment note:** the only failing module in a full `mvn test` is
`dmp.examples.aas-jsonata-mqtt` (TestAASUpdater) — it publishes to the public
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

aas-model 4/4; management-node pytest 104/104; dmp.core 34/34;
dmp.component 13/13 (incl. smoke test on the new semantics); examples
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

1. **`dmp.component/target/lib` is only refreshed by the `docker-push`
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
