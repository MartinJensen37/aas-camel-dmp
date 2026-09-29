# Management node (M3)

The management node orchestrates per-asset DMP containers (SPEC §11):
it reacts to AAS events (Kafka), parses the asset's **AID** + **AIMC**
submodels, resolves references, compiles the DMP descriptor
(`routes.json`, `luatransformer.json`, `queries/*.lua`), writes it into the
asset's config dir and (re)deploys the DMP container. Reconciliation is
idempotent and single-flight per asset.

Language: **Python** (ADR-013) — the DMP data plane stays Java/Camel.

## Run

```bash
export AAS_BASE_URL=http://localhost:8081
export MANAGED_ASSETS_FILE=example.managed_assets.json
export KAFKA_BOOTSTRAP=kafka:9092
python -m management_node
```

Env vars (kg-bridge conventions, ADR-010): `KAFKA_BOOTSTRAP`,
`KAFKA_TOPIC_PATTERN`, `KAFKA_CONSUMER_GROUP`, `KAFKA_AUTO_OFFSET_RESET`,
plus `AAS_BASE_URL`, `DMP_CONFIG_ROOT`, `DMP_IMAGE`, `MANAGED_ASSETS_FILE`.

Operation-delegation (DMP v3): the REST surface is derived by convention —
`POST /operations/{aas_id_short}/{action}` (the action = the owning skill's
idShort), carried in the bridge route's `triggerData`. Nothing configurable.
Reply topics come from the DECLARED `forms.response.href` of the command
affordance — no `/CMD/`→`/DATA/` rewriting anywhere.

## Structure (kept largely separated)

```
management_node/
├── kafka_listener.py      # AAS eventing (Kafka only, ADR-010); AIMC discriminator
├── aas_client.py          # fetch submodels from the AAS REST API
├── submodel_locator.py    # reference-anchored AID directory for cross-asset refs (ADR-019)
├── endpoints.py           # shared <scheme>://host[:port][/path] endpoint parsing
├── parsers/
│   ├── aid.py             # DEDICATED AID parser (no AIMC knowledge)
│   └── aimc.py            # DEDICATED AIMC parser (no AID knowledge)
├── resolver.py            # links AIMC Source/Sink refs -> endpoints; anchor-first cross-asset (ADR-019)
├── descriptor.py          # compiles DMP config files + Lua blobs (scoped ids, ADR-017)
├── deploy.py              # docker container lifecycle per asset (hashed health ports)
└── orchestrator.py        # idempotent reconcile off the Kafka thread (entry: main.py)
```

The AID and AIMC parsers are self-contained and do not import each other;
reference resolution and descriptor compilation are separate steps.

## Tests

```bash
python -m pytest tests/
```

`tests/data/` holds the real `AssetInterfacesDescription` and
`AssetInterfacesMappingConfiguration` submodels (extracted from
`AASDescriptions/Resource/configs/syntegonStoppering.json`).

## Container

`Dockerfile` packages the management node; it drives the host docker CLI
(mount `/var/run/docker.sock`) and talks to Kafka + the AAS REST API.
