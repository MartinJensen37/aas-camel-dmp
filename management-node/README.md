# Management node (M3 — planned)

The management node is the orchestrator that instantiates/updates/stops
per-asset DMP containers from AAS events (SPEC §11). It is a **later milestone
(M3)**.

Language: **Python** (ADR-013) — the AID/AIMC pydantic models already exist in
Python (`aas_pydantic` + `Registration_Service` templates), and the Kafka
eventing conventions follow `kg-bridge` (Python).

## Planned structure (kept largely separated)

```
management_node/
├── kafka_listener.py      # AAS eventing (Kafka only, ADR-010)
├── aas_client.py          # fetch submodels from the AAS REST API
├── parsers/
│   ├── aid.py             # DEDICATED AID parser (no AIMC knowledge)
│   └── aimc.py            # DEDICATED AIMC parser (no AID knowledge)
├── resolver.py            # links AIMC Source/Sink refs -> AID endpoints (separate)
├── descriptor.py          # compiles a DMP descriptor (routes.json, luatransformers.json, blobs)
└── deploy.py              # docker container lifecycle per asset
```

The AID and AIMC parsers are self-contained and do not import each other;
reference resolution and descriptor compilation are separate steps.

## Groundwork

`tests/data/` contains the real `AssetInterfacesDescription` and
`AssetInterfacesMappingConfiguration` submodels (extracted from
`AASDescriptions/Resource/configs/syntegonStoppering.json`) to drive the parser
tests.
