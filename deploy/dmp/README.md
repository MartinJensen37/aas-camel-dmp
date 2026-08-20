# Per-asset DMP container (aas-camel-dmp)

One DMP container per asset (ADR-012): it hosts N `MappingConfigurations`
(multiple mappings, one process). The management node (M3) writes the config
into `./config` and starts/updates/stops the container; for manual runs you
can also just fill `./config` yourself.

## Layout

```
config/
├── routes.json               # route topology (datasource -> transformers -> datasinks)
├── timerconsumer.json        # datasource config (timer), if a timer route is used
├── luatransformers.json      # LUA transformer configs (SPEC §8.5)
├── mqttdatasink.json         # datasink config (MQTT publish)
└── queries/
    └── <asset>_<mapping>.lua # AIMC transformation blobs (runtime-loaded, reload on change)
```

The container mounts `./config` at `/usr/share/config` (the executable's
default config path) and runs with `-Xmx512m` (the effective per-blob memory
bound, SPEC §9.3).

## Example

```bash
# build the DMP image (from the repo root)
cd aas-camel-dmp
mvn -DskipTests package
# place your config (see config/ examples) then:
docker compose -f deploy/dmp/docker-compose.yml up -d
# health check
curl http://localhost:8085/health
```

The `lua:` transformer in `routes.json` is referenced by uniqueId from
`luatransformers.json`; each entry points at a `.lua` blob in `queries/`.
Dropping in a new blob is picked up without a restart (mtime/sha reload,
SPEC §7.4).
