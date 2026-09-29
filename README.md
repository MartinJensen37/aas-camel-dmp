# Eclipse BaSyx DMP
[![BaSyx Logo](https://www.eclipse.org/basyx/img/basyxlogo.png)](https://www.eclipse.org/basyx/)

The Eclipse BaSyx DMP is part of the [Eclipse BaSyx middleware](https://www.eclipse.org/basyx/). It has been created within the [BaSys](https://www.basys40.de/) project.

The DMP can be used to integrate data sources within Asset Administration Shells.
It utilizes [Apache Camel](https://camel.apache.org/) to enable multiple datasources, transformers and the usage of the AAS as a datasink.

To learn more on how to use and adapt the DMP component, visit the [documentation](https://wiki.basyx.org/en/latest/content/user_documentation/basyx_components/databridge/index.html) in the BaSyx Wiki or take a look at the [Device Integration Example](https://wiki.basyx.org/en/latest/content/user_documentation/developer/basyx_java_v1/knowledge_base/scenarios/device-integration.html).

## aas-camel-dmp

This repository is a fork of the Eclipse BaSyx DMP, renamed to **`aas-camel-dmp`**. It reworks the DMP into the **DMP** (DMP MQTT Platform): a per-asset integration container that

1. consumes data from industrial protocols (OPC UA, PLC, MQTT, Kafka, HTTP, …),
2. transforms incoming data with **Lua by default** in compliance with the **AIMC** (Asset Interfaces Mapping Configuration v2.0) transformation contract, or with JSONata when the transformation semantic ID starts with the private `https://example.org/jsonata/` namespace, and
3. writes transformed data to sinks (AAS submodel elements, MQTT topics, other protocols).

See [SPEC.md](SPEC.md) for the full design specification, milestones, and decision records.

### Key changes vs. upstream

- **Baseline:** Apache Camel **4.x** on **Java 17** (upstream was Camel 3.21 / Java 11).
- **`dmp.camel-lua`:** new Camel component (`lua:` scheme) that runs AIMC-compliant transformation blobs via **LuaJ** (`org.luaj:luaj-jse:3.0.1`). There is no `org.apache.camel:camel-lua` on Maven Central — this module *is* the Lua integration.
  - Whitelisted sandbox (base functions, `math`, `table`, `string`, `json`, `os.clock`; no `io`, `os` (except `clock`), `require`/`dofile`/`loadfile`/`load`, `debug`, `luajava`, `package`, `coroutine`).
  - CPU timeout via instruction-count hook (default 2 s per invocation).
  - Runtime-loaded blobs with mtime/sha-based reload; max blob size enforced at load.
  - `LuaHarness` CLI + contract tests to verify blobs before deployment.
- **Runtime config:** `<config>/luatransformer.json` (LUA equivalent of `jsonatatransformer.json`) and `<config>/queries/*.lua` blobs; `LUA_ENV_VAR_NAME` (`luatransformer`) mirroring the JSONata env-var mechanism.

### Provenance

- Fork origin: [eclipse-basyx/basyx-databridge](https://github.com/eclipse-basyx/basyx-databridge) (MIT).
- Derived files retain the original MIT copyright headers (`Copyright (C) … the Eclipse BaSyx Authors`, SPDX-License-Identifier: MIT); the project remains **MIT** overall (see `LICENSE`).
- AIMC/AID submodel template semantics: IDTA submodel templates (`admin-shell-io/submodel-templates`, CC BY 4.0).
- Submodel template models: `aas_pydantic` (`SebBehrendt/aas_pydantic`, MIT).
- This is a derivative work, not an official Eclipse/BaSyx component.
