# M4 — Interface transforms

Decorate a protocol-only resource with another interface (SPEC §10). The
canonical payload form is normalized **at the protocol boundary** by the
protocol adapters; the Lua blob stays a protocol-agnostic JSON→JSON function.

This example implements the SPEC §10.2 pattern: an **OPC UA-only** asset
gains an **MQTT** interface — bidirectional.

```
OPC UA --(state)--> [opc2mqtt.lua] --> MQTT  (aas/<asset>/State)
MQTT   --(cmd)----> [mqtt2opc.lua] --> OPC UA (writes a node)
```

## Canonical OPC UA payload (SPEC §10.6)

The `dmp.camel-opcua` module ships the protocol adapters:

| Direction | Adapter | Behaviour |
|---|---|---|
| OPC UA → Lua | `OpcUaCanonicalConverter` | camel-milo puts a Milo `DataValue` on the body; a registered Camel type converter turns it into canonical JSON automatically when the Lua component reads the body (no route step needed) |
| Lua → OPC UA | `OpcUaNormalizer` | reads the canonical blob output (`{ "value": … }`), extracts `.value`, and sets a Milo `Variant` body that the milo-client producer writes |

Canonical OPC UA value handed to the blob (SPEC §10.6):

```json
{ "nodeId": "ns=2;s=State", "value": 42.5, "status": 0, "timestamp": "2026-01-02T03:04:05Z" }
```

- `value` — the unboxed, JSON-friendly value (number/string/boolean; binary → base64)
- `status` — OPC UA status code (long)
- `timestamp` — source time as ISO-8601 (falls back to server time)
- `nodeId` — present when the exchange carries a `CamelMiloNodeId` header

## Routes (camel-milo 4.22)

Monitored items are enabled with `monitorFilterType=dataChangeFilter`
(camel-milo 4.22 replaced the old `monitor=true` option).

```java
// OPC UA -> MQTT (state)
from("milo-client:opc.tcp://<asset>:4840/milo?node=RAW(ns=2;s=State)&monitorFilterType=dataChangeFilter&requestedPublishingInterval=1000")
    .to("lua:file:/etc/dmp/opc2mqtt.lua?timeoutSeconds=2")
    .to("paho:aas/<asset>/State?brokerUrl=tcp://<broker>:1883");

// MQTT -> OPC UA (commands)
from("paho:aas/<asset>/cmd?brokerUrl=tcp://<broker>:1883")
    .to("lua:file:/etc/dmp/mqtt2opc.lua?timeoutSeconds=2")
    .process(new OpcUaNormalizer())                       // canonical -> Milo Variant
    .to("milo-client:opc.tcp://<asset>:4840/milo?node=RAW(ns=2;s=State)");
```

The same routes expressed as DMP config files are in `config/` (the DMP routes
are JSON-driven; the `OpcUaNormalizer` step consumes the decoded Lua value
tree and turns the selected value into a Milo Variant).

## Blobs

- `config/opc2mqtt.lua` — maps the canonical OPC UA value to a compact MQTT
  state payload.
- `config/mqtt2opc.lua` — maps an MQTT command payload to the canonical write
  object the normalizer feeds to the OPC UA node.

## Files

```
config/opcuadatasource.json.example   OPC UA datasource (monitored item)
config/mqttdatasink.json.example    MQTT state sink
config/mqttdatasource.json.example  MQTT command source
config/luatransformer.json.example the two Lua transformers
config/routes.json.example          the two routes
config/opc2mqtt.lua                 AIMC blob: OPC UA -> MQTT
config/mqtt2opc.lua                 AIMC blob: MQTT -> OPC UA
RouteExample.java                   equivalent raw route DSL
```

Verify any blob quickly with the Lua harness (M1):

```
java -cp <camel-lua classpath> org.eclipse.digitaltwin.basyx.dmp.lua.harness.LuaHarness \
    config/opc2mqtt.lua sources.json
```
