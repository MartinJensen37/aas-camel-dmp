/*******************************************************************************
 * M4 example (SPEC §10.2): decorate an OPC UA-only resource with an MQTT
 * interface — bidirectional, two routes, two AIMC Lua blobs.
 *
 * This is the raw Camel route DSL equivalent of the JSON-driven DMP routes in
 * config/routes.json.example. It shows where the canonical adapter classes of
 * databridge.camel-opcua slot in.
 *
 *   databridge.camel-opcua ... canonical.OpcUaCanonicalConverter
 *       OPC UA -> Lua: auto-invoked when the Lua component reads the body
 *       (DataValue -> canonical JSON); no route step required.
 *
 *   databridge.camel-opcua ... canonical.OpcUaNormalizer
 *       Lua -> OPC UA: canonical blob output { "value": ... } -> Milo Variant.
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.databridge.interfaceexample;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.databridge.opcua.canonical.OpcUaNormalizer;

public class InterfaceTransformExampleRoutes extends RouteBuilder {

    /** OPC UA node that publishes the machine state. */
    private static final String OPC_STATE =
            "milo-client:opc.tcp://<asset>:4840/milo?node=RAW(ns=2;s=State)"
            + "&monitorFilterType=dataChangeFilter&requestedPublishingInterval=1000";

    /** OPC UA node that accepts commands. */
    private static final String OPC_CMD =
            "milo-client:opc.tcp://<asset>:4840/milo?node=RAW(ns=2;s=Command)";

    private static final String MQTT_STATE = "paho:aas/<asset>/State?brokerUrl=tcp://<broker>:1883";
    private static final String MQTT_CMD = "paho:aas/<asset>/cmd?brokerUrl=tcp://<broker>:1883";

    @Override
    public void configure() {
        // OPC UA -> MQTT (state): the DataValue body is normalized to canonical
        // JSON by the registered OpcUaCanonicalConverter when lua: reads it.
        from(OPC_STATE)
                .to("lua:file:/etc/dmp/opc2mqtt.lua?inputType=JsonString&outputType=JsonString&timeoutSeconds=2")
                .to(MQTT_STATE);

        // MQTT -> OPC UA (commands): the blob emits { "nodeId": ..., "value": ... };
        // OpcUaNormalizer turns .value into the Milo Variant the producer writes.
        from(MQTT_CMD)
                .to("lua:file:/etc/dmp/mqtt2opc.lua?inputType=JsonString&outputType=JsonString&timeoutSeconds=2")
                .process(new OpcUaNormalizer())
                .to(OPC_CMD);
    }
}
