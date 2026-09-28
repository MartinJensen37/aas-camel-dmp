package org.eclipse.digitaltwin.basyx.databridge.opcua.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.eclipse.digitaltwin.basyx.databridge.opcua.configuration.OpcuaConsumerConfiguration;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;

class OpcuaConsumerConfigurationContractTest {

	@Test
	void customDeserializerKeepsSourceContractAndEndpointOptions() {
		OpcuaConsumerConfiguration source = new Gson().fromJson("""
				{
				  "uniqueId": "map_pressure", "sourceId": "Pressure", "delivery": "sync",
				  "pollingIntervalMs": 2500, "valuePath": "$.value",
				  "sourceKind": "opcua-property", "mode": "poll",
				  "eventFilter": {"elementPath": "Pressure"},
				  "serverUrl": "device", "serverPort": 4840,
				  "pathToService": "opcua/server", "nodeInformation": "ns=2;s=Pressure",
				  "requestedPublishingInterval": 1500,
				  "parameters": {"requestedPublishingInterval": 3000, "keepAlive": "1500", "ignored": null}
				}
				""", OpcuaConsumerConfiguration.class);

		assertEquals("Pressure", source.getSourceId());
		assertEquals("sync", source.getDelivery());
		assertEquals(2500, source.getPollingIntervalMs());
		assertEquals("$.value", source.getValuePath());
		assertEquals("opcua-property", source.getSourceKind());
		assertEquals("poll", source.getMode());
		assertEquals(Map.of("elementPath", "Pressure"), source.getEventFilter());
		assertEquals("1500", source.getConfiguration().get("requestedPublishingInterval"));
		assertEquals("1500", source.getConfiguration().get("keepAlive"));
		assertEquals("ns=2;s=Pressure", source.getNodeInformation());
	}
}
