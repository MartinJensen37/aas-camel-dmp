package org.eclipse.digitaltwin.basyx.dmp.paho.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

class MqttJsonPayloadConverterTest {
	@Test
	void convertsDecodedMapsToJsonOnlyAtMqttByteBoundary() throws Exception {
		try (var context = new DefaultCamelContext()) {
			byte[] payload = context.getTypeConverter().convertTo(byte[].class,
					Map.of("state", "READY", "count", 7));
			assertEquals(Map.of("state", "READY", "count", 7L),
					org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec.parseJson(
							new String(payload, StandardCharsets.UTF_8)));
		}
	}

	@Test
	void preservesTopLevelJsonNullOnTheMqttWire() throws Exception {
		try (var context = new DefaultCamelContext()) {
			byte[] payload = context.getTypeConverter().convertTo(byte[].class, JsonPointer.JSON_NULL);
			assertEquals("null", new String(payload, StandardCharsets.UTF_8));
		}
	}

	@Test
	void distinguishesJsonStringValuesFromOpaqueTextAtTheConnectorBoundary() throws Exception {
		try (var context = new DefaultCamelContext()) {
			byte[] jsonString = context.getTypeConverter().convertTo(byte[].class,
					new ConnectorJsonPayload("103.5585973"));
			assertEquals("\"103.5585973\"", new String(jsonString, StandardCharsets.UTF_8));
			byte[] opaqueText = context.getTypeConverter().convertTo(byte[].class, "103.5585973");
			assertEquals("103.5585973", new String(opaqueText, StandardCharsets.UTF_8));
		}
	}
}
