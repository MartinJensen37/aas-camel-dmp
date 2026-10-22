package dk.aau.smartproductionlab.dmp.http.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

public class HttpJsonPayloadConverterTest {
	@Test
	public void encodesDecodedMapsOnlyWhenAnHttpProducerRequestsAStream() throws Exception {
		try (var context = new DefaultCamelContext();
				InputStream stream = context.getTypeConverter().mandatoryConvertTo(InputStream.class,
						Map.of("state", "READY", "count", 7))) {
			Object decoded = dk.aau.smartproductionlab.dmp.core.util.JsonCodec.parseJson(
					new String(stream.readAllBytes(), StandardCharsets.UTF_8));
			assertEquals(Map.of("state", "READY", "count", 7L), decoded);
		}
	}

	@Test
	public void preservesTopLevelJsonNullOnTheHttpWire() throws Exception {
		try (var context = new DefaultCamelContext();
				InputStream stream = context.getTypeConverter().mandatoryConvertTo(InputStream.class, JsonPointer.JSON_NULL)) {
			assertEquals("null", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
		}
	}

	@Test
	public void serializesDeclaredJsonStringButKeepsOpaqueText() throws Exception {
		try (var context = new DefaultCamelContext();
				InputStream json = context.getTypeConverter().mandatoryConvertTo(InputStream.class,
						new ConnectorJsonPayload("READY"));
				InputStream opaque = context.getTypeConverter().mandatoryConvertTo(InputStream.class, "READY")) {
			assertEquals("\"READY\"", new String(json.readAllBytes(), StandardCharsets.UTF_8));
			assertEquals("READY", new String(opaque.readAllBytes(), StandardCharsets.UTF_8));
		}
	}
}
