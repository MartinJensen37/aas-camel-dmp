package org.eclipse.digitaltwin.basyx.dmp.paho.converter;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;

/** JSON-encodes decoded value trees only when the Paho producer requests bytes. */
public final class MqttJsonPayloadConverter {
	private MqttJsonPayloadConverter() { }

	public static byte[] mapToBytes(Map<?, ?> value) {
		return JsonCodec.toJson(value).getBytes(StandardCharsets.UTF_8);
	}

	public static byte[] listToBytes(List<?> value) {
		return JsonCodec.toJson(value).getBytes(StandardCharsets.UTF_8);
	}

	public static byte[] numberToBytes(Number value) {
		return JsonCodec.toJson(value).getBytes(StandardCharsets.UTF_8);
	}

	public static byte[] booleanToBytes(Boolean value) {
		return JsonCodec.toJson(value).getBytes(StandardCharsets.UTF_8);
	}

	public static byte[] jsonNullToBytes(Object value) {
		if (value != JsonPointer.JSON_NULL) throw new IllegalArgumentException("not a JSON null marker");
		return "null".getBytes(StandardCharsets.UTF_8);
	}

	public static byte[] jsonPayloadToBytes(ConnectorJsonPayload value) {
		return JsonCodec.toJson(value.value()).getBytes(StandardCharsets.UTF_8);
	}
}
