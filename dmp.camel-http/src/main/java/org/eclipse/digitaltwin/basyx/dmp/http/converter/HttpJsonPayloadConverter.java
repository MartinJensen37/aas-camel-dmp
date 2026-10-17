package org.eclipse.digitaltwin.basyx.dmp.http.converter;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;

/** Encodes decoded JSON-compatible values only when an HTTP producer needs a wire stream. */
public final class HttpJsonPayloadConverter {
	private HttpJsonPayloadConverter() { }

	public static InputStream mapToInputStream(Map<?, ?> value) {
		return jsonStream(value);
	}

	public static InputStream listToInputStream(List<?> value) {
		return jsonStream(value);
	}

	public static InputStream numberToInputStream(Number value) {
		return jsonStream(value);
	}

	public static InputStream booleanToInputStream(Boolean value) {
		return jsonStream(value);
	}

	public static InputStream jsonNullToInputStream(Object value) {
		if (value != JsonPointer.JSON_NULL) throw new IllegalArgumentException("not a JSON null marker");
		return new ByteArrayInputStream("null".getBytes(StandardCharsets.UTF_8));
	}

	public static InputStream jsonPayloadToInputStream(ConnectorJsonPayload value) {
		return jsonStream(value.value());
	}

	private static InputStream jsonStream(Object value) {
		return new ByteArrayInputStream(JsonCodec.toJson(value).getBytes(StandardCharsets.UTF_8));
	}
}
