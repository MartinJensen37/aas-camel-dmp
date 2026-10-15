package org.eclipse.digitaltwin.basyx.dmp.paho.mqtt5.component;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

/** MQTT 5 request/reply invocation binding and reply normalization. */
public final class MqttSynchronousInvocationAdapter implements SynchronousInvocationAdapter {
	private static final String SINK_SCHEME = "paho-mqtt5:";
	/** A sink's bound credentials, referenced as two beans; the InOut endpoint takes the binding by name. */
	private static final java.util.regex.Pattern CREDENTIALS =
			java.util.regex.Pattern.compile("userName=#bean:([^&]+)\\.user&password=#bean:\\1\\.password");

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && endpointUri.startsWith(SINK_SCHEME);
	}

	@Override
	public java.util.Set<String> ownedEndpointUris(String endpointUri, InvocationSchema schema) {
		return java.util.Set.of(invocationUri(endpointUri, schema));
	}

	@Override
	public Map<String, Object> invoke(CamelContext context, String endpointUri,
			Map<String, Object> namedRequest, InvocationSchema schema) {
		String uri = invocationUri(endpointUri, schema);
		ProducerTemplate template = context.createProducerTemplate();
		try {
			Object body = template.requestBody(uri, namedRequest);
			if (body instanceof Map<?, ?> map) return stringMap(map);
			if (body == null) return Map.of();
			Object decoded = body instanceof String text ? JsonCodec.parseJson(text) : body;
			if (!(decoded instanceof Map<?, ?> map)) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"MQTT_REPLY_INVALID", "MQTT response is not a JSON object", Map.of());
			}
			return stringMap(map);
		} catch (ConnectorInvocationException e) {
			throw e;
		} catch (RuntimeException e) {
			throw translateFailure(e);
		} finally {
			template.stop();
		}
	}

	private static String invocationUri(String endpointUri, InvocationSchema schema) {
		Object requestReply = schema.connectorOptions().get("requestReply");
		Object topic = schema.connectorOptions().get("responseTopic");
		if (!Boolean.TRUE.equals(requestReply) || topic == null || String.valueOf(topic).isBlank()) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
					"MQTT_REQUEST_REPLY_NOT_DECLARED",
					"MQTT action form must declare request/reply and a response topic", Map.of());
		}
		long timeout = number(schema.connectorOptions().get("timeoutSeconds"), 30);
		String options = CREDENTIALS.matcher(endpointUri.substring(SINK_SCHEME.length())).replaceAll("credentials=$1");
		return "paho-mqtt5-inout:" + options + "&replyTopic=" + topic + "&replyTimeout=" + timeout + "&skipReplyStates=RUNNING";
	}

	static ConnectorInvocationException connectorFailure(Throwable failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof ConnectorInvocationException typed) return typed;
			if (cause.getCause() == cause) break;
		}
		return null;
	}

	static ConnectorInvocationException translateFailure(RuntimeException failure) {
		ConnectorInvocationException typed = connectorFailure(failure);
		if (typed != null) return typed;
		if (hasTimeoutCause(failure)) {
			return new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"MQTT_REQUEST_TIMEOUT", "Timed out awaiting correlated MQTT reply",
					Map.of());
		}
		return new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
				"MQTT_REQUEST_FAILED", "MQTT request/reply exchange failed",
				Map.of());
	}

	private static boolean hasTimeoutCause(Throwable failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof java.util.concurrent.TimeoutException
					|| cause instanceof org.apache.camel.ExchangeTimedOutException) return true;
			if (cause.getCause() == cause) break;
		}
		return false;
	}

	private static long number(Object value, long fallback) {
		if (value instanceof Number number) return number.longValue();
		try { return value == null ? fallback : Long.parseLong(String.valueOf(value)); }
		catch (NumberFormatException e) { return fallback; }
	}

	private static Map<String, Object> stringMap(Map<?, ?> value) {
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		value.forEach((key, item) -> result.put(String.valueOf(key), item));
		return result;
	}
}
