package org.eclipse.digitaltwin.basyx.dmp.paho.mqtt5.component;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

/** MQTT 5 invocation binding: request/reply with reply normalization, or a one-way invoke-only publish. */
public final class MqttSynchronousInvocationAdapter implements SynchronousInvocationAdapter {
	private static final String SINK_SCHEME = "paho-mqtt5:";
	/** A request/reply sink the compiler already built (MqttV5OperationDataSinkConfiguration). */
	private static final String INOUT_SCHEME = "paho-mqtt5-inout:";
	/** A sink's bound credentials, referenced as two beans; the InOut endpoint takes the binding by name. */
	private static final java.util.regex.Pattern CREDENTIALS =
			java.util.regex.Pattern.compile("userName=#bean:([^&]+)\\.user&password=#bean:\\1\\.password");

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && (endpointUri.startsWith(SINK_SCHEME) || endpointUri.startsWith(INOUT_SCHEME));
	}

	@Override
	public java.util.Set<String> ownedEndpointUris(String endpointUri, InvocationSchema schema) {
		return java.util.Set.of(invocationUri(endpointUri, schema));
	}

	@Override
	public void configureInvoke(ProcessorDefinition<?> route, String endpointUri, InvocationSchema schema) {
		if (invokeOnly(endpointUri, schema)) {
			// No reply topic: the command is published one way; the broker's acknowledgement
			// (QoS 1/2) ends the call, and the action has no output.
			route.doTry()
					.to(ExchangePattern.InOnly, endpointUri)
					.process(exchange -> exchange.getMessage().setBody(new java.util.LinkedHashMap<String, Object>()))
				.doCatch(Exception.class)
					.process(exchange -> {
						throw translateFailure(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class));
					})
				.end();
			return;
		}
		route.doTry()
				.to(ExchangePattern.InOut, invocationUri(endpointUri, schema))
				.process(exchange -> exchange.getMessage().setBody(reply(exchange.getMessage().getBody())))
			.doCatch(Exception.class)
				.process(exchange -> {
					throw translateFailure(exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class));
				})
			.end();
	}

	/** The correlated reply: a JSON object, or nothing. */
	static Map<String, Object> reply(Object body) {
		if (body instanceof Map<?, ?> map) return stringMap(map);
		if (body == null) return Map.of();
		Object decoded = body instanceof String text ? JsonCodec.parseJson(text)
				: body instanceof byte[] bytes ? JsonCodec.parseJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)) : body;
		if (!(decoded instanceof Map<?, ?> map)) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"MQTT_REPLY_INVALID", "MQTT response is not a JSON object", Map.of());
		}
		return stringMap(map);
	}

	/** An action the compiler bound without a reply topic (requestReply=false). */
	private static boolean invokeOnly(String endpointUri, InvocationSchema schema) {
		return endpointUri.startsWith(INOUT_SCHEME) && Boolean.FALSE.equals(schema.connectorOptions().get("requestReply"));
	}

	private static String invocationUri(String endpointUri, InvocationSchema schema) {
		if (endpointUri.startsWith(INOUT_SCHEME)) return endpointUri;
		Object requestReply = schema.connectorOptions().get("requestReply");
		Object topic = schema.connectorOptions().get("responseTopic");
		if (!Boolean.TRUE.equals(requestReply) || topic == null || String.valueOf(topic).isBlank()) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
					"MQTT_REQUEST_REPLY_NOT_DECLARED",
					"MQTT action form must declare request/reply and a response topic", Map.of());
		}
		long timeout = number(schema.connectorOptions().get("timeoutSeconds"), 30);
		String options = CREDENTIALS.matcher(endpointUri.substring(SINK_SCHEME.length())).replaceAll("credentials=$1");
		// The first reply carrying the request's correlation data is the terminal reply.
		return "paho-mqtt5-inout:" + options + "&replyTopic=" + topic + "&replyTimeout=" + timeout;
	}

	static ConnectorInvocationException connectorFailure(Throwable failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof ConnectorInvocationException typed) return typed;
			if (cause.getCause() == cause) break;
		}
		return null;
	}

	static ConnectorInvocationException translateFailure(Throwable failure) {
		if (failure == null) failure = new IllegalStateException("MQTT request/reply failed without a cause");
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
