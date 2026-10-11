package org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.http.HttpClientConfigurer;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

/** HTTP InOut binding for a WoT form's declared methodName and JSON payload. */
public final class HttpSynchronousInvocationAdapter implements SynchronousInvocationAdapter {

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && (endpointUri.startsWith("http://")
				|| endpointUri.startsWith("https://"));
	}

	@Override
	public Map<String, Object> invoke(CamelContext context, String endpointUri,
			Map<String, Object> namedRequest, InvocationSchema schema) {
		Map<String, Object> headers = new LinkedHashMap<>();
		Object method = schema.connectorOptions().get("httpMethod");
		headers.put(Exchange.HTTP_METHOD, method == null ? "POST" : String.valueOf(method));
		headers.put(Exchange.CONTENT_TYPE, "application/json");
		headers.put("Accept", "application/json");
		ProducerTemplate template = context.createProducerTemplate();
		try {
			long timeoutSeconds = number(schema.connectorOptions().get("timeoutSeconds"), 30);
			// Isolate the action client's no-retry policy from ordinary HTTP endpoints.
			String invokeUri = endpointUri + (endpointUri.contains("?") ? "&" : "?")
					+ "httpClientConfigurer=#class:" + NoRetryHttpClientConfigurer.class.getName();
			Future<Object> future = template.asyncRequestBodyAndHeaders(
					invokeUri, JsonCodec.toJson(namedRequest), headers);
			Object reply = await(future, timeoutSeconds);
			if (reply instanceof Map<?, ?> map) return stringMap(map);
			String text = reply == null ? null : context.getTypeConverter().convertTo(String.class, reply);
			if (text == null || text.isBlank()) return Map.of();
			Object decoded = JsonCodec.parseJson(text);
			if (!(decoded instanceof Map<?, ?> resultMap)) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"HTTP_REPLY_INVALID", "HTTP action reply is not a JSON object", Map.of());
			}
			return stringMap(resultMap);
		} catch (ConnectorInvocationException e) {
			throw e;
		} catch (RuntimeException e) {
			HttpOperationFailedException failedResponse = findHttpFailure(e);
			if (failedResponse != null) {
				Map<String, Object> details = new LinkedHashMap<>();
				details.put("status", failedResponse.getStatusCode());
				details.put("statusText", String.valueOf(failedResponse.getStatusText()));
				Object deviceError = safeErrorBody(failedResponse.getResponseBody());
				if (deviceError != null) details.put("deviceError", deviceError);
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"HTTP_" + failedResponse.getStatusCode(),
						"HTTP action endpoint returned an error", details);
			}
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
					"HTTP_INVOKE_FAILED", "HTTP action invocation failed: " + causalMessage(e),
					Map.of("cause", causalMessage(e)));
		} finally {
			template.stop();
		}
	}

	public static final class NoRetryHttpClientConfigurer implements HttpClientConfigurer {
		@Override
		public void configureHttpClient(HttpClientBuilder clientBuilder) {
			clientBuilder.disableAutomaticRetries();
		}
	}

	private static Object await(Future<Object> future, long timeoutSeconds) {
		try {
			return future.get(timeoutSeconds, TimeUnit.SECONDS);
		} catch (TimeoutException timeout) {
			future.cancel(true);
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"HTTP_INVOKE_TIMEOUT", "HTTP action endpoint timed out", Map.of("timeoutSeconds", timeoutSeconds));
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
					"HTTP_INVOKE_INTERRUPTED", "HTTP action invocation was interrupted", Map.of());
		} catch (ExecutionException execution) {
			Throwable cause = execution.getCause();
			if (cause instanceof RuntimeException runtime) throw runtime;
			throw new IllegalStateException(cause);
		}
	}

private static long number(Object value, long fallback) {
		if (value instanceof Number number) return number.longValue();
		try { return value == null ? fallback : Long.parseLong(String.valueOf(value)); }
		catch (NumberFormatException ignored) { return fallback; }
	}

	private static String causalMessage(Throwable error) {
		StringBuilder text = new StringBuilder();
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (!text.isEmpty()) text.append(" <- ");
			text.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage());
		}
		return text.toString();
	}

	private static HttpOperationFailedException findHttpFailure(Throwable error) {
		for (Throwable current = error; current != null; current = current.getCause()) {
			if (current instanceof HttpOperationFailedException failure) return failure;
		}
		return null;
	}

	private static Object safeErrorBody(String body) {
		if (body == null || body.isBlank()) return null;
		try {
			Object decoded = JsonCodec.parseJson(body);
			if (decoded instanceof Map<?, ?> map) {
				Map<String, Object> safe = new LinkedHashMap<>();
				for (String key : java.util.List.of("code", "error", "message")) {
					if (map.containsKey(key)) safe.put(key, map.get(key));
				}
				return safe.isEmpty() ? null : safe;
			}
		} catch (RuntimeException ignored) {
			// Do not echo arbitrary device response bodies to the caller.
		}
		return null;
	}

	private static Map<String, Object> stringMap(Map<?, ?> map) {
		Map<String, Object> result = new LinkedHashMap<>();
		map.forEach((key, value) -> result.put(String.valueOf(key), value));
		return result;
	}
}
