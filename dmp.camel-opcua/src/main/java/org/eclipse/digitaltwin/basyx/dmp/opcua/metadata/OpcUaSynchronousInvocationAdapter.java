package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.milo.MiloConstants;
import org.apache.camel.component.milo.client.MiloClientEndpoint;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadValidationException;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodResult;

import com.google.gson.Gson;

/** Performs the OPC UA-specific named-object to positional Method Call conversion. */
public final class OpcUaSynchronousInvocationAdapter implements SynchronousInvocationAdapter {
	private static final Gson GSON = new Gson();
	private final CamelOpcUaMethodMetadataProvider metadata;

	public OpcUaSynchronousInvocationAdapter() {
		this(new CamelOpcUaMethodMetadataProvider());
	}

	OpcUaSynchronousInvocationAdapter(CamelOpcUaMethodMetadataProvider metadata) {
		this.metadata = metadata;
	}

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && endpointUri.startsWith("milo-client:");
	}

	@Override
	public Map<String, Object> invoke(CamelContext context, String endpointUri,
			Map<String, Object> namedRequest, InvocationSchema schema) {
		final OpcUaMethodMetadataProvider.MethodSignature signature;
		try {
			signature = metadata.read(context, endpointUri);
		} catch (RuntimeException error) {
			throw failure(ConnectorInvocationException.Category.TRANSPORT,
					"METHOD_METADATA_UNAVAILABLE", "Unable to read the configured OPC UA Method signature", error);
		}

		boolean legacyJson = OpcUaMethodArguments.isLegacyJsonString(signature);
		try {
			OpcUaMethodArguments.validateSchemaSignature(signature,
					schema.inputTypes(), schema.inputRequired(),
					schema.outputTypes(), schema.outputRequired(), legacyJson);
		} catch (PayloadValidationException mismatch) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
					"METHOD_SIGNATURE_MISMATCH", mismatch.getMessage(),
					Map.of("violations", mismatch.getViolations()));
		}

		Object requestBody;
		if (legacyJson) {
			requestBody = JsonCodec.toJson(namedRequest);
		} else {
			try {
				List<Object> values = OpcUaMethodArguments.toPositional(namedRequest, signature,
						schema.inputTypes().keySet(), schema.inputRequired());
				requestBody = metadata.toVariantArray(values, signature.inputs());
			} catch (PayloadValidationException mismatch) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
						"METHOD_ARGUMENT_MISMATCH", mismatch.getMessage(),
						Map.of("violations", mismatch.getViolations()));
			}
		}

		try {
			long timeoutSeconds = timeoutSeconds(schema.connectorOptions().get("timeoutSeconds"));
			if (context.getEndpoint(endpointUri) instanceof MiloClientEndpoint endpoint) {
				var connection = endpoint.createConnection();
				try {
					var client = connection.getOpcUaClient();
					if (client == null) throw new IllegalStateException("OPC UA client is not connected");
					var node = ExpandedNodeId.parse(endpoint.getNode()).toNodeIdOrThrow(client.getNamespaceTable());
					var method = ExpandedNodeId.parse(endpoint.getMethod()).toNodeIdOrThrow(client.getNamespaceTable());
					Variant[] arguments = requestBody instanceof Variant[] variants ? variants
							: new Variant[] { new Variant(requestBody) };
					// Camel 4.22's SubscriptionManager.call discards the result. Use the same owned client, once.
					var response = (org.eclipse.milo.opcua.stack.core.types.structured.CallResponse) await(
							client.callAsync(List.of(new CallMethodRequest(node, method, arguments))), timeoutSeconds);
					if (response == null || response.getResults() == null || response.getResults().length != 1) {
						throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
								"METHOD_RESULT_INVALID", "OPC UA Method returned an unreadable result", Map.of());
					}
					return decode(context, response.getResults()[0], signature);
				} finally {
					endpoint.releaseConnection(connection);
				}
			}
			ProducerTemplate template = context.createProducerTemplate();
			try {
				return decode(context, await(template.asyncRequestBodyAndHeader(endpointUri, requestBody,
						MiloConstants.HEADER_AWAIT, true), timeoutSeconds), signature);
			} finally { template.stop(); }
		} catch (ConnectorInvocationException error) {
			throw error;
		} catch (RuntimeException | org.eclipse.milo.opcua.stack.core.UaException error) {
			throw failure(ConnectorInvocationException.Category.REMOTE,
					"METHOD_CALL_FAILED", "OPC UA Method invocation failed", error);
		}
	}

	private static Object await(Future<?> future, long timeoutSeconds) {
		try {
			return future.get(timeoutSeconds, TimeUnit.SECONDS);
		} catch (TimeoutException timeout) {
			future.cancel(true);
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"METHOD_CALL_TIMEOUT", "OPC UA Method invocation timed out",
					Map.of("timeoutSeconds", timeoutSeconds));
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
					"METHOD_CALL_INTERRUPTED", "OPC UA Method invocation was interrupted", Map.of());
		} catch (ExecutionException execution) {
			Throwable cause = execution.getCause();
			if (cause instanceof RuntimeException runtime) throw runtime;
			throw new IllegalStateException(cause);
		}
	}

	private static long timeoutSeconds(Object value) {
		if (value instanceof Number number && number.longValue() > 0) return number.longValue();
		try { return value == null ? 30 : Math.max(1, Long.parseLong(String.valueOf(value))); }
		catch (NumberFormatException ignored) { return 30; }
	}

	static Map<String, Object> decode(CamelContext context, Object raw,
			OpcUaMethodMetadataProvider.MethodSignature signature) {
		if (raw instanceof CallMethodResult call) {
			if (call.getStatusCode() == null || !call.getStatusCode().isGood()) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"METHOD_STATUS_BAD", "OPC UA Method returned a bad status", Map.of());
			}
			var output = call.getOutputArguments();
			raw = Map.of("statusCode", Map.of("good", true), "outputArguments", output == null ? List.of()
					: java.util.Arrays.stream(output).map(variant -> variant == null ? null : variant.getValue()).toList());
		}
		if (raw == null) return Map.of();
		Map<?, ?> result;
		try {
			result = context.getTypeConverter().tryConvertTo(Map.class, raw);
			if (result == null) result = GSON.fromJson(GSON.toJson(raw), Map.class);
		} catch (RuntimeException error) {
			throw failure(ConnectorInvocationException.Category.REMOTE,
					"METHOD_RESULT_INVALID", "OPC UA Method returned an unreadable result", error);
		}
		if (result == null) return Map.of();
		Object status = result.get("statusCode");
		if (status instanceof Map<?, ?> statusInfo && Boolean.FALSE.equals(statusInfo.get("good"))) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"METHOD_STATUS_BAD", "OPC UA Method returned a bad status",
					Map.of("status", String.valueOf(statusInfo.get("value"))));
		}
		Object output = result.get("outputArguments");
		if (!(output instanceof List<?> values)) {
			Object statuses = result.get("results");
			if (statuses instanceof List<?> statusValues && statusValues.stream().anyMatch(v -> !isGood(v))) {
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"METHOD_STATUS_BAD", "OPC UA Method returned a bad result status",
						Map.of("statuses", statusValues.stream().map(String::valueOf).toList()));
			}
			return Map.of();
		}
		try {
			return OpcUaMethodArguments.fromPositional(values, signature);
		} catch (PayloadValidationException mismatch) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"METHOD_RESULT_MISMATCH", mismatch.getMessage(),
					Map.of("violations", mismatch.getViolations()));
		}
	}

	private static boolean isGood(Object status) {
		if (status == null) return false;
		if ("Good".equalsIgnoreCase(String.valueOf(status))) return true;
		if (status instanceof Number number) return number.longValue() == 0;
		if (status instanceof Map<?, ?> map) {
			return Boolean.TRUE.equals(map.get("good"));
		}
		return false;
	}

	private static ConnectorInvocationException failure(ConnectorInvocationException.Category category,
			String code, String message, Exception cause) {
		return new ConnectorInvocationException(category, code, message,
				Map.of("cause", String.valueOf(cause.getMessage())));
	}
}
