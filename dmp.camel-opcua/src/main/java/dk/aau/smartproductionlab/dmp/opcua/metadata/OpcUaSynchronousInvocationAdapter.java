package dk.aau.smartproductionlab.dmp.opcua.metadata;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.opcua.component.DmpMiloMethodProducer;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.PayloadValidationException;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
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
		return endpointUri != null && endpointUri.startsWith("dmp-milo-client:");
	}

	/**
	 * The Method call as route steps: map the named request onto the server's
	 * input arguments, call through {@code dmp-milo-client:} (whose producer
	 * returns the Call result), then map its output arguments back by name.
	 */
	@Override
	public void configureInvoke(ProcessorDefinition<?> route, String endpointUri, InvocationSchema schema) {
		long timeoutMillis = timeoutSeconds(schema.connectorOptions().get("timeoutSeconds")) * 1000;
		route.process(exchange -> {
					@SuppressWarnings("unchecked")
					Map<String, Object> request = exchange.getMessage().getBody(Map.class);
					PreparedCall prepared = prepare(exchange.getContext(), endpointUri, request == null ? Map.of() : request, schema);
					exchange.setProperty(SIGNATURE_PROPERTY, prepared.signature());
					exchange.getMessage().setHeader(DmpMiloMethodProducer.CALL_TIMEOUT_HEADER, timeoutMillis);
					exchange.getMessage().setBody(prepared.arguments());
				})
				.to(endpointUri)
				.process(exchange -> exchange.getMessage().setBody(decode(exchange.getContext(),
						exchange.getMessage().getBody(), exchange.getProperty(SIGNATURE_PROPERTY,
								OpcUaMethodMetadataProvider.MethodSignature.class))));
	}

	private static final String SIGNATURE_PROPERTY = "dmp.opcua.methodSignature";

	/** A Method call ready to send: the server's signature and the positional input arguments. */
	record PreparedCall(OpcUaMethodMetadataProvider.MethodSignature signature, Variant[] arguments) { }

	/** Reads the Method's signature and maps the named request onto its input arguments. */
	PreparedCall prepare(CamelContext context, String endpointUri,
			Map<String, Object> namedRequest, InvocationSchema schema) {
		final OpcUaMethodMetadataProvider.MethodSignature signature;
		try {
			signature = metadata.read(context, endpointUri);
		} catch (RuntimeException error) {
			throw failure(ConnectorInvocationException.Category.TRANSPORT,
					"METHOD_METADATA_UNAVAILABLE", "Unable to read the configured OPC UA Method signature", error);
		}

		try {
			OpcUaMethodArguments.validateSchemaSignature(signature,
					schema.inputTypes(), schema.inputRequired(),
					schema.outputTypes(), schema.outputRequired());
		} catch (PayloadValidationException mismatch) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
					"METHOD_SIGNATURE_MISMATCH", mismatch.getMessage(),
					Map.of("violations", mismatch.getViolations()));
		}

		final Object arguments;
		try {
			List<Object> values = OpcUaMethodArguments.toPositional(namedRequest, signature,
					schema.inputTypes().keySet(), schema.inputRequired());
			arguments = metadata.toVariantArray(values, signature.inputs());
		} catch (PayloadValidationException mismatch) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
					"METHOD_ARGUMENT_MISMATCH", mismatch.getMessage(),
					Map.of("violations", mismatch.getViolations()));
		}

		return new PreparedCall(signature, (Variant[]) arguments);
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
