package dk.aau.smartproductionlab.dmp.opcua.metadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.camel.component.milo.MiloConstants;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousSourceReadAdapter;
import dk.aau.smartproductionlab.dmp.opcua.canonical.OpcUaCanonical;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;

/** Issues a UA Read service call instead of sampling monitored-item events. */
public final class OpcUaSynchronousSourceReadAdapter implements SynchronousSourceReadAdapter {
	/** The endpoint's node: a NodeId ({@code node}) or an AID browse path ({@code browsePath}), resolved by the producer. */
	private static final Pattern NODE_PARAMETER = Pattern.compile("(?:[?&])(?:node|browsePath)=RAW\\(([^)]*)\\)");

	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && (endpointUri.startsWith("dmp-milo-client:") || endpointUri.startsWith("milo-client:"));
	}

	@Override
	public void configureRead(ProcessorDefinition<?> route, String endpointUri) {
		String nodeId = nodeIdFromEndpoint(endpointUri);
		route.process(exchange -> readHeaders(nodeId).forEach(exchange.getMessage()::setHeader))
				.to(endpointUri)
				.process(exchange -> {
					exchange.getMessage().setBody(normalizeReadResult(awaitReadResult(exchange.getMessage().getBody())));
					// Already a decoded value: a string is not JSON text to parse.
					exchange.setProperty(dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter.NORMALIZED_VALUE_PROPERTY, true);
				});
	}

	static Map<String, Object> readHeaders(String nodeId) {
		return Map.of(MiloConstants.HEADER_NODE_IDS, List.of(nodeId), MiloConstants.HEADER_AWAIT, true);
	}

	static Object awaitReadResult(Object result) {
		if (!(result instanceof CompletionStage<?> stage)) return result;
		try {
			return stage.toCompletableFuture().join();
		} catch (CompletionException e) {
			throw new IllegalStateException("OPC UA Read failed", e.getCause() == null ? e : e.getCause());
		}
	}

	static String nodeIdFromEndpoint(String endpointUri) {
		Matcher matcher = NODE_PARAMETER.matcher(endpointUri);
		if (!matcher.find()) {
			throw new IllegalArgumentException("OPC UA Read endpoint must declare a node parameter");
		}
		return matcher.group(1);
	}

	/** The read variable's value as JSON (an MQTT or HTTP source delivers its value the same way). */
	static Object normalizeReadResult(Object result) {
		if (!(result instanceof List<?> values) || values.size() != 1
				|| !(values.get(0) instanceof DataValue dataValue)) {
			throw new IllegalStateException("OPC UA Read returned no single DataValue");
		}
		if (dataValue.getStatusCode() == null || !dataValue.getStatusCode().isGood()
				|| dataValue.getValue() == null) {
			throw new IllegalStateException("OPC UA Read returned an absent or bad-status value");
		}
		Object value = OpcUaCanonical.jsonValue(dataValue.getValue());
		return value == null ? dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL : value;
	}
}
