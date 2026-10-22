package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

import java.util.Map;
import java.util.Set;

import org.apache.camel.model.ProcessorDefinition;

/**
 * A connector's action invocation as route steps. The bridge runs them in a
 * command route on a copy of the operation exchange (Content Enricher), so
 * caller headers never reach the device and only the reply comes back.
 */
public interface SynchronousInvocationAdapter {
	boolean supports(String endpointUri);

	/**
	 * Appends the invocation to {@code route}: the body enters as the named
	 * request ({@code Map<String, Object>}) and leaves as the named reply
	 * ({@code Map<String, Object>}). A device, transport or timeout failure is
	 * raised as {@link ConnectorInvocationException}.
	 */
	void configureInvoke(ProcessorDefinition<?> route, String endpointUri, InvocationSchema schema);

	/** Pure preflight inventory, including connector-derived invocation endpoints. */
	default Set<String> ownedEndpointUris(String endpointUri, InvocationSchema schema) {
		return Set.of(endpointUri);
	}

	record InvocationSchema(Map<String, String> inputTypes, Set<String> inputRequired,
			Map<String, String> outputTypes, Set<String> outputRequired,
			Map<String, Object> connectorOptions) {
		public InvocationSchema(Map<String, String> inputTypes, Set<String> inputRequired,
				Map<String, String> outputTypes, Set<String> outputRequired) {
			this(inputTypes, inputRequired, outputTypes, outputRequired, Map.of());
		}

		public InvocationSchema {
			inputTypes = Map.copyOf(inputTypes);
			inputRequired = Set.copyOf(inputRequired);
			outputTypes = Map.copyOf(outputTypes);
			outputRequired = Set.copyOf(outputRequired);
			connectorOptions = Map.copyOf(connectorOptions);
		}
	}
}
