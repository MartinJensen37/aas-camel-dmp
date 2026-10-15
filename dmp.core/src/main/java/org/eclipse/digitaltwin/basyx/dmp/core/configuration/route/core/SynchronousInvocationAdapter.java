package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core;

import java.util.Map;
import java.util.Set;

import org.apache.camel.CamelContext;

/** Connector-neutral adapter for synchronous named request/reply endpoints. */
public interface SynchronousInvocationAdapter {
	boolean supports(String endpointUri);

	Map<String, Object> invoke(CamelContext context, String endpointUri,
			Map<String, Object> namedRequest, InvocationSchema schema);

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
