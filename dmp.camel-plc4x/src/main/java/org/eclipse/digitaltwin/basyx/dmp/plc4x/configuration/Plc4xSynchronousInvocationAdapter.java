package org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;

/** Rejects action invocation until PLC4X exposes a native action binding. */
public final class Plc4xSynchronousInvocationAdapter implements SynchronousInvocationAdapter {
	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && endpointUri.startsWith("plc4x:");
	}

	@Override
	public Map<String, Object> invoke(CamelContext context, String endpointUri,
			Map<String, Object> namedRequest, InvocationSchema schema) {
		throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
				"PLC4X_ACTION_UNSUPPORTED",
				"PLC4X tag reads and writes are not a synchronous action invocation",
				Map.of("endpoint", endpointUri));
	}
}
