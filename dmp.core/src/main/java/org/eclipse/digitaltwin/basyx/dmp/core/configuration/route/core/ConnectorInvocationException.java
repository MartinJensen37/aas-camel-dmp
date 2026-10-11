package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core;

import java.util.Map;

/** Protocol-neutral invocation outcome failure for a source adapter to translate. */
public final class ConnectorInvocationException extends RuntimeException {
	public enum Category { REMOTE, TIMEOUT, CONFIGURATION, TRANSPORT }

	private final Category category;
	private final String connectorCode;
	private final Map<String, Object> details;

	public ConnectorInvocationException(Category category, String connectorCode,
			String message, Map<String, Object> details) {
		super(message);
		this.category = category;
		this.connectorCode = connectorCode;
		this.details = Map.copyOf(details);
	}

	public Category getCategory() { return category; }
	public String getConnectorCode() { return connectorCode; }
	public Map<String, Object> getDetails() { return details; }
}
