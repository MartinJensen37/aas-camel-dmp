/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import org.apache.camel.Producer;
import org.apache.camel.component.milo.client.MiloClientConnectionManager;
import org.apache.camel.component.milo.client.MiloClientEndpoint;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;

/**
 * A camel-milo client endpoint whose Method producer returns the Call's
 * {@code CallMethodResult} (status and output arguments) — camel-milo 4.22's
 * producer completes a Method call without its result —, whose Write producer
 * waits for the server and fails on a Bad status, and whose consumer observes
 * a variable or a notifier's events with a subscription of its own. Its node
 * is a NodeId ({@code node}) or an AID browse path ({@code browsePath}, see
 * {@link OpcUaBrowsePaths}), resolved once per client session.
 */
public class DmpMiloClientEndpoint extends MiloClientEndpoint {
	DmpMiloClientEndpoint(String uri, DmpMiloClientComponent component, String endpointUri,
			MiloClientConnectionManager connectionManager) {
		super(uri, component, endpointUri, connectionManager);
	}

	@Override
	public Producer createProducer() throws Exception {
		return getMethod() == null ? new DmpMiloWriteProducer(this) : new DmpMiloMethodProducer(this);
	}

	private EventSelection eventSelection;
	private String browsePath;
	private OpcUaClient resolvedFor;
	private NodeId resolvedNode;

	/** The AID browse path naming this endpoint's node, when it has no {@code node}. */
	String getBrowsePath() { return browsePath; }

	void setBrowsePath(String browsePath) {
		if (!OpcUaBrowsePaths.isBrowsePath(browsePath))
			throw new IllegalArgumentException("browsePath is an absolute path starting with /: " + browsePath);
		OpcUaBrowsePaths.relativePath(browsePath);
		this.browsePath = browsePath;
	}

	/** The endpoint's node on the server {@code client} is connected to: its NodeId, or its resolved browse path. */
	synchronized NodeId node(OpcUaClient client) throws UaException {
		if (browsePath == null) return OpcUaBrowsePaths.resolve(client, getNode());
		if (resolvedFor != client) {
			resolvedNode = OpcUaBrowsePaths.resolve(client, browsePath);
			resolvedFor = client;
		}
		return resolvedNode;
	}

	/** The selected fields and EventType when the endpoint's node is an event notifier; otherwise {@code null}. */
	EventSelection getEventSelection() { return eventSelection; }

	void setEventSelection(EventSelection eventSelection) { this.eventSelection = eventSelection; }

	/**
	 * An observed variable or an event notifier gets a subscription of its own
	 * (see {@link DmpMiloSubscriptionConsumer}).
	 */
	@Override
	public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) throws Exception {
		DmpMiloSubscriptionConsumer consumer = new DmpMiloSubscriptionConsumer(this, processor);
		configureConsumer(consumer);
		return consumer;
	}
}
