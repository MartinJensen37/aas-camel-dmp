/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import org.apache.camel.Producer;
import org.apache.camel.component.milo.client.MiloClientConnectionManager;
import org.apache.camel.component.milo.client.MiloClientEndpoint;

/**
 * A camel-milo client endpoint whose Method producer returns the Call's
 * {@code CallMethodResult} (status and output arguments) — camel-milo 4.22's
 * producer completes a Method call without its result — and whose consumer
 * observes a variable with a subscription of its own.
 */
public class DmpMiloClientEndpoint extends MiloClientEndpoint {
	DmpMiloClientEndpoint(String uri, DmpMiloClientComponent component, String endpointUri,
			MiloClientConnectionManager connectionManager) {
		super(uri, component, endpointUri, connectionManager);
	}

	@Override
	public Producer createProducer() throws Exception {
		return getMethod() == null ? super.createProducer() : new DmpMiloMethodProducer(this);
	}

	private EventSelection eventSelection;

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
