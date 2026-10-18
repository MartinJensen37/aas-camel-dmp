/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.snmp.component;

import org.apache.camel.Exchange;
import org.apache.camel.support.PollingConsumerSupport;

/** One GET per poll (e.g. {@code pollEnrich}); a failed or rejected GET raises. */
final class DmpSnmpPollingConsumer extends PollingConsumerSupport {
	private final DmpSnmpEndpoint endpoint;

	DmpSnmpPollingConsumer(DmpSnmpEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override public Exchange receive() { return receive(0); }

	@Override public Exchange receiveNoWait() { return receive(0); }

	@Override public Exchange receive(long timeout) {
		Exchange exchange = endpoint.createExchange();
		exchange.getMessage().setBody(endpoint.read());
		return exchange;
	}

	@Override protected void doStart() { }

	@Override protected void doStop() { }
}
