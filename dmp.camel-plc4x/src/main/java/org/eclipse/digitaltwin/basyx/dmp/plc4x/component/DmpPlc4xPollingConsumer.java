/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.plc4x.component;

import org.apache.camel.Exchange;
import org.apache.camel.support.PollingConsumerSupport;

/** One fresh tag read per poll (e.g. {@code pollEnrich}); a failed or rejected read raises. */
final class DmpPlc4xPollingConsumer extends PollingConsumerSupport {
	private final DmpPlc4xEndpoint endpoint;

	DmpPlc4xPollingConsumer(DmpPlc4xEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public Exchange receive() { return receive(DmpPlc4xEndpoint.REQUEST_TIMEOUT_MS); }

	@Override
	public Exchange receiveNoWait() { return receive(DmpPlc4xEndpoint.REQUEST_TIMEOUT_MS); }

	@Override
	public Exchange receive(long timeout) {
		Exchange exchange = endpoint.createExchange();
		exchange.getMessage().setBody(endpoint.read());
		return exchange;
	}

	@Override protected void doStart() { }

	@Override protected void doStop() { }
}
