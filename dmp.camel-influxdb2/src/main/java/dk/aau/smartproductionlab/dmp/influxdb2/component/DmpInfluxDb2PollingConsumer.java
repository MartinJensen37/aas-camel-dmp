/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.component;

import org.apache.camel.Exchange;
import org.apache.camel.support.PollingConsumerSupport;

/** One latest-row query per poll (e.g. {@code pollEnrich}); a failed or rejected query raises. */
final class DmpInfluxDb2PollingConsumer extends PollingConsumerSupport {
	private final DmpInfluxDb2Endpoint endpoint;

	DmpInfluxDb2PollingConsumer(DmpInfluxDb2Endpoint endpoint) {
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
