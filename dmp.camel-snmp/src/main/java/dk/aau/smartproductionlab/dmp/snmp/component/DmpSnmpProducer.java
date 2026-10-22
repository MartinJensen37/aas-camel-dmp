/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.component;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;

/** A SET of the body's value; the agent's answer acknowledges it, and the body stays the written value. */
final class DmpSnmpProducer extends DefaultProducer {
	private final DmpSnmpEndpoint endpoint;

	DmpSnmpProducer(DmpSnmpEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) {
		endpoint.write(exchange.getMessage().getBody());
	}
}
