/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.component;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;

/** One point per row; InfluxDB's answer acknowledges it, and the body stays the written row. */
final class DmpInfluxDb2Producer extends DefaultProducer {
	private final DmpInfluxDb2Endpoint endpoint;

	DmpInfluxDb2Producer(DmpInfluxDb2Endpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) {
		Object body = exchange.getMessage().getBody();
		if (body instanceof ConnectorJsonPayload payload) body = payload.value();
		endpoint.write(body);
	}
}
