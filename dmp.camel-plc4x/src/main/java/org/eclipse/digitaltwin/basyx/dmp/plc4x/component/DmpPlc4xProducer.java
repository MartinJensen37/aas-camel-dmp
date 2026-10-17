/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.plc4x.component;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;

/**
 * An acknowledged tag write. The body is the value of a single-tag endpoint or
 * a value per tag; a request/reply exchange gets an empty reply (a tag write
 * has no output, e.g. when it starts an action).
 */
final class DmpPlc4xProducer extends DefaultProducer {
	private final DmpPlc4xEndpoint endpoint;

	DmpPlc4xProducer(DmpPlc4xEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) {
		endpoint.write(endpoint.valuesByTag(exchange.getMessage().getBody()));
		if (exchange.getPattern().isOutCapable()) exchange.getMessage().setBody(Map.of());
	}
}
