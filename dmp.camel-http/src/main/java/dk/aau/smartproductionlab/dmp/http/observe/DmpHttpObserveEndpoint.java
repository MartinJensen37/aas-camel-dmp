/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.observe;

import java.net.URI;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultEndpoint;

/** One observed HTTP resource; consume-only. */
final class DmpHttpObserveEndpoint extends DefaultEndpoint {
	private final DmpHttpObserveComponent.Subprotocol subprotocol;
	private final URI url;
	private final String contentType;
	private final String username;
	private final String password;
	private final java.util.Map<String, String> headers;

	DmpHttpObserveEndpoint(String uri, DmpHttpObserveComponent component, DmpHttpObserveComponent.Subprotocol subprotocol,
			URI url, String contentType, String username, String password, java.util.Map<String, String> headers) {
		super(uri, component);
		this.subprotocol = subprotocol;
		this.url = url;
		this.contentType = contentType;
		this.username = username;
		this.password = password;
		this.headers = java.util.Map.copyOf(headers);
	}

	@Override public Producer createProducer() {
		throw new UnsupportedOperationException("dmp-http-observe endpoints only observe");
	}

	@Override public Consumer createConsumer(Processor processor) throws Exception {
		Consumer consumer = new DmpHttpObserveConsumer(this, processor);
		configureConsumer(consumer);
		return consumer;
	}

	DmpHttpObserveComponent.Subprotocol subprotocol() { return subprotocol; }
	URI url() { return url; }
	String contentType() { return contentType; }
	String username() { return username; }
	String password() { return password; }
	java.util.Map<String, String> headers() { return headers; }
}
