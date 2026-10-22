/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.apache.camel.component.http.HttpClientConfigurer;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;

/**
 * A form's static {@code htv_headers}, sent with every request of its endpoint. Header values may
 * carry secrets (API keys, bearer tokens), so they stay out of endpoint URIs: while the owning
 * bundle lives, an {@link HttpClientConfigurer} that adds them is bound in the Camel registry
 * under an opaque name, and the URI only names it ({@code httpClientConfigurer=#bean:<name>}),
 * as {@link dk.aau.smartproductionlab.dmp.core.security.BoundCredential} does for
 * credentials. The configurer also disables automatic retries: a write or an action is sent once.
 */
public final class StaticHttpHeaders implements ManagedRouteResource {
	private final String name = "dmp-http-headers-" + UUID.randomUUID();
	private final Map<String, String> headers;
	private CamelContext boundContext;
	private int owners;

	public StaticHttpHeaders(Map<String, String> headers) {
		this.headers = Map.copyOf(new LinkedHashMap<>(Objects.requireNonNull(headers, "headers")));
	}

	/** The camel-http option naming the bound configurer; not a secret. */
	public String clientConfigurerOption() { return "httpClientConfigurer=#bean:" + name; }

	/** The registry reference of the header map itself, for endpoints that take it directly. */
	public String headersReference() { return "#bean:" + name + ".headers"; }

	public Map<String, String> headers() { return headers; }

	@Override
	public synchronized void bind(CamelContext context) {
		if (boundContext != null) {
			if (boundContext != context) throw new IllegalStateException("headers are bound in another CamelContext");
			owners++;
			return;
		}
		context.getRegistry().bind(name, HttpClientConfigurer.class, new Configurer(headers));
		context.getRegistry().bind(name + ".headers", Map.class, headers);
		boundContext = context;
		owners = 1;
	}

	@Override
	public synchronized void close() {
		if (boundContext == null) return;
		if (--owners > 0) return;
		boundContext.getRegistry().unbind(name);
		boundContext.getRegistry().unbind(name + ".headers");
		boundContext = null;
	}

	@Override public String toString() { return "StaticHttpHeaders[" + name + ", " + headers.keySet() + ", <values redacted>]"; }

	private record Configurer(Map<String, String> headers) implements HttpClientConfigurer {
		@Override
		public void configureHttpClient(HttpClientBuilder builder) {
			builder.disableAutomaticRetries();
			builder.addRequestInterceptorLast((request, entity, context) -> headers.forEach(request::setHeader));
		}
	}
}
