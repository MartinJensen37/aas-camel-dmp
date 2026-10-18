/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.http.observe;

import java.net.URI;
import java.util.Locale;
import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;

/**
 * {@code dmp-http-observe:<subprotocol>?url=RAW(…)}: observes an HTTP resource as
 * the WoT HTTP binding's {@code subprotocol} describes it, {@code longpoll} (a GET
 * the server answers on the next change, repeated) or {@code sse} (an
 * {@code text/event-stream} whose events each carry one value). camel-http has no
 * consumer, so the DMP ships this one (SPEC §9).
 */
public final class DmpHttpObserveComponent extends DefaultComponent {
	public static final String SCHEME = "dmp-http-observe";

	public enum Subprotocol { LONGPOLL, SSE }

	/** The endpoint URI; values are passed RAW so Camel leaves them unchanged. */
	public static String uri(String subprotocol, String url, String contentType, String username, String password) {
		StringBuilder uri = new StringBuilder(SCHEME).append(':').append(subprotocol.toLowerCase(Locale.ROOT))
				.append("?url=RAW(").append(url).append(')');
		if (contentType != null) uri.append("&contentType=RAW(").append(contentType).append(')');
		if (username != null && password != null)
			uri.append("&authUsername=RAW(").append(username).append(")&authPassword=RAW(").append(password).append(')');
		return uri.toString();
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Subprotocol subprotocol;
		try { subprotocol = Subprotocol.valueOf(remaining.toUpperCase(Locale.ROOT)); }
		catch (IllegalArgumentException unknown) { throw new IllegalArgumentException("dmp-http-observe supports longpoll and sse, not " + remaining); }
		String url = getAndRemoveParameter(parameters, "url", String.class);
		if (url == null || url.isBlank()) throw new IllegalArgumentException("dmp-http-observe endpoint requires a url");
		URI target = URI.create(url);
		if (!"http".equalsIgnoreCase(target.getScheme()) && !"https".equalsIgnoreCase(target.getScheme()))
			throw new IllegalArgumentException("dmp-http-observe requires an http(s) url");
		String contentType = getAndRemoveParameter(parameters, "contentType", String.class, "application/json");
		String username = getAndRemoveParameter(parameters, "authUsername", String.class);
		String password = getAndRemoveParameter(parameters, "authPassword", String.class);
		return new DmpHttpObserveEndpoint(uri, this, subprotocol, target, contentType, username, password);
	}
}
