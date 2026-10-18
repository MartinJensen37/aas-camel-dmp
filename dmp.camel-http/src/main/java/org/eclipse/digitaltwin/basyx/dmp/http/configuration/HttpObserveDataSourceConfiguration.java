/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.http.configuration;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.http.observe.DmpHttpObserveComponent;

/** An HTTP property observed, or an HTTP event subscribed, through a WoT {@code subprotocol} (longpoll, sse). */
public class HttpObserveDataSourceConfiguration extends DataSourceConfiguration
		implements org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource {
	/** The form's static htv_headers; bound in the registry while the bundle lives. */
	private StaticHttpHeaders headers;

	public void setHeaders(java.util.Map<String, String> headers) {
		this.headers = headers == null || headers.isEmpty() ? null : new StaticHttpHeaders(headers);
	}

	@Override
	public void bind(org.apache.camel.CamelContext context) {
		if (headers != null) headers.bind(context);
	}

	@Override
	public void close() {
		if (headers != null) headers.close();
	}

	private String subprotocol;
	private String contentType;
	private String authUsername;
	private String authPassword;

	public HttpObserveDataSourceConfiguration() {}

	public HttpObserveDataSourceConfiguration(String uniqueId, String serverUrl, int serverPort, String subprotocol,
			String contentType, String authUsername, String authPassword) {
		super(uniqueId, serverUrl, serverPort);
		this.subprotocol = subprotocol;
		this.contentType = contentType;
		this.authUsername = authUsername;
		this.authPassword = authPassword;
		setDelivery("async");
	}

	public String getSubprotocol() { return subprotocol; }

	@Override
	public String getConnectionURI() {
		String uri = DmpHttpObserveComponent.uri(subprotocol, getServerUrl(), contentType, authUsername, authPassword);
		return headers == null ? uri : uri + "&headers=" + headers.headersReference();
	}
}
