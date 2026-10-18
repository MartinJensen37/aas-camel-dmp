/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.http.configuration;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.http.observe.DmpHttpObserveComponent;

/** An HTTP property observed, or an HTTP event subscribed, through a WoT {@code subprotocol} (longpoll, sse). */
public class HttpObserveDataSourceConfiguration extends DataSourceConfiguration {
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
		return DmpHttpObserveComponent.uri(subprotocol, getServerUrl(), contentType, authUsername, authPassword);
	}
}
