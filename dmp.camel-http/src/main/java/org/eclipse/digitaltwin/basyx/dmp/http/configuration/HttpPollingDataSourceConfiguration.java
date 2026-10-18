/*******************************************************************************
 * Copyright (C) 2022 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.http.configuration;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;

/**
 * Configuration of an HTTP source polled with GET requests
 * @author n14s - Niklas Mertens
 *
 */
public class HttpPollingDataSourceConfiguration extends DataSourceConfiguration
		implements org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource {
	public HttpPollingDataSourceConfiguration() {}
	private	String authUsername;
	private String authPassword;
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
	
	public HttpPollingDataSourceConfiguration(String uniqueId, String serverUrl, int serverPort, String authUsername, String authPassword) {
		super(uniqueId, serverUrl, serverPort);
		this.authUsername = authUsername;
		this.authPassword = authPassword;
	}

	@Override
	public String getConnectionURI() {
		String uri = getServerUrl();
		if(!(authUsername == null || authPassword == null)) {
			uri = uri + (uri.contains("?")? "&" : "?") + "authUsername=" + authUsername + "&authPassword=" + authPassword+"&authMethod=Basic&authenticationPreemptive=true";
		}
		if (headers != null) uri = uri + (uri.contains("?") ? "&" : "?") + headers.clientConfigurerOption();
		return uri;
	}
}
