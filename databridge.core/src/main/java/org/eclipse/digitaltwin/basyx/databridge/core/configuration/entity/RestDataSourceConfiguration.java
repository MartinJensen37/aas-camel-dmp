/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity;

/**
 * A REST operation-delegation datasource (M7).
 *
 * <p>
 * The REST endpoint of an executable Operation (e.g.
 * {@code POST /operations/syntegonStopperingSystemAAS/Halt}) is a source in
 * the DataBridge route model: the DMP exposes it via camel-rest (backed by
 * camel-jetty) on the consolidated REST surface and bridges the invocation to
 * the asset's native MQTT action.
 *
 * <p>
 * These sources are asset-dependent — the management node only emits them for
 * assets that declare the corresponding operation (bidirectional
 * request/response mapping) in their AIMC.
 *
 * @author aas-camel-dmp
 */
public class RestDataSourceConfiguration extends DataSourceConfiguration {

	private String restMethod;   // e.g. "POST"
	private String restPath;     // e.g. "/operations/syntegonStopperingSystemAAS/Halt"
	private String contentType;  // e.g. "application/json"

	public RestDataSourceConfiguration() {
	}

	public RestDataSourceConfiguration(String uniqueId, String restMethod, String restPath, String contentType) {
		super(uniqueId, null, 0);
		this.restMethod = restMethod;
		this.restPath = restPath;
		this.contentType = contentType;
	}

	public String getRestMethod() {
		return restMethod;
	}

	public void setRestMethod(String restMethod) {
		this.restMethod = restMethod;
	}

	public String getRestPath() {
		return restPath;
	}

	public void setRestPath(String restPath) {
		this.restPath = restPath;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * Descriptive URI of the REST source (not a {@code from()} endpoint —
	 * the camel-rest consumer is built from {@link #getRestMethod()} /
	 * {@link #getRestPath()} via the rest DSL).
	 */
	@Override
	public String getConnectionURI() {
		return "rest:" + (restMethod == null || restMethod.isBlank() ? "POST" : restMethod) + ":" + restPath;
	}
}
