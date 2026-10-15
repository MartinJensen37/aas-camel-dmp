/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.aas;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.camel.Category;
import org.apache.camel.Consumer;
import org.apache.camel.PollingConsumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;
import org.eclipse.digitaltwin.basyx.dmp.aas.http.HTTPRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AAS component which can connect to Asset Administration Shells via a given
 * registry
 */
@UriEndpoint(firstVersion = "1.0.0-SNAPSHOT", scheme = "aas", title = "AAS", syntax = "aas:name", category = {
		Category.API })
public class AASEndpoint extends DefaultEndpoint {
	private static final Logger logger = LoggerFactory.getLogger(AASEndpoint.class);

	/** The only supported AAS API: AAS Part 2 HTTP/REST API v3. */
	public static final String DOT_AAS_V3 = "DotAAS-V3";
	private static final String API_V3_SUFFIX = "/$value";

	@UriPath
	@Metadata(required = true)
	private String name;

	@UriParam(defaultValue = "")
	private String propertyPath;

	/** Legacy selector; only the DotAAS-V3 API remains, so it is validated and otherwise ignored. */
	@UriParam(defaultValue = DOT_AAS_V3)
	private String api = DOT_AAS_V3;

	/**
	 * Optional JSON field to extract from the message body before writing
	 * (the AIMC SinkId out of the Lua blob output object).
	 */
	@UriParam(defaultValue = "")
	private String valuePath;

	/** Resolved AAS key type, used to select generic element reads. */
	@UriParam(defaultValue = "Property")
	private String elementType = "Property";

	private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
			new com.fasterxml.jackson.databind.ObjectMapper();

	public AASEndpoint() {
	}

	public AASEndpoint(String uri, AASComponent component) {
		super(uri, component);
	}

	@Override
	public Producer createProducer() throws Exception {
		return new AASProducer(this);
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		return null;
	}

	/**
	 * Sets the name
	 * 
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	/**
	 * The path to the property relative to the target AAS
	 */
	public String getPropertyPath() {
		return propertyPath;
	}

	public void setPropertyPath(String propertyPath) {
		this.propertyPath = propertyPath;
	}

	public String getElementType() {
		return elementType;
	}

	public void setElementType(String elementType) {
		this.elementType = elementType;
	}

	public String getApi() {
		return api;
	}

	public void setApi(String api) {
		requireSupportedApi(api);
		this.api = api;
	}

	/**
	 * Rejects descriptors that still select the removed BaSyx v1 API. Every
	 * historical spelling of the v3 API (DotAAS-V3, DotAasV3, DOT_AAS_V3) is accepted.
	 */
	public static void requireSupportedApi(String api) {
		if (api != null && !"DOTAASV3".equals(api.replaceAll("[^A-Za-z0-9]", "").toUpperCase(java.util.Locale.ROOT)))
			throw new IllegalArgumentException("AAS api '" + api + "' is not supported; only " + DOT_AAS_V3
					+ " (BaSyx v2 / AAS Part 2 API v3) remains. Remove the api field or set it to " + DOT_AAS_V3 + ".");
	}

	public void setPropertyValue(Object content) throws IOException {
		Object value = extractValuePath(content);
		if (value == org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.JSON_NULL) value = null;
		logger.debug("AAS write {} <- {} ({})", propertyPath, value,
				value == null ? "null" : value.getClass().getSimpleName());
		// Encode decoded values once at the DotAAS-V3 wire boundary.
		HTTPRequest.patchRequest(getFullProxyUrl() + API_V3_SUFFIX, MAPPER.writeValueAsString(value));

		logger.info("Transferred message={}", value);
	}

	/**
	 * When configured, extracts the SinkId field (valuePath) out of the JSON
	 * message body so each sink writes only its own value. The valuePath is a
	 * JSON Pointer (SPEC §6.5.4), so both a bare key ({@code "PackMLState"})
	 * and a nested pointer ({@code "$.SetPoint"}) work; absent → write the body
	 * as-is (coarse sink).
	 */
	private Object extractValuePath(Object content) throws IOException {
		if (valuePath == null || valuePath.isBlank() || content == null) {
			return content;
		}
		if (!(content instanceof Map<?, ?>) && !(content instanceof List<?>)) {
			throw new IllegalArgumentException("AAS valuePath requires a decoded object or array payload");
		}
		Object node = content;
		java.util.Optional<Object> field = org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.extract(node, valuePath);
		if (field.isEmpty()) {
			throw new IllegalArgumentException("AAS valuePath does not resolve in the decoded payload: " + valuePath);
		}
		Object value = field.get();
		if (value == org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.JSON_NULL) {
			// present-but-null leaf: write an explicit JSON null (T1.12)
			return null;
		}
		// collections are returned as the JAVA OBJECT so the DotAasV3 writer
		// can JSON-string-encode them properly (their toString()/naive wrap
		// is invalid JSON the server truncates — e.g. the occupation queue)
		return value;
	}

	public String getValuePath() {
		return valuePath;
	}

	public void setValuePath(String valuePath) {
		this.valuePath = valuePath;
	}

	private String createDotAasApiProxyUrl() {
		String proxyUrl = String.format("%s/submodel-elements/%s", getSubmodelEndpoint(), propertyPath);

		logger.info("Proxy URL: " + proxyUrl);

		return proxyUrl;
	}

	public String getSubmodelEndpoint() {
		String submodelEndpoint = this.getEndpointBaseUri().substring(4);

		// The endpoint URI is "aas:" + an ABSOLUTE http(s) URL. Camel normalises
		// the scheme-relative remainder, so the captured value can come back with
		// leading slashes ("/http://host" or "//http://host"). Left in place the
		// HTTP client rejects it with "ProtocolException: Target host is not
		// specified", so every read of a DotAAS-V3 source fails and the mapping
		// never receives a value. Only a genuine scheme marks the real start, so
		// strip the padding without touching a path-relative endpoint.
		while (submodelEndpoint.startsWith("/") && submodelEndpoint.contains("://")) {
			submodelEndpoint = submodelEndpoint.substring(1);
		}

		logger.info("SubmodelEndpoint " + submodelEndpoint);

		return submodelEndpoint;
	}

	public String getFullProxyUrl() {
		return createDotAasApiProxyUrl();
	}

	@Override
	public PollingConsumer createPollingConsumer() throws Exception {
		return new AASPollingConsumer(this);
	}
}
