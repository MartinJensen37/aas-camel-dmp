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
package dk.aau.smartproductionlab.dmp.aas;

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
import dk.aau.smartproductionlab.dmp.aas.http.HTTPRequest;
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
	private static final String API_V3_SUFFIX = "/$value";

	@UriPath
	@Metadata(required = true)
	private String name;

	@UriParam(defaultValue = "")
	private String propertyPath;

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

	public void setPropertyValue(Object content) throws IOException {
		Object value = extractValuePath(content);
		if (value == dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL) value = null;
		logger.debug("AAS write {} <- {} ({})", propertyPath, value,
				value == null ? "null" : value.getClass().getSimpleName());
		// Encode decoded values once at the DotAAS-V3 wire boundary.
		long started = System.nanoTime();
		try {
			HTTPRequest.patchRequest(getFullProxyUrl() + API_V3_SUFFIX, MAPPER.writeValueAsString(wireValue(value)));
		} catch (IOException | RuntimeException failure) {
			AasAudit.write(this, value, started, failure);
			throw failure;
		}
		AasAudit.write(this, value, started, null);

		logger.info("Transferred message={}", value);
	}

	/**
	 * A Property's {@code $value} is its lexical form as a JSON string: BaSyx
	 * rejects a JSON number or boolean ("not as defined in the Dot AAS Part 2").
	 * Numbers are written without exponent, so the text is valid for xs:decimal
	 * and xs:double alike, and an integral number fits the integer types.
	 */
	private Object wireValue(Object value) {
		if (!"Property".equalsIgnoreCase(elementType)) return value;
		if (value instanceof Boolean) return value.toString();
		if (value instanceof Number number) {
			double asDouble = number.doubleValue();
			if (Double.isNaN(asDouble)) return "NaN";
			if (Double.isInfinite(asDouble)) return asDouble > 0 ? "INF" : "-INF";
			return new java.math.BigDecimal(number.toString()).stripTrailingZeros().toPlainString();
		}
		return value;
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
		java.util.Optional<Object> field = dk.aau.smartproductionlab.dmp.core.util.JsonPointer.extract(node, valuePath);
		if (field.isEmpty()) {
			throw new IllegalArgumentException("AAS valuePath does not resolve in the decoded payload: " + valuePath);
		}
		Object value = field.get();
		if (value == dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL) {
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
