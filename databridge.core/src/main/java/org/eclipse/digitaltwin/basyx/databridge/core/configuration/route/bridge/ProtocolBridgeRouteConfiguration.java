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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.bridge;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;

/**
 * Interactive mapping compiled as a ProtocolBridge (ADR-020): an operation
 * delegation bridged to another protocol's command/reply pair. All
 * interaction specifics ride in {@code triggerData} — the DERIVED caller
 * path ({@code restPath}/{@code restMethod}, DMP v3: the descriptor derives
 * the REST surface by convention and the bridge emits the consumer from it),
 * {@code responseTransformer},
 * {@code timeoutSeconds} — exactly what the descriptor emits; no name
 * sniffing anywhere.
 */
public class ProtocolBridgeRouteConfiguration extends RouteConfiguration {

	public static final String ROUTE_TRIGGER = "bridge";
	public static final String REST_PATH = "restPath";
	public static final String REST_METHOD = "restMethod";
	/** The native command affordance's protocol (DMP v3): mqtt | http | opcua | plc4x. */
	public static final String PROTOCOL = "protocol";
	public static final String RESPONSE_TRANSFORMER = "responseTransformer";
	public static final String TIMEOUT_SECONDS = "timeoutSeconds";
	/** DECLARED MQTT 5 request/reply correlation (AID ``mqv_request_reply``). */
	public static final String REQUEST_REPLY = "requestReply";
	/** DECLARED MQTT 5 Response Topic (AID ``mqv_response_topic``, absolute). */
	public static final String RESPONSE_TOPIC = "responseTopic";
	/** DECLARED command parameter types (AID input DataSchema: name → JSON type). */
	public static final String PARAMETER_TYPES = "parameterTypes";
	/** DECLARED reply field types (AID output DataSchema: name → JSON type). */
	public static final String RESPONSE_TYPES = "responseTypes";
	/** DECLARED positional reply order (CCI operation output_variable order). */
	public static final String RESPONSE_ORDER = "responseOrder";
	public static final long DEFAULT_TIMEOUT_SECONDS = 30L;
	public static final String DEFAULT_REST_METHOD = "POST";
	public static final String DEFAULT_PROTOCOL = "mqtt";

	public ProtocolBridgeRouteConfiguration(RouteConfiguration configuration) {
		super(configuration);
	}

	@Override
	public String getRouteTrigger() {
		return ROUTE_TRIGGER;
	}

	public String getRestPath() {
		Object value = getTriggerData().get(REST_PATH);
		return value == null ? null : String.valueOf(value);
	}

	public String getRestMethod() {
		Object value = getTriggerData().get(REST_METHOD);
		return value == null ? DEFAULT_REST_METHOD : String.valueOf(value);
	}

	/** mqtt (default) uses the MQTT 5 InOut endpoint; synchronous protocols
	 * (http/opcua/plc4x) answer inline. */
	public String getProtocol() {
		Object value = getTriggerData().get(PROTOCOL);
		return value == null ? DEFAULT_PROTOCOL : String.valueOf(value);
	}

	public String getResponseTransformer() {
		return (String) getTriggerData().get(RESPONSE_TRANSFORMER);
	}

	public long getTimeoutSeconds() {
		Object value = getTriggerData().get(TIMEOUT_SECONDS);
		if (value == null) {
			return DEFAULT_TIMEOUT_SECONDS;
		}
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		return Long.parseLong(String.valueOf(value));
	}

	/** DECLARED MQTT 5 request/reply correlation (``mqv_request_reply``). */
	public boolean isRequestReply() {
		Object value = getTriggerData().get(REQUEST_REPLY);
		if (value instanceof Boolean b) {
			return b;
		}
		return value != null && Boolean.parseBoolean(String.valueOf(value));
	}

	/** DECLARED MQTT 5 Response Topic (absolute; may be null). */
	public String getResponseTopic() {
		Object value = getTriggerData().get(RESPONSE_TOPIC);
		return value == null ? null : String.valueOf(value);
	}

	/** DECLARED command parameter types (name → JSON-schema type); never null. */
	public java.util.Map<String, String> getParameterTypes() {
		return typeMap(PARAMETER_TYPES);
	}

	/** DECLARED reply field types (name → JSON-schema type); never null. */
	public java.util.Map<String, String> getResponseTypes() {
		return typeMap(RESPONSE_TYPES);
	}

	/** DECLARED positional reply order (the CCI operation's output_variable
	 * id_shorts, in order) — empty when not declared. */
	public java.util.List<String> getResponseOrder() {
		Object value = getTriggerData().get(RESPONSE_ORDER);
		if (!(value instanceof java.util.List<?> list)) {
			return java.util.List.of();
		}
		java.util.List<String> out = new java.util.ArrayList<>();
		for (Object entry : list) {
			if (entry != null) {
				out.add(String.valueOf(entry));
			}
		}
		return out;
	}

	private java.util.Map<String, String> typeMap(String key) {
		Object value = getTriggerData().get(key);
		java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
		if (value instanceof java.util.Map<?, ?> map) {
			for (java.util.Map.Entry<?, ?> e : map.entrySet()) {
				out.put(String.valueOf(e.getKey()),
						e.getValue() == null ? "string" : String.valueOf(e.getValue()));
			}
		}
		return out;
	}
}
