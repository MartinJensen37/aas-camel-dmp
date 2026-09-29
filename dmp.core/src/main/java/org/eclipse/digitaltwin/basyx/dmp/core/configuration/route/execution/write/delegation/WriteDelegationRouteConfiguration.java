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
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.write.delegation;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;

/**
 * Property write-delegation route (W2, Shape A): the caller PUTs the DMP's
 * derived surface, the device command leg writes through to the asset, and
 * after the device acknowledges the effective value is written back into the
 * AAS property. All specifics ride in {@code triggerData} — the derived
 * caller path ({@code restPath}/{@code restMethod}), the device
 * {@code protocol}, the {@code writebackSink} (AAS property endpoint id), the
 * optional {@code responseTransformer} (device reply → effective value) and
 * the MQTT 5 InOut reply facts ({@code requestReply}/{@code responseTopic}).
 */
public class WriteDelegationRouteConfiguration extends RouteConfiguration {

	public static final String ROUTE_TRIGGER = "write-delegation";
	public static final String REST_PATH = "restPath";
	public static final String REST_METHOD = "restMethod";
	/** The device command affordance's protocol: mqtt | http | opcua. */
	public static final String PROTOCOL = "protocol";
	public static final String WRITEBACK_SINK = "writebackSink";
	public static final String RESPONSE_TRANSFORMER = "responseTransformer";
	public static final String REQUEST_REPLY = "requestReply";
	public static final String RESPONSE_TOPIC = "responseTopic";
	public static final String PARAMETER_TYPES = "parameterTypes";
	public static final String RESPONSE_TYPES = "responseTypes";
	public static final String TIMEOUT_SECONDS = "timeoutSeconds";
	public static final long DEFAULT_TIMEOUT_SECONDS = 30L;
	public static final String DEFAULT_REST_METHOD = "PUT";
	public static final String DEFAULT_PROTOCOL = "opcua";

	public WriteDelegationRouteConfiguration(RouteConfiguration configuration) {
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

	public String getProtocol() {
		Object value = getTriggerData().get(PROTOCOL);
		return value == null ? DEFAULT_PROTOCOL : String.valueOf(value);
	}

	public String getWritebackSink() {
		return (String) getTriggerData().get(WRITEBACK_SINK);
	}

	public String getResponseTransformer() {
		return (String) getTriggerData().get(RESPONSE_TRANSFORMER);
	}

	public boolean isRequestReply() {
		Object value = getTriggerData().get(REQUEST_REPLY);
		return value instanceof Boolean b ? b : value != null && Boolean.parseBoolean(String.valueOf(value));
	}

	public String getResponseTopic() {
		Object value = getTriggerData().get(RESPONSE_TOPIC);
		return value == null ? null : String.valueOf(value);
	}

	public java.util.Map<String, String> getParameterTypes() {
		return typeMap(PARAMETER_TYPES);
	}

	public java.util.Map<String, String> getResponseTypes() {
		return typeMap(RESPONSE_TYPES);
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

	private java.util.Map<String, String> typeMap(String key) {
		Object value = getTriggerData().get(key);
		java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
		if (value instanceof java.util.Map<?, ?> map) {
			for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
				out.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
			}
		}
		return out;
	}
}
