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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.operation;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;

/**
 * A single REST operation-delegation route (M7).
 *
 * <p>
 * The route reuses the standard DataBridge source/transformer/sink model:
 *
 * <ul>
 * <li><b>datasource</b> — the REST operation-delegation endpoint (a
 * {@code RestDataSourceConfiguration}, exposed via camel-rest);</li>
 * <li><b>transformers[0]</b> — the request-direction Lua blob
 * (REST operation variables → native MQTT command message);</li>
 * <li><b>datasinks[0]</b> — the native MQTT command topic (a paho producer).</li>
 * </ul>
 *
 * The response half of the request/reply exchange lives in the trigger data:
 *
 * <ul>
 * <li>{@code responseDatasource} — the MQTT response topic consumer
 * (a paho consumer on e.g. {@code .../DATA/<skill>});</li>
 * <li>{@code responseTransformer} — the response-direction Lua blob
 * (native MQTT response → REST operation-delegation response);</li>
 * <li>{@code timeoutSeconds} — how long to wait for the correlated reply.</li>
 * </ul>
 *
 * The routes are asset-dependent: they only exist for assets whose AIMC
 * declares the corresponding bidirectional (request/response) mapping.
 *
 * @author aas-camel-dmp
 */
public class OperationRouteConfiguration extends RouteConfiguration {
	public static final String ROUTE_TRIGGER = "operation";
	public static final String RESPONSE_DATASOURCE = "responseDatasource";
	public static final String RESPONSE_TRANSFORMER = "responseTransformer";
	public static final String TIMEOUT_SECONDS = "timeoutSeconds";
	public static final long DEFAULT_TIMEOUT_SECONDS = 30L;

	public OperationRouteConfiguration(RouteConfiguration configuration) {
		super(configuration);
	}

	public String getResponseDatasource() {
		return (String) getTriggerData().get(RESPONSE_DATASOURCE);
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
}
