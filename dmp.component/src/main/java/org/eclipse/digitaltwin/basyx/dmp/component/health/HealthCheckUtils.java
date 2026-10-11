/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.component.health;

import org.springframework.http.HttpMethod;

/**
 * Utility class for health check
 *
 * @author danish
 *
 */
public class HealthCheckUtils {
	public static final String ROUTE_ID = "app.health.context";
	public static final String DEFAULT_PORT = "8085";

	/** Env var / system property that overrides the health endpoint port (per-asset DMPs). */
	public static final String HEALTH_PORT_ENV_VAR = "DATABRIDGE_HEALTH_PORT";
	public static final String HEALTH_PORT_SYSTEM_PROPERTY = "databridge.health.port";

	private static final String COMPONENT = "jetty";
	private static final String PROTOCOL = "http";
	private static final String HOST = "0.0.0.0";
	private static final String PORT = resolvePort();
	private static final String PATH = "/health";
	private static final String HTTP_METHOD_RESTRICT_PARAMETER = "httpMethodRestrict=" + HttpMethod.GET;

	private HealthCheckUtils() {
		throw new IllegalStateException("Utility class");
	}

	private static String resolvePort() {
		// the SYSTEM PROPERTY wins (tests pick a free port via
		// databridge.health.port; an inherited DATABRIDGE_HEALTH_PORT env var
		// must not shadow it); env otherwise drives the per-asset deployments
		String fromProperty = System.getProperty(HEALTH_PORT_SYSTEM_PROPERTY);
		if (fromProperty != null && !fromProperty.isBlank()) {
			return fromProperty;
		}
		String fromEnv = System.getenv(HEALTH_PORT_ENV_VAR);
		if (fromEnv != null && !fromEnv.isBlank()) {
			return fromEnv;
		}
		return DEFAULT_PORT;
	}

	public static String getHealthCheckEndpoint() {
		return COMPONENT + ":" + PROTOCOL + "://" + HOST + ":" + PORT + PATH + "?" + HTTP_METHOD_RESTRICT_PARAMETER;
	}
	
	public static String getHealthCheckEndpointRequestURI() {
		return PROTOCOL + "://" + HOST + ":" + PORT + PATH;
	}

	public static String getHealthCheckPort() {
		return PORT;
	}
}
