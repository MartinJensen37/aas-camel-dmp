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
package dk.aau.smartproductionlab.dmp.component.health;


/**
 * Utility class for health check
 *
 * @author danish
 *
 */
public class HealthCheckUtils {
	public static final String ROUTE_ID = "app.health.context";
	public static final String DEFAULT_PORT = "8080";

	/** Env var / system property that overrides the port of the HTTP listener (health, status, Operations). */
	public static final String HEALTH_PORT_ENV_VAR = "DMP_HTTP_PORT";
	public static final String HEALTH_PORT_SYSTEM_PROPERTY = "dmp.http.port";

	private static final String COMPONENT = "jetty";
	private static final String PROTOCOL = "http";
	private static final String HOST = "0.0.0.0";
	private static final String PATH = "/health";
	private static final String HTTP_METHOD_RESTRICT_PARAMETER = "httpMethodRestrict=GET";

	private HealthCheckUtils() {
		throw new IllegalStateException("Utility class");
	}

	private static String resolvePort() {
		// The system property wins (tests pick a free port with dmp.http.port; an
		// inherited DMP_HTTP_PORT must not shadow it); the env var otherwise.
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
		return COMPONENT + ":" + PROTOCOL + "://" + HOST + ":" + resolvePort() + PATH + "?" + HTTP_METHOD_RESTRICT_PARAMETER;
	}
	
	public static String getHealthCheckEndpointRequestURI() {
		return PROTOCOL + "://" + HOST + ":" + resolvePort() + PATH;
	}

	public static String getHealthCheckPort() {
		return resolvePort();
	}
}
