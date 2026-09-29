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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.bridge.ProtocolBridgeRouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RoutesConfigurationFactoryErrorTest {
	@TempDir
	Path tempDir;

	private List<RouteConfiguration> load(String json) throws Exception {
		Path file = tempDir.resolve("routes.json");
		Files.writeString(file, json);
		return new RoutesConfigurationFactory(file.toString(), getClass().getClassLoader()).create();
	}

	@Test
	void policyAndBridgeRoutesMapToTheirConfigurations() throws Exception {
		List<RouteConfiguration> routes = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"r1\",\"datasources\":[\"a\"],\"datasinks\":[]},"
						+ "{\"trigger\":\"bridge\",\"routeId\":\"r2\",\"datasources\":[\"a\"],\"datasinks\":[]}]");
		assertEquals(2, routes.size());
		assertEquals(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER, routes.get(0).getRouteTrigger());
		assertEquals(ProtocolBridgeRouteConfiguration.ROUTE_TRIGGER, routes.get(1).getRouteTrigger());
	}

	@Test
	void legacyTriggersFailWithNamedError() throws Exception {
		for (String legacy : new String[] {"event", "timer", "request", "operation"}) {
			IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
					() -> load("[{\"trigger\":\"" + legacy + "\",\"routeId\":\"old\",\"datasources\":[\"a\"],\"datasinks\":[]}]"));
			assertTrue(error.getMessage().contains("route old declares trigger '" + legacy + "'"));
		}
	}

	@Test
	void unknownTriggerFailsRatherThanSilentlyDroppingRoute() throws Exception {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> load(
				"[{\"trigger\":\"webhook\",\"routeId\":\"mystery\"},"
						+ "{\"trigger\":\"execution-policy\",\"routeId\":\"kept\",\"datasources\":[\"a\"],\"datasinks\":[]}]"));
		assertTrue(error.getMessage().contains("route mystery declares trigger 'webhook'"));
	}

	@Test
	void missingTriggerFailsWithClearMessageInsteadOfNpe() throws Exception {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> load("[{\"datasources\":[\"a\"],\"datasinks\":[]}]"));
		assertTrue(error.getMessage().contains("routes.json[0]") && error.getMessage().contains("declares no trigger"),
				"error must name the problem: " + error.getMessage());
	}

	@Test
	void blankTriggerFailsNamingRoute() {
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
				() -> load("[{\"trigger\":\"  \",\"routeId\":\"blank\"}]"));
		assertTrue(error.getMessage().contains("route blank declares no trigger"));
	}

	@Test
	void legacySingularDatasourceKeyIsNoLongerTolerated() throws Exception {
		List<RouteConfiguration> routes = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"r\",\"datasource\":\"a\",\"datasinks\":[]}]");
		// the key is ignored (not mapped): the route loads but has NO sources,
		// which every creator then rejects as an authoring error
		assertTrue(routes.get(0).getDatasources().isEmpty(),
				"the singular legacy key must not map onto datasources");
	}
}
