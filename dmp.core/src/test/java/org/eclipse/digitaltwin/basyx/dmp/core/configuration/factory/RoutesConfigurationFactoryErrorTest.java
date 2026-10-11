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
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.factory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.routebuilder.DmpRouteBuilder;
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

	private IllegalArgumentException configureWithoutConnector(String json) throws Exception {
		RoutesConfiguration routes = new RoutesConfiguration(List.of(), List.of(), List.of(), load(json));
		return assertThrows(IllegalArgumentException.class,
				() -> new DmpRouteBuilder(routes, Map.of()).configure());
	}

	@Test
	void routeConfigurationsAreLoadedGenericallyForConnectorFactories() throws Exception {
		List<RouteConfiguration> routes = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"r1\",\"datasources\":[\"a\"],\"datasinks\":[]},"
						+ "{\"trigger\":\"bridge\",\"routeId\":\"r2\",\"datasources\":[\"a\"],\"datasinks\":[]}]");
		assertEquals(2, routes.size());
		assertEquals(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER, routes.get(0).getRouteTrigger());
		assertEquals("bridge", routes.get(1).getRouteTrigger());
	}

	@Test
	void legacyTriggersArePreservedButFailWithoutRegisteredCreator() throws Exception {
		for (String legacy : new String[] {"event", "timer", "request", "operation"}) {
			String json = "[{\"trigger\":\"" + legacy
					+ "\",\"routeId\":\"old\",\"datasources\":[\"a\"],\"datasinks\":[]}]";
			assertEquals(legacy, load(json).get(0).getRouteTrigger());
			IllegalArgumentException error = configureWithoutConnector(json);
			assertTrue(error.getMessage().contains("route old: no route creator factory registered for trigger '"
					+ legacy + "'"));
		}
	}

	@Test
	void unknownTriggerIsPreservedButFailsWithoutRegisteredCreator() throws Exception {
		String json = "[{\"trigger\":\"webhook\",\"routeId\":\"mystery\"},"
				+ "{\"trigger\":\"execution-policy\",\"routeId\":\"kept\",\"datasources\":[\"a\"],\"datasinks\":[]}]";
		List<RouteConfiguration> routes = load(json);
		assertEquals(2, routes.size());
		assertEquals("webhook", routes.get(0).getRouteTrigger());
		IllegalArgumentException error = configureWithoutConnector(json);
		assertTrue(error.getMessage().contains(
				"route mystery: no route creator factory registered for trigger 'webhook'"));
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
	void explicitTriggerSourceIdsMustBeNonemptyUniqueStrings() throws Exception {
		RouteConfiguration route = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"r\","
						+ "\"triggerData\":{\"triggerSourceIds\":[\"Invoke\"]}}]").get(0);
		assertEquals(List.of("Invoke"), route.getTriggerSourceIds());
		RouteConfiguration legacy = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"legacy\"}]").get(0);
		assertEquals(null, legacy.getTriggerSourceIds());
		RouteConfiguration empty = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"empty\","
						+ "\"triggerData\":{\"triggerSourceIds\":[]}}]").get(0);
		assertThrows(IllegalArgumentException.class, empty::getTriggerSourceIds);
		RouteConfiguration duplicates = load(
				"[{\"trigger\":\"execution-policy\",\"routeId\":\"duplicates\","
						+ "\"triggerData\":{\"triggerSourceIds\":[\"Invoke\",\"Invoke\"]}}]").get(0);
		assertThrows(IllegalArgumentException.class, duplicates::getTriggerSourceIds);
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
