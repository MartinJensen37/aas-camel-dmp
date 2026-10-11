/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons whom the Software is furnished to do so, subject to
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
package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpComponent;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.component.health.HealthCheckUtils;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.factory.LuaDefaultConfigurationFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * DMP smoke test (M2): boots the real {@link DmpComponent} (the same
 * runtime as {@code DmpExecutable}) with a {@code sync-poll -> lua ->
 * sink} route (fixed execution semantic, decision 2026-09-15: the sync
 * source is polled at its declared interval and every poll tick evaluates
 * the mapping), configured through the standard JSON config files, and
 * verifies the AIMC Lua blob executes and its output reaches the datasink.
 */
public class SmokeTestLuaDmpRouteTest {

	/** Free port so the health endpoint does not clash with other services. */
	private static final String HEALTH_PORT = freePort();

	private static String freePort() {
		try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
			return String.valueOf(socket.getLocalPort());
		} catch (java.io.IOException e) {
			return "18085";
		}
	}

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private final List<Map<?, ?>> captured = new CopyOnWriteArrayList<>();

	@Test
	public void timerToLuaToSinkRunsInRealDmp() throws Exception {
		System.setProperty(HealthCheckUtils.HEALTH_PORT_SYSTEM_PROPERTY, HEALTH_PORT);
		try {
			Path configDir = tempFolder.getRoot().toPath();
			writeConfigFiles(configDir);

			ClassLoader loader = getClass().getClassLoader();
			RoutesConfiguration configuration = new RoutesConfiguration();

			RoutesConfigurationFactory routesFactory = new RoutesConfigurationFactory(
					path(configDir, "routes.json"), loader);
			configuration.addRoutes(routesFactory.create());

			LuaDefaultConfigurationFactory luaFactory = new LuaDefaultConfigurationFactory(
					path(configDir, "luatransformer.json"), loader);
			configuration.addTransformers(luaFactory.create());

			CaptureDataSinkDefaultConfigurationFactory sinkFactory = new CaptureDataSinkDefaultConfigurationFactory(
					path(configDir, "capture.datasink.json"), loader);
			configuration.addDatasinks(sinkFactory.create());

			// the sync source: polled every 1000 ms (the evaluation driver);
			// a producer route feeds its seda queue below
			DataSourceConfiguration tickSource = new DataSourceConfiguration("tickSource", null, 0) {
				@Override
				public String getConnectionURI() {
					return "seda:smokeTick";
				}
			};
			tickSource.setSourceId("body");
			tickSource.setDelivery("sync");
			tickSource.setPollingIntervalMs(1000);
			tickSource.setSchema(Map.of("type", "object", "properties",
					Map.of("value", Map.of("type", "integer"))));
			configuration.addDatasources(List.of(tickSource));

			ExposedDmpComponent updater = new ExposedDmpComponent(configuration);

			updater.startComponent();

			// capture route for the direct: datasink (added after start; routes
			// added to a not-yet-started context are not registered in Camel 4)
			updater.getContext().addRoutes(new RouteBuilder() {
				@Override
				public void configure() throws Exception {
					from("direct:luaCapture").process(exchange -> captured
							.add(exchange.getMessage().getBody(Map.class)));
					// feeds the sync source's seda queue
					from("timer:smokeFeed?period=200")
							.setBody(constant("{\"value\":1}"))
							.to("seda:smokeTick");
				}
			});

			try {
				// the sync poll interval is 1000 ms; wait for a few cycles
				Thread.sleep(5000);
			} finally {
				updater.stopComponent();
			}

			assertFalse("no exchange reached the datasink", captured.isEmpty());
			Map<?, ?> first = captured.get(0);
			assertTrue("datasink output must contain the Lua-transformed value, got: " + first,
					first.containsKey("doubled"));
			assertEquals(2, ((Number) first.get("doubled")).intValue());
		} finally {
			System.clearProperty(HealthCheckUtils.HEALTH_PORT_SYSTEM_PROPERTY);
		}
	}

	private void writeConfigFiles(Path configDir) throws Exception {
		write(configDir, "routes.json", """
				[
				  {
				    "datasources": ["tickSource"],
				    "transformers": ["luaTransformer1"],
				    "datasinks": ["captureSink"],
				    "trigger": "execution-policy"
				  }
				]
				""");
		write(configDir, "luatransformer.json", """
				[
				  {
				    "uniqueId": "luaTransformer1",
				    "queryPath": "queries/simple.lua"
				  }
				]
				""");
		write(configDir, "capture.datasink.json", """
				[
				  {
				    "uniqueId": "captureSink",
				    "captureEndpoint": "luaCapture"
				  }
				]
				""");
		Files.createDirectories(configDir.resolve("queries"));
		write(configDir, "queries/simple.lua", """
				-- AIMC-compliant blob: doubles the incoming "value" field.
				function aimc_main(sources)
				    local payload = sources["body"] or {}
				    local value = payload["value"] or 1
				    return {
				        ["doubled"] = value * 2
				    }
				end
				""");
	}

	private String path(Path configDir, String name) {
		return configDir.resolve(name).toString();
	}

	private void write(Path configDir, String name, String content) throws Exception {
		Files.writeString(configDir.resolve(name), content, StandardCharsets.UTF_8);
	}

	/** Exposes the protected Camel context for the test capture route. */
	private static class ExposedDmpComponent extends DmpComponent {
		ExposedDmpComponent(RoutesConfiguration configuration) {
			super(configuration);
		}

		CamelContext getContext() {
			return camelContext;
		}
	}
}
