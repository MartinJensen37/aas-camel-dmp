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
package org.eclipse.digitaltwin.basyx.dmp.core.component;

import java.util.HashMap;
import java.util.Map;
import org.apache.camel.CamelContext;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.health.DefaultHealthCheckRegistry;
import org.apache.camel.impl.health.RoutesHealthCheckRepository;
import org.eclipse.basyx.components.IComponent;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.routebuilder.DmpRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core Dmp component which can run the updater if routes configuration is
 * provided
 *
 * @author haque, fischer, danish
 *
 */
public class DmpComponent implements IComponent {
	private static Logger logger = LoggerFactory.getLogger(DmpComponent.class);
	private DmpRouteBuilder orchestrator;

	protected CamelContext camelContext;

	public DmpComponent(RoutesConfiguration configuration) {
		camelContext = new DefaultCamelContext();
		orchestrator = new DmpRouteBuilder(configuration, getRouteCreatorFactoryMapDefault());
	}

	private static Map<String, RouteCreatorFactory> getRouteCreatorFactoryMapDefault() {
		Map<String, RouteCreatorFactory> defaultRouteCreatorFactoryMap = new HashMap<>();
		for (RouteCreatorFactory factory : java.util.ServiceLoader
				.load(RouteCreatorFactory.class, DmpComponent.class.getClassLoader())) {
			RouteCreatorFactory existing = defaultRouteCreatorFactoryMap.putIfAbsent(
					factory.routeTrigger(), factory);
			if (existing != null) {
				throw new IllegalStateException("multiple route creator factories register trigger '"
						+ factory.routeTrigger() + "'");
			}
		}
		if (!defaultRouteCreatorFactoryMap.containsKey(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER)) {
			throw new IllegalStateException("no connector registers the standard execution-policy route");
		}
		return defaultRouteCreatorFactoryMap;
	}

	/**
	 * Starts the Camel component
	 */
	@Override
	public void startComponent() {
		startRoutes();
	}

	public void startRoutes() {
		try {
			configureHealthCheck();
			camelContext.addRoutes(orchestrator);
			camelContext.start();
			logger.info("Updater started");
		} catch (Exception e) {
			e.printStackTrace();
			camelContext = null;
		}
	}

	private void configureHealthCheck() throws Exception {
		camelContext.setLoadHealthChecks(true);
		// Camel 4.x: setExtension() was removed; use the context plugin API instead
		camelContext.getCamelContextExtension().addContextPlugin(HealthCheckRegistry.class,
				configureHealthCheckRegistry());
		for (DmpRuntimeExtension extension : java.util.ServiceLoader
				.load(DmpRuntimeExtension.class, Thread.currentThread().getContextClassLoader())) {
			extension.configure(camelContext);
		}
	}

	private HealthCheckRegistry configureHealthCheckRegistry() {
		HealthCheckRegistry registry = new DefaultHealthCheckRegistry();
		registry.register(new RoutesHealthCheckRepository());
		return registry;
	}

	/**
	 * Stops the Camel component
	 */
	@Override
	public void stopComponent() {
		if (camelContext != null && !camelContext.isStopped()) {
			camelContext.stop();
			logger.info("Updater stopped");
		}
	}
}
