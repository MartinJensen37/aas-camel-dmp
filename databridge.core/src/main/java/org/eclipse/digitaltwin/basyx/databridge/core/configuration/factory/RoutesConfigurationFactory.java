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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.factory;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.bridge.ProtocolBridgeRouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.writedelegation.WriteDelegationRouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.policy.ExecutionPolicyRouteConfiguration;

/**
 * A generic implementation of routes configuration factory
 *
 * @author haque
 *
 */
public class RoutesConfigurationFactory extends ConfigurationFactory {
	public static final String DEFAULT_FILE_PATH = "routes.json";

	/**
	 * This constructor uses the {@link ExecutionPolicyRouteConfiguration} as the default
	 * configuration
	 *
	 * @param filePath
	 * @param loader
	 */
	public RoutesConfigurationFactory(String filePath, ClassLoader loader) {
		super(filePath, loader, RouteConfiguration.class);
	}

	/**
	 * This constructor uses the default path {@link #DEFAULT_FILE_PATH} and the
	 * {@link ExecutionPolicyRouteConfiguration} as the default configuration
	 *
	 * @param loader
	 */
	public RoutesConfigurationFactory(ClassLoader loader) {
		super(DEFAULT_FILE_PATH, loader, RouteConfiguration.class);
	}

	@SuppressWarnings("unchecked")
	public List<RouteConfiguration> create() {
		List<RouteConfiguration> configurations = (List<RouteConfiguration>) getConfigurationLoader().loadListConfiguration();

		return mapToSpecificRouteConfigurations(configurations);
	}

	private List<RouteConfiguration> mapToSpecificRouteConfigurations(List<RouteConfiguration> configurations) {
		List<RouteConfiguration> mapped = new ArrayList<>();

		for (int index = 0; index < configurations.size(); index++) {
			RouteConfiguration configuration = configurations.get(index);
			String label = configuration.getRouteId() != null
					? configuration.getRouteId()
					: "routes.json[" + index + "]";
			String trigger = configuration.getRouteTrigger();
			if (trigger == null || trigger.isBlank()) {
				throw new IllegalArgumentException(
						"route " + label + " declares no trigger (expected one of: "
								+ ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER + ", "
								+ ProtocolBridgeRouteConfiguration.ROUTE_TRIGGER + ", "
								+ WriteDelegationRouteConfiguration.ROUTE_TRIGGER + ")");
			}
			if (ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER.equals(trigger)) {
				mapped.add(new ExecutionPolicyRouteConfiguration(configuration));
			} else if (ProtocolBridgeRouteConfiguration.ROUTE_TRIGGER.equals(trigger)) {
				mapped.add(new ProtocolBridgeRouteConfiguration(configuration));
			} else if (WriteDelegationRouteConfiguration.ROUTE_TRIGGER.equals(trigger)) {
				mapped.add(new WriteDelegationRouteConfiguration(configuration));
			} else {
				throw new IllegalArgumentException("route " + label + " declares trigger '" + trigger
						+ "' (supported: " + ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER + ", "
						+ ProtocolBridgeRouteConfiguration.ROUTE_TRIGGER + ", "
						+ WriteDelegationRouteConfiguration.ROUTE_TRIGGER + ")");
			}
		}

		return mapped;
	}
}
