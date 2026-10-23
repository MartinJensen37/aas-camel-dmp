/*******************************************************************************
 * Copyright (C) 2024 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;

/**
 * @author Dmp authors, jungjan
 */
public class RouteCreatorHelper {
	private RouteCreatorHelper() {
	}

	/**
	 * Resolves one datasource endpoint, failing with a contextual message when
	 * the id is unknown.
	 */
	public static String getDataSourceEndpoint(RoutesConfiguration routesConfiguration, String dataSourceId,
			String routeLabel) {
		Map<String, DataSourceConfiguration> datasources = routesConfiguration.getDatasources();
		DataSourceConfiguration config = datasources == null ? null : datasources.get(dataSourceId);
		if (config == null) {
			throw new IllegalArgumentException(
					"route " + routeLabel + " references unknown datasource '" + dataSourceId + "'");
		}
		return config.getConnectionURI();
	}

	public static String getDataSinkEndpoint(RoutesConfiguration routesConfiguration, String dataSinkId, String routeLabel) {
		Map<String, DataSinkConfiguration> datasinks = routesConfiguration.getDatasinks();
		DataSinkConfiguration config = datasinks == null ? null : datasinks.get(dataSinkId);
		if (config == null) {
			throw new IllegalArgumentException(
					"route " + routeLabel + " references unknown datasink '" + dataSinkId + "'");
		}
		return config.getConnectionURI();
	}

	public static String[] getDataSinkEndpoints(RoutesConfiguration routesConfiguration, List<String> dataSinkIdList,
			String routeLabel) {
		return dataSinkIdList.stream()
				.map(id -> getDataSinkEndpoint(routesConfiguration, id, routeLabel))
				.toArray(String[]::new);
	}

	public static String[] getDataTransformerEndpoints(RoutesConfiguration routesConfiguration, List<String> transformerIds,
			String routeLabel) {
		if (transformerIds == null) {
			return new String[0];
		}
		Map<String, DataTransformerConfiguration> transformers = routesConfiguration.getTransformers();
		return transformerIds.stream()
				.map(id -> {
					DataTransformerConfiguration config = transformers == null ? null : transformers.get(id);
					if (config == null) {
						throw new IllegalArgumentException("route " + routeLabel
								+ " references unknown transformer '" + id + "'");
					}
					return config.getConnectionURI();
				})
				.toArray(String[]::new);
	}

	/**
	 * One fresh Read of a sync source: its connector's {@link SynchronousSourceReadAdapter}
	 * when one is registered, otherwise a bounded {@code pollEnrich} of the consumer endpoint.
	 */
	public static void configureSyncRead(org.apache.camel.model.ProcessorDefinition<?> route, String endpointUri,
			long timeoutMs) {
		SynchronousSourceReadAdapter adapter = java.util.ServiceLoader.load(SynchronousSourceReadAdapter.class).stream()
				.map(java.util.ServiceLoader.Provider::get)
				.filter(candidate -> candidate.supports(endpointUri))
				.findFirst().orElse(null);
		if (adapter == null) {
			route.setBody(org.apache.camel.builder.Builder.constant(null)).pollEnrich(endpointUri, timeoutMs);
		} else {
			adapter.configureRead(route, endpointUri);
		}
	}

	/**
	 * Normalizes a subscribed source's connector messages (e.g. OPC UA DataValues)
	 * when the source declares a {@code sourceKind}; messages that carry no value
	 * are filtered out.
	 */
	public static org.apache.camel.model.ProcessorDefinition<?> normalizeSourceEvents(
			org.apache.camel.model.ProcessorDefinition<?> route, DataSourceConfiguration config, String routeLabel) {
		if (config == null || config.getSourceKind() == null || config.getSourceKind().isBlank()) return route;
		SourceEventAdapter adapter = java.util.ServiceLoader.load(SourceEventAdapter.class).stream()
				.map(java.util.ServiceLoader.Provider::get)
				.filter(candidate -> config.getSourceKind().equals(candidate.sourceKind()))
				.findFirst().orElseThrow(() -> new IllegalArgumentException("route " + routeLabel
						+ " has no connector for event sourceKind " + config.getSourceKind()));
		return route.process(adapter.normalizer(config))
				.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY)));
	}

}
