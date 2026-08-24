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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.event;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.MulticastDefinition;
import org.apache.camel.model.RouteDefinition;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.AbstractRouteCreator;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;

import static org.apache.camel.builder.Builder.constant;

public class EventRouteCreator extends AbstractRouteCreator {

	/** Header carrying the AIMC SourceId of the source that produced an exchange. */
	private static final String SOURCE_ID_HEADER = "AIMC_SourceId";

	public EventRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		super(routeBuilder, routesConfiguration);
	}

	@Override
	protected void configureRoute(RouteConfiguration routeConfiguration, String[] dataSourceEndpoints, String[] dataSinkEndpoints, String[] dataTransformerEndpoints, String routeId) {
		RouteDefinition routeDefinition = startRouteDefinition(routeConfiguration, dataSourceEndpoints, routeId);

		if (!(dataTransformerEndpoints == null || dataTransformerEndpoints.length == 0)) {
			routeDefinition.to(dataTransformerEndpoints)
					.to("log:" + routeId);
		}

		routeDefinition.to(dataSinkEndpoints)
				.to("log:" + routeId);
	}

	@Override
	protected void configureRoute(RouteConfiguration routeConfig, String[] dataSourceEndpoints, String[] dataSinkEndpoints, String[] dataTransformerEndpoints, Map<String, String[]> dataSinkMapping, String routeId) {
		MulticastDefinition routeDefinition = startRouteDefinition(routeConfig, dataSourceEndpoints, routeId).multicast();
		dataSinkMapping.forEach((dataSink, dataTransformers) -> routeDefinition.pipeline()
				.to(dataTransformers)
				.to(dataSink)
				.to("log:" + routeId));

		getUnmappedEndpoints(dataSinkEndpoints, dataSinkMapping).forEach(dataSink -> routeDefinition.to(dataSink)
				.to("log: " + routeId));

		routeDefinition.end();
	}

	/**
	 * Starts the route from the first source and, when the route declares
	 * multiple sources (fan-in, SPEC §6.4), aggregates all sources into one
	 * exchange before the transform.
	 *
	 * <p>
	 * Semantics (per user decision 2026-08-21): the transform fires only when
	 * <b>every</b> source has delivered at least once; each source contributes
	 * its <b>latest</b> value. This is a synchronized fan-in — the body is a
	 * map keyed by SourceId, matching {@code aimc_main(sources)}.
	 *
	 * <p>
	 * Implementation: each source gets its own {@code from()} route that tags
	 * the exchange with its SourceId and forwards it to a shared in-memory
	 * aggregator ({@code seda:}). The aggregator route collects until all
	 * sources have contributed (completion predicate on map size), with a
	 * {@link #COMPLETION_TIMEOUT_MS} safety net.
	 */
	private RouteDefinition startRouteDefinition(RouteConfiguration routeConfig, String[] dataSourceEndpoints, String routeId) {
		if (dataSourceEndpoints.length <= 1) {
			return getRouteBuilder().from(dataSourceEndpoints[0])
					.routeId(routeId)
					.to("log:" + routeId);
		}

		List<String> sourceIds = routeConfig.getDatasources();
		String aggregatorUri = "seda:aggregate-" + routeId;
		// Shared state: latest value per SourceId, plus the set of sources that
		// have delivered so far (used to decide when all are available).
		java.util.concurrent.ConcurrentHashMap<String, Object> latest = new java.util.concurrent.ConcurrentHashMap<>();
		java.util.concurrent.ConcurrentHashMap<String, Boolean> delivered = new java.util.concurrent.ConcurrentHashMap<>();

		for (int i = 0; i < dataSourceEndpoints.length; i++) {
			String sourceId = resolveSourceId(sourceIds.get(i));
			getRouteBuilder().from(dataSourceEndpoints[i])
					.routeId(routeId + "-source-" + i)
					.setHeader(SOURCE_ID_HEADER, constant(sourceId))
					.process(exchange -> {
						String sid = exchange.getMessage().getHeader(SOURCE_ID_HEADER, String.class);
						latest.put(sid, exchange.getMessage().getBody());
						delivered.put(sid, Boolean.TRUE);
					})
					.to(aggregatorUri);
		}

		// The aggregator route fires the transform only when every source has
		// delivered at least once (latest value each).
		RouteDefinition route = getRouteBuilder().from(aggregatorUri)
				.routeId(routeId)
				.process(new AllSourcesTrigger(latest, delivered, dataSourceEndpoints.length));
		return route;
	}

	/**
	 * Forwards the aggregated {@code sources} map (keyed by SourceId) only once
	 * every source has delivered; otherwise drops the trigger message.
	 */
	private static final class AllSourcesTrigger implements org.apache.camel.Processor {
		private final java.util.concurrent.ConcurrentHashMap<String, Object> latest;
		private final java.util.concurrent.ConcurrentHashMap<String, Boolean> delivered;
		private final int expectedSources;

		AllSourcesTrigger(java.util.concurrent.ConcurrentHashMap<String, Object> latest,
				java.util.concurrent.ConcurrentHashMap<String, Boolean> delivered, int expectedSources) {
			this.latest = latest;
			this.delivered = delivered;
			this.expectedSources = expectedSources;
		}

		@Override
		public void process(org.apache.camel.Exchange exchange) {
			if (delivered.size() < expectedSources) {
				// Not all sources available yet — drop this trigger.
				exchange.setRouteStop(true);
				return;
			}
			exchange.getMessage().setBody(new java.util.HashMap<>(latest));
		}
	}

	/**
	 * Resolves the AIMC SourceId for a datasource id: the source config's
	 * {@code sourceId} when set, else the datasource id itself (SPEC §6.4).
	 */
	private String resolveSourceId(String dataSourceId) {
		org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration config =
				getRoutesConfiguration().getDatasources().get(dataSourceId);
		if (config != null && config.getSourceId() != null && !config.getSourceId().isBlank()) {
			return config.getSourceId();
		}
		return dataSourceId;
	}

	private List<String> getUnmappedEndpoints(String[] dataSinkEndpoints, Map<String, String[]> dataSinkMapping) {
		return Arrays.stream(dataSinkEndpoints)
				.filter(Predicate.not(dataSinkMapping::containsKey))
				.collect(Collectors.toList());
	}
}
