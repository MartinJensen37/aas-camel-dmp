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

		// Per-sink assembly (fine sinks, ADR-014 / SPEC §6.5.4): the transform
		// output is a return object keyed by SinkId; each sink consumes only its
		// own value (or a constructed payload around it).
		List<String> sinkIds = routeConfiguration.getDatasinks();
		for (int i = 0; i < dataSinkEndpoints.length; i++) {
			if (i < sinkIds.size()) {
				applySinkAssembly(routeDefinition, sinkIds.get(i));
			}
			routeDefinition.to(dataSinkEndpoints[i])
					.to("log:" + routeId);
		}
	}

	@Override
	protected void configureRoute(RouteConfiguration routeConfig, String[] dataSourceEndpoints, String[] dataSinkEndpoints, String[] dataTransformerEndpoints, Map<String, String[]> dataSinkMapping, String routeId) {
		MulticastDefinition routeDefinition = startRouteDefinition(routeConfig, dataSourceEndpoints, routeId).multicast();
		dataSinkMapping.forEach((dataSink, dataTransformers) -> routeDefinition.pipeline()
				.to(dataTransformers)
				.process(exchange -> applySinkAssemblyProcessor(exchange, sinkIdForEndpoint(routeConfig, dataSink)))
				.to(dataSink)
				.to("log:" + routeId));

		getUnmappedEndpoints(dataSinkEndpoints, dataSinkMapping).forEach(dataSink -> routeDefinition.to(dataSink)
				.to("log: " + routeId));

		routeDefinition.end();
	}

	/**
	 * Inserts the post-transform assembly processor for one sink when the sink
	 * declares a {@code valuePath} / {@code sinkId} and does not handle its
	 * {@code valuePath} internally (e.g. the AAS sink self-extracts).
	 */
	private void applySinkAssembly(RouteDefinition routeDefinition, String dataSinkId) {
		org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration config =
				getRoutesConfiguration().getDatasinks().get(dataSinkId);
		if (config == null || config.handlesValuePathInternally()) {
			return;
		}
		String valuePath = config.getValuePath();
		String sinkId = config.getSinkId();
		if ((valuePath == null || valuePath.isBlank()) && (sinkId == null || sinkId.isBlank())) {
			return;
		}
		routeDefinition.process(exchange -> applySinkAssemblyProcessor(exchange, dataSinkId));
	}

	private void applySinkAssemblyProcessor(org.apache.camel.Exchange exchange, String dataSinkId) {
		if (dataSinkId == null) {
			return;
		}
		org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration config =
				getRoutesConfiguration().getDatasinks().get(dataSinkId);
		if (config == null || config.handlesValuePathInternally()) {
			return;
		}
		String valuePath = config.getValuePath();
		String sinkId = config.getSinkId();

		Object body = exchange.getMessage().getBody();
		Object json = body;
		if (body instanceof String s) {
			json = parseSinkPayload(s, dataSinkId);
		} else if (body instanceof byte[] bytes) {
			json = parseSinkPayload(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), dataSinkId);
		}

		Object value = json;
		if (sinkId != null && !sinkId.isBlank()) {
			if (!(json instanceof java.util.Map<?, ?> map) || !map.containsKey(sinkId)) {
				throw new IllegalArgumentException("Sink '" + dataSinkId
						+ "': SinkId '" + sinkId + "' not present in the transform result");
			}
			value = map.get(sinkId);
		}

		com.google.gson.Gson gson = new com.google.gson.Gson();
		if (valuePath == null || valuePath.isBlank()) {
			// coarse: write the whole SinkId value
			exchange.getMessage().setBody(value instanceof String ? (String) value : gson.toJson(value));
		} else {
			// fine: construct the outgoing payload with the value at the pointer
			java.util.Map<String, Object> assembled = new java.util.LinkedHashMap<>();
			assembled.put(org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer.parse(valuePath).leafKey(), value);
			exchange.getMessage().setBody(gson.toJson(assembled));
		}
	}

	private Object parseSinkPayload(String text, String dataSinkId) {
		try {
			return new com.google.gson.Gson().fromJson(text, Object.class);
		} catch (Exception e) {
			throw new IllegalArgumentException("Sink '" + dataSinkId
					+ "': payload is not valid JSON: " + text, e);
		}
	}

	/**
	 * Reverse-looks-up the sink config id for a resolved endpoint URI (used in
	 * the sink-mapped multicast path whose keys are endpoint URIs).
	 */
	private String sinkIdForEndpoint(RouteConfiguration routeConfig, String endpointUri) {
		for (String id : routeConfig.getDatasinks()) {
			org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration config =
					getRoutesConfiguration().getDatasinks().get(id);
			if (config != null && endpointUri.equals(config.getConnectionURI())) {
				return id;
			}
		}
		return null;
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
			String dataSourceId = routeConfig.getDatasources().get(0);
			return getRouteBuilder().from(dataSourceEndpoints[0])
					.routeId(routeId)
					.process(exchange -> disassembleSource(exchange, dataSourceId))
					.to("log:" + routeId);
		}

		List<String> sourceIds = routeConfig.getDatasources();
		String aggregatorUri = "seda:aggregate-" + routeId;
		// Shared state: latest value per SourceId, plus the set of sources that
		// have delivered so far (used to decide when all are available).
		java.util.concurrent.ConcurrentHashMap<String, Object> latest = new java.util.concurrent.ConcurrentHashMap<>();
		java.util.concurrent.ConcurrentHashMap<String, Boolean> delivered = new java.util.concurrent.ConcurrentHashMap<>();

		for (int i = 0; i < dataSourceEndpoints.length; i++) {
			String dataSourceId = sourceIds.get(i);
			String sourceId = resolveSourceId(dataSourceId);
			getRouteBuilder().from(dataSourceEndpoints[i])
					.routeId(routeId + "-source-" + i)
					.setHeader(SOURCE_ID_HEADER, constant(sourceId))
					.process(exchange -> {
						String sid = exchange.getMessage().getHeader(SOURCE_ID_HEADER, String.class);
						latest.put(sid, disassembleSource(exchange, dataSourceId));
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
	 * Applies this source's {@code valuePath} (fine resolution, ADR-014 /
	 * SPEC §6.3+§6.5.4) at the DMP boundary: reduces the source's canonical
	 * payload to the pinned JSON-Pointer leaf before it is placed under the
	 * {@code SourceId} (disassembly). Absent → coarse (whole payload). A
	 * pointer on a missing path is a clear route error (SPEC §8.4 #8).
	 */
	private Object disassembleSource(org.apache.camel.Exchange exchange, String dataSourceId) {
		org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration config =
				getRoutesConfiguration().getDatasources().get(dataSourceId);
		String pointer = config == null ? null : config.getValuePath();
		if (pointer == null || pointer.isBlank()) {
			return exchange.getMessage().getBody();
		}

		Object body = exchange.getMessage().getBody();
		Object json = body;
		if (body instanceof String text) {
			json = parseSourcePayload(text, pointer, dataSourceId);
		} else if (body instanceof byte[] bytes) {
			json = parseSourcePayload(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), pointer, dataSourceId);
		}

		java.util.Optional<Object> leaf = org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer.extract(json, pointer);
		if (leaf.isEmpty()) {
			throw new IllegalArgumentException("Source '" + dataSourceId
					+ "': valuePath '" + pointer + "' does not resolve in the incoming payload");
		}
		return leaf.get();
	}

	private Object parseSourcePayload(String text, String pointer, String dataSourceId) {
		try {
			return new com.google.gson.Gson().fromJson(text, Object.class);
		} catch (Exception e) {
			throw new IllegalArgumentException("Source '" + dataSourceId
					+ "': valuePath '" + pointer + "' set but the payload is not valid JSON: " + text, e);
		}
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
