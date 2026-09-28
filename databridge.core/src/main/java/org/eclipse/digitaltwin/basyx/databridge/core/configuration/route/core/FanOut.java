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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared transform-once / multicast fan-out (T2.1, SPEC §6.4).
 *
 * <p>
 * The route-level transformers run exactly once on the aggregated body; every
 * sink then receives a COPY of that result (multicast), so each sink's
 * assembly reads the shared return object instead of the previous sink's
 * mutated output — the sequential multi-sink corruption is structurally gone.
 */
public final class FanOut {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanOut.class);

	private FanOut() {
	}

	/**
	 * Appends transform + fan-out to {@code route}.
	 *
	 * @param transformerEndpoints
	 *            route-level transformers (run once)
	 * @param perSinkTransformers
	 *            optional heterogeneous {@code datasinkMappingConfiguration}
	 *            sequences keyed by sink ENDPOINT URI; when every sink maps to
	 *            the same sequence it is hoisted to run once instead.
	 */
	public static void apply(RouteBuilder routeBuilder, ProcessorDefinition<?> route,
			RoutesConfiguration routesConfiguration,
			RouteConfiguration routeConfig, String[] transformerEndpoints, String[] sinkEndpoints,
			Map<String, String[]> perSinkTransformers, String routeId) {
		if (perSinkTransformers != null && !perSinkTransformers.isEmpty()) {
			String[] hoisted = hoistCommonSequence(perSinkTransformers, sinkEndpoints);
			if (hoisted != null) {
				perSinkTransformers = null; // fully covered; plain fan-out below
				transformerEndpoints = hoisted;
			} else {
				// heterogeneous per-sink sequences stay per-branch (override
				// semantics); no route-level pass.
				transformerEndpoints = new String[0];
			}
		}

		if (transformerEndpoints != null && transformerEndpoints.length > 0) {
			route.process(exchange -> LOGGER.debug("route {}: body before transformer: {}",
					routeId, exchange.getMessage().getBody(String.class)));
			route.to(transformerEndpoints).to("log:" + routeId);
		}

		List<String> sinkIds = routeConfig.getDatasinks();
		if (sinkEndpoints.length == 0) {
			LOGGER.warn("route {}: no sink endpoints", routeId);
			return;
		}
		if (sinkEndpoints.length == 1 && perSinkTransformers == null) {
			String configId = sinkIds.get(0);
			if (needsAssembly(routesConfiguration.getDatasinks().get(configId))) {
				route.process(exchange -> applySinkAssemblyProcessor(exchange, configId, routesConfiguration));
			}
			route.to(sinkEndpoints[0]).to("log:" + routeId);
			return;
		}

		String[] branches = new String[sinkEndpoints.length];
		for (int i = 0; i < sinkEndpoints.length; i++) {
			String endpoint = sinkEndpoints[i];
			String configId = i < sinkIds.size() ? sinkIds.get(i)
					: sinkIdForEndpoint(routesConfiguration, routeConfig, endpoint);
			String[] extra = perSinkTransformers == null ? null : perSinkTransformers.get(endpoint);
			branches[i] = "direct:fanout-" + routeId + "-" + i;
			ProcessorDefinition<?> branch = routeBuilder.from(branches[i])
					.routeId(routeId + "-sink-" + i);
			LOGGER.debug("fan-out {}: branch {} sink={} transformers={}",
					routeId, i, endpoint,
					extra == null ? "[]" : java.util.Arrays.toString(extra));
			if (extra != null && extra.length > 0) {
				branch = branch.to(extra);
			}
			branch.process(exchange -> applySinkAssemblyProcessor(exchange, configId, routesConfiguration))
					.to(endpoint);
		}
		route.multicast().to(branches).end().to("log:" + routeId);
	}

	/**
	 * The common transformer sequence when every sink endpoint maps to the
	 * identical non-null sequence; {@code String[0]} when they are all empty;
	 * {@code null} when the mapping is heterogeneous or does not cover all
	 * sinks.
	 */
	private static String[] hoistCommonSequence(Map<String, String[]> dataSinkMapping, String[] dataSinkEndpoints) {
		if (dataSinkEndpoints.length != dataSinkMapping.size()
				|| !Arrays.stream(dataSinkEndpoints).allMatch(dataSinkMapping::containsKey)) {
			return null;
		}
		String[] first = dataSinkMapping.get(dataSinkEndpoints[0]);
		boolean identical = Arrays.stream(dataSinkEndpoints)
				.allMatch(endpoint -> Arrays.equals(first, dataSinkMapping.get(endpoint)));
		return identical ? (first == null ? new String[0] : first) : null;
	}

	private static boolean needsAssembly(DataSinkConfiguration config) {
		if (config == null || config.handlesValuePathInternally()) {
			return false;
		}
		String valuePath = config.getValuePath();
		String sinkId = config.getSinkId();
		return !(valuePath == null || valuePath.isBlank()) || !(sinkId == null || sinkId.isBlank());
	}

	private static void applySinkAssemblyProcessor(Exchange exchange, String dataSinkId,
			RoutesConfiguration routesConfiguration) {
		if (dataSinkId == null) {
			return;
		}
		DataSinkConfiguration config = routesConfiguration.getDatasinks().get(dataSinkId);
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
			if (!(json instanceof Map<?, ?> map) || !map.containsKey(sinkId)) {
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
			Object assembled = assembleAt(JsonPointer.parse(valuePath).tokens(), value);
			exchange.getMessage().setBody(gson.toJson(assembled));
		}
	}

	private static Object assembleAt(List<String> tokens, Object value) {
		return assembleAt(tokens, 0, value);
	}

	private static Object assembleAt(List<String> tokens, int offset, Object value) {
		if (offset == tokens.size()) {
			return value;
		}
		String token = tokens.get(offset);
		if (token.startsWith("[") && token.endsWith("]")) {
			int index = Integer.parseInt(token.substring(1, token.length() - 1));
			if (index < 0) {
				throw new IllegalArgumentException("negative JSON array index in sink path: " + token);
			}
			java.util.List<Object> array = new java.util.ArrayList<>();
			while (array.size() <= index) {
				array.add(null);
			}
			array.set(index, assembleAt(tokens, offset + 1, value));
			return array;
		}
		java.util.Map<String, Object> object = new java.util.LinkedHashMap<>();
		object.put(token, assembleAt(tokens, offset + 1, value));
		return object;
	}

	private static Object parseSinkPayload(String text, String dataSinkId) {
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
	private static String sinkIdForEndpoint(RoutesConfiguration routesConfiguration,
			RouteConfiguration routeConfig, String endpointUri) {
		for (String id : routeConfig.getDatasinks()) {
			DataSinkConfiguration config = routesConfiguration.getDatasinks().get(id);
			if (config != null && endpointUri.equals(config.getConnectionURI())) {
				return id;
			}
		}
		return null;
	}
}
