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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fan-out: the mirror image of the fan-in half (SPEC §6.4).
 *
 * <p>Both halves follow the same shape and share one vocabulary:
 *
 * <pre>
 *   fan-in    N source contributions ─▶ ONE {@link KeyedSnapshot} keyed by SourceId
 *             (disassembled at the source, gated on completeness)
 *   transform ONE evaluation of the shared snapshot
 *   fan-out   the shared result      ─▶ ONE {@link KeyedSnapshot} keyed by SinkId
 *             (parsed once)            ─▶ N independent payloads, one per sink
 * </pre>
 *
 * <p>The invariants are explicit here, not inherited from a framework detail:
 *
 * <ol>
 * <li>the transform result is parsed <b>once</b> into a single immutable
 * {@link KeyedSnapshot};</li>
 * <li>every sink payload is computed from that snapshot <b>up front</b>, before
 * a single byte is sent;</li>
 * <li>the route then only walks the precomputed list, so a sink can never
 * observe a sibling's output and no per-branch message copying is relied
 * upon.</li>
 * </ol>
 *
 * <p>Sinks are emitted in configured order from a single route — no
 * {@code multicast} (whose contract is "the <i>same</i> message to N endpoints",
 * not "N differently shaped messages") and no per-sink {@code direct:} branch
 * routes, so route count stays a function of mappings rather than of sinks.
 */
public final class FanOut {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanOut.class);

	/** Exchange property carrying the one shared, immutable result snapshot. */
	private static final String RESULT_PROPERTY = "dmp.sharedResult";
	/** Exchange property carrying the shared result serialised for a sink. */
	private static final String SINK_PAYLOADS_PROPERTY = "dmp.sharedPayload";
	/** Exchange property carrying the sinks that failed, if any. */
	private static final String SINK_FAILURES_PROPERTY = "dmp.sinkFailures";

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
	public static void apply(ProcessorDefinition<?> route, RoutesConfiguration routesConfiguration,
			RouteConfiguration routeConfig, String[] transformerEndpoints, String[] sinkEndpoints,
			Map<String, String[]> perSinkTransformers, String routeId) {
		if (perSinkTransformers != null && !perSinkTransformers.isEmpty()) {
			String[] hoisted = hoistCommonSequence(perSinkTransformers, sinkEndpoints);
			if (hoisted != null) {
				perSinkTransformers = null; // fully covered; one shared sequence below
				transformerEndpoints = hoisted;
			} else {
				// heterogeneous per-sink sequences stay per-sink (override
				// semantics); no route-level pass.
				transformerEndpoints = new String[0];
			}
		}

		if (transformerEndpoints != null && transformerEndpoints.length > 0) {
			route.process(exchange -> LOGGER.debug("route {}: body before transformer: {}",
					routeId, exchange.getMessage().getBody(String.class)));
			route.to(transformerEndpoints);
		}

		List<String> sinkIds = routeConfig.getDatasinks();
		if (sinkEndpoints.length == 0) {
			LOGGER.warn("route {}: no sink endpoints", routeId);
			return;
		}

		String[] configIds = new String[sinkEndpoints.length];
		for (int i = 0; i < sinkEndpoints.length; i++) {
			configIds[i] = i < sinkIds.size() ? sinkIds.get(i)
					: sinkIdForEndpoint(routesConfiguration, routeConfig, sinkEndpoints[i]);
		}

		// invariant 1: the shared result is parsed ONCE into one immutable
		// snapshot; every sink below starts from that same value
		route.process(exchange -> {
			KeyedSnapshot result = KeyedSnapshot.parse(exchange.getMessage().getBody());
			exchange.setProperty(RESULT_PROPERTY, result);
			exchange.setProperty(SINK_PAYLOADS_PROPERTY, sharedJson(result));
			exchange.setProperty(SINK_FAILURES_PROPERTY, new java.util.ArrayList<String>());
		});

		// invariant 2 + 3: each sink's body is derived from the shared value
		// (never from a sibling's output) and no per-branch message copying is
		// relied upon. Sinks are emitted in configured order from one route, so
		// route count is a function of mappings, not of sinks.
		final Map<String, String[]> sinkTransformers = perSinkTransformers;
		for (int i = 0; i < sinkEndpoints.length; i++) {
			final String configId = configIds[i];
			final String[] extra = sinkTransformers == null
					? null
					: sinkTransformers.get(sinkEndpoints[i]);
			// start every sink from the shared result; an unmapped sink is
			// already final, a mapped one still needs its own transformer chain
			route.process(exchange -> exchange.getMessage().setBody(
					extra == null || extra.length == 0
							? sinkPayload(exchange, configId, routesConfiguration)
							: sharedJson(snapshotOf(exchange))));
			if (extra != null && extra.length > 0) {
				// per-sink override semantics: this chain runs on its sink's
				// copy only, and its output is what gets assembled below
				route.to(extra);
				route.process(exchange -> exchange.getMessage().setBody(
						assemble(KeyedSnapshot.parse(exchange.getMessage().getBody()),
								configId, routesConfiguration)));
			}
			// every sink is attempted; failures are collected and reported once
			// at the end rather than aborting the remaining sinks
			final String endpoint = sinkEndpoints[i];
			route.doTry()
					.to(endpoint)
					.doCatch(Exception.class)
					.process(e -> {
						sinkFailures(e).add(endpoint);
						LOGGER.warn("route {}: sink {} ({}) failed",
								routeId, configId, endpoint, e);
					})
					.end();
		}

		route.process(FanOut::reportFailures);
	}

	private static String sharedJson(KeyedSnapshot result) {
		return result.toJson();
	}

	private static KeyedSnapshot snapshotOf(org.apache.camel.Exchange exchange) {
		return exchange.getProperty(RESULT_PROPERTY, KeyedSnapshot.class);
	}

	@SuppressWarnings("unchecked")
	private static List<String> sinkFailures(org.apache.camel.Exchange exchange) {
		return (List<String>) exchange.getProperty(SINK_FAILURES_PROPERTY);
	}

	/**
	 * Makes a partial fan-out visible: every sink was attempted, and a sink that
	 * failed must not pass as a successful write. Throwing here (after the fact)
	 * keeps both properties — all sinks attempted, and the failure reported.
	 * Change this to a debug log to make fan-out best-effort.
	 */
	private static void reportFailures(org.apache.camel.Exchange exchange) {
		List<String> failures = sinkFailures(exchange);
		if (failures.isEmpty()) {
			return;
		}
		throw new IllegalStateException("fan-out could not deliver to sinks: " + failures);
	}

	/**
	 * The payload one sink receives, derived from the shared result: its own
	 * {@code SinkId} entry (when declared), optionally re-shaped at the sink's
	 * {@code valuePath}. A sink that self-handles its value path (e.g. AAS)
	 * receives the whole result.
	 */
	private static String sinkPayload(org.apache.camel.Exchange exchange, String dataSinkId,
			RoutesConfiguration routesConfiguration) {
		return assemble(snapshotOf(exchange), dataSinkId, routesConfiguration);
	}

	/** The same shaping step, over an arbitrary (possibly transformed) value. */
	private static String assemble(KeyedSnapshot result, String dataSinkId,
			RoutesConfiguration routesConfiguration) {
		DataSinkConfiguration config = dataSinkId == null
				? null
				: routesConfiguration.getDatasinks().get(dataSinkId);
		if (config == null || config.handlesValuePathInternally()) {
			return result.toJson();
		}
		String sinkId = config.getSinkId();
		String valuePath = config.getValuePath();

		Object value = result.root();
		if (sinkId != null && !sinkId.isBlank()) {
			if (!result.has(sinkId)) {
				throw new IllegalArgumentException("Sink '" + dataSinkId
						+ "': SinkId '" + sinkId + "' not present in the transform result");
			}
			value = result.value(sinkId);
		}

		if (valuePath == null || valuePath.isBlank()) {
			// coarse: write the whole SinkId value
			return value instanceof String ? (String) value : KeyedSnapshot.of(value).toJson();
		}
		// fine: construct the outgoing payload with the value at the pointer
		return KeyedSnapshot.of(assembleAt(JsonPointer.parse(valuePath).tokens(), value)).toJson();
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
