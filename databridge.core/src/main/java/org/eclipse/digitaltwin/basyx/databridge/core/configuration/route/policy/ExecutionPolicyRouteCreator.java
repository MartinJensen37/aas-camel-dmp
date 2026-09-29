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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.policy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.FanOut;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreator;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.KeyedSnapshot;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ONE mapping-route creator: every MappingConfiguration compiles to
 * {@code sources[] + sinks[] + transformer(s)} and this creator wires exactly
 * that, with the FIXED execution semantic (decision 2026-09-15, replacing the
 * authorable ADR-020 policy):
 *
 * <ul>
 * <li>every source contributes its LATEST value to a per-route cache —
 * async sources on message arrival, sync sources polled by their own
 * declared {@code pollingIntervalMs} (the AIMC's IDTA polling interval, MC
 * level or per-source override);</li>
 * <li>an update of ANY source (async arrival or sync poll tick) evaluates
 * the mapping <b>only once ALL sources of the mapping have delivered</b> —
 * a partial sources map is never emitted (the blob contract).</li>
 * </ul>
 *
 * <p>
 * There are no authorable knobs: no trigger kinds, no barriers, no
 * completion timeouts, no staleness bounds — sync cadence comes from the
 * declared IDTA polling intervals (plan Q8), and a partial map is never
 * emitted (plan Q9's conservative disposition is now the only behavior).
 *
 * <p>
 * Fan-out reuses the shared transform-once multicast {@link FanOut}.
 */
public class ExecutionPolicyRouteCreator implements RouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPolicyRouteCreator.class);

	/** Header carrying the AIMC SourceId on async feeder routes. */
	private static final String SOURCE_ID_HEADER = "AIMC_SourceId";

	private final RouteBuilder routeBuilder;
	private final RoutesConfiguration routesConfiguration;

	public ExecutionPolicyRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		this.routeBuilder = routeBuilder;
		this.routesConfiguration = routesConfiguration;
	}

	@Override
	public void addRouteToRouteBuilder(RouteConfiguration routeConfig) {
		String routeId = routeConfig.getRouteId() != null ? routeConfig.getRouteId() : "<unnamed-policy>";

		String[] dataSourceEndpoints = RouteCreatorHelper.getDataSourceEndpoints(
				routesConfiguration, routeConfig.getDatasources(), routeId);
		String[] dataSinkEndpoints = RouteCreatorHelper.getDataSinkEndpoints(
				routesConfiguration, routeConfig.getDatasinks(), routeId);
		String[] dataTransformerEndpoints = RouteCreatorHelper.getDataTransformerEndpoints(
				routesConfiguration, routeConfig.getTransformers(), routeId);
		Map<String, String[]> datasinkMapping = RouteCreatorHelper.getDataSinkMapping(
				routesConfiguration, routeConfig.getDatasinkMappingConfiguration(), routeId);

		SourceCache cache = new SourceCache();
		String collectUri = "direct:policy-" + routeId;

		for (int i = 0; i < routeConfig.getDatasources().size(); i++) {
			String endpointId = routeConfig.getDatasources().get(i);
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			String sourceId = resolveSourceId(endpointId);
			if (config != null && "sync".equalsIgnoreCase(config.getDelivery())) {
				// sync source: a dedicated scheduler polls the consumer at
				// its declared cadence and feeds the same cache
				long periodMs = config.getPollingIntervalMs() > 0
						? config.getPollingIntervalMs()
						: DEFAULT_SYNC_POLL_MS;
				LOGGER.info("route {}: sync source {} polled every {} ms", routeId, sourceId, periodMs);
				routeBuilder
						.from("timer:" + timerName(routeId, endpointId) + "?period=" + periodMs)
						.routeId(routeId + "-poll-" + endpointId)
						// pollEnrich retains the original exchange when a read returns
						// null; the timer body must never enter the source cache.
						.setBody(org.apache.camel.builder.Builder.constant(null))
						.pollEnrich(dataSourceEndpoints[i], SYNC_POLL_TIMEOUT_MS)
						.filter(exchange -> exchange.getMessage().getBody() != null)
						.setHeader(SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceId))
						.process(disassembleProcessor(endpointId, sourceId, cache))
						.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(SOURCE_CONTRIBUTION)))
						.to(collectUri);
			} else {
				// async source: the consumer feeds the cache and drives an
				// evaluation on every arrival
				ProcessorDefinition<?> sourceRoute = routeBuilder
						.from(routesConfiguration.getDatasources().get(endpointId).getConnectionURI())
						.routeId(routeId + "-source-" + endpointId)
						;
				if (config != null && "aas-element-event".equals(config.getSourceKind())) {
					sourceRoute = sourceRoute.process(new AasPropertyEventProcessor(config))
							.filter(exchange -> Boolean.TRUE.equals(
									exchange.getProperty(AasPropertyEventProcessor.MATCH_PROPERTY)));
				}
				sourceRoute
						.setHeader(SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceId))
						.process(disassembleProcessor(endpointId, sourceId, cache))
						.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(SOURCE_CONTRIBUTION)))
						.to(collectUri);
			}
		}

		ProcessorDefinition<?> cycle = routeBuilder.from(collectUri)
				.routeId(routeId)
				.filter(gateProcessor(routeConfig, cache))
				.process(mergeProcessor(cache));

		FanOut.apply(cycle, routesConfiguration, routeConfig,
				dataTransformerEndpoints, dataSinkEndpoints, datasinkMapping, routeId);
	}

	/** Bounded poll cadence for sync sources that declare none. */
	static final long DEFAULT_SYNC_POLL_MS = Long.getLong("dmp.policy.defaultSyncPollMs", 5000L);

	/** Per-source poll ceiling for one sync read. */
	static final long SYNC_POLL_TIMEOUT_MS = Long.getLong("dmp.policy.syncPollTimeoutMs", 5000L);
	private static final String SOURCE_CONTRIBUTION = "dmp.sourceContribution";

	private static String timerName(String routeId, String endpointId) {
		return ("policy-" + routeId + "-" + endpointId).replaceAll("[^A-Za-z0-9_.-]", "-");
	}

	/**
	 * The availability gate: evaluation proceeds only when EVERY source of
	 * the mapping has delivered at least once (a partial sources map is
	 * never emitted). The gate is a Camel {@link org.apache.camel.Predicate}:
	 * {@code false} silently drops the evaluation.
	 */
	private org.apache.camel.Predicate gateProcessor(RouteConfiguration routeConfig, SourceCache cache) {
		List<String> sourceIds = new ArrayList<>();
		for (String endpointId : routeConfig.getDatasources()) {
			sourceIds.add(resolveSourceId(endpointId));
		}
		return exchange -> {
			List<String> missing = new ArrayList<>();
			for (String id : sourceIds) {
				if (!cache.has(id)) {
					missing.add(id);
				}
			}
			if (missing.isEmpty()) {
				return true;
			}
			LOGGER.debug("route {}: waiting for sources {} (have {})",
					routeConfig.getRouteId(), missing, cache.ids());
			return false;
		};
	}

	private Processor disassembleProcessor(String dataSourceId, String sourceId, SourceCache cache) {
		return exchange -> {
			exchange.setProperty(SOURCE_CONTRIBUTION, false);
			Object body = exchange.getMessage().getBody();
			LOGGER.debug("source {}: raw body type={} value={}", sourceId,
					body == null ? "null" : body.getClass().getName(),
					body instanceof String ? ((String) body) : body);
			Object value = disassembleSource(exchange, dataSourceId);
			if (value == null) {
				// an empty/failed poll (e.g. nothing on the queue yet) must
				// not poison the cache with a null entry — the gate treats
				// "never delivered" and "delivered null" alike
				return;
			}
			cache.put(sourceId, value);
			exchange.setProperty(SOURCE_CONTRIBUTION, true);
		};
	}

	/**
	 * Materialises the one {@link KeyedSnapshot} of this cycle: the gate has
	 * already proven every source delivered, so this is the shared value the
	 * transform runs on. The transformer chain contract is a JSON STRING of the
	 * sources map (the Lua/JSONata processors parse it; keys are the AIMC
	 * SourceIds) — blobs return tables keyed by SinkId.
	 */
	private Processor mergeProcessor(SourceCache cache) {
		return exchange -> exchange.getMessage().setBody(
				KeyedSnapshot.of(cache.snapshot().asMap()).toJson());
	}

	/**
	 * Thread-safe latest-value cache of SourceId → payload. The fan-in
	 * counterpart of the fan-out sink snapshot: both accumulate keyed
	 * contributions and hand out exactly one immutable {@link KeyedSnapshot}.
	 */
	static final class SourceCache {
		private final ConcurrentHashMap<String, Object> latest = new ConcurrentHashMap<>();

		void put(String sourceId, Object value) {
			latest.put(sourceId, value);
		}

		boolean has(String sourceId) {
			return latest.containsKey(sourceId);
		}

		java.util.Set<String> ids() {
			return latest.keySet();
		}

		/** One immutable view of the current values, nulls preserved. */
		KeyedSnapshot snapshot() {
			Map<String, Object> values = new LinkedHashMap<>();
			latest.forEach((key, value) -> values.put(key,
					value == JsonPointer.JSON_NULL ? null : value));
			return KeyedSnapshot.of(values);
		}
	}

	/**
	 * Normalizes the payload and applies the source's {@code valuePath}
	 * (declared fine granularity, ADR-020) at the DMP boundary: JSON string
	 * payloads are parsed to structured objects so the merged {@code sources}
	 * map carries real values (never double-encoded strings), and a pointer
	 * reduces the payload to the pinned leaf before it is placed under the
	 * SourceId. Absent pointer → coarse (whole payload); a pointer on a
	 * missing path is a clear route error (SPEC §8.4 #8).
	 */
	private Object disassembleSource(Exchange exchange, String dataSourceId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(dataSourceId);
		String pointer = config == null ? null : config.getValuePath();
		Object body = exchange.getMessage().getBody();
		if (body == JsonPointer.JSON_NULL) {
			return JsonPointer.JSON_NULL;
		}
		// connector POJOs (e.g. a Milo DataValue) have no JSON meaning — run
		// them through the registered Camel type converters, which yield the
		// connector's canonical JSON form (OpcUaCanonicalConverterLoader)
		if (!(body instanceof String || body instanceof byte[] || body instanceof java.io.InputStream
				|| body instanceof java.util.Map || body instanceof java.util.List)) {
			Object converted = exchange.getContext().getTypeConverter().tryConvertTo(String.class, exchange, body);
			if (converted instanceof String s) {
				body = s;
			}
		}
		Object json = normalize(body);
		if (pointer == null || pointer.isBlank()) {
			// connector canonical forms may wrap the payload in a single-key
			// envelope keyed by the ENDPOINT id (e.g. the milo data-change
			// value) — strip it: the cached source payload is the message
			json = unwrapCanonicalEnvelope(json, dataSourceId);
			return json == JsonPointer.JSON_NULL ? JsonPointer.JSON_NULL : json;
		}

		java.util.Optional<Object> leaf = JsonPointer.extract(json, pointer);
		if (leaf.isEmpty()) {
			throw new IllegalArgumentException("Source '" + dataSourceId + "': valuePath '" + pointer
					+ "' does not resolve in the incoming payload");
		}
		Object value = leaf.get();
		return value == JsonPointer.JSON_NULL ? JsonPointer.JSON_NULL : value;
	}

	/** Strips a single-key envelope whose key is the endpoint id itself. */
	private Object unwrapCanonicalEnvelope(Object json, String dataSourceId) {
		if (json instanceof Map<?, ?> map && map.size() == 1
				&& map.containsKey(dataSourceId)) {
			return map.get(dataSourceId);
		}
		return json;
	}

	/**
	 * JSON text (or bytes) becomes a structured object; anything that is not
	 * valid JSON (plain device strings, already-structured bodies) passes
	 * through unchanged.
	 */
	private Object normalize(Object body) {
		String text = null;
		if (body instanceof String s) {
			text = s;
		} else if (body instanceof byte[] bytes) {
			text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		} else if (body instanceof java.io.InputStream stream) {
			// polled HTTP and similar connectors deliver the body as an
			// input stream — same boundary treatment as byte[] payloads
			try {
				text = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				return body;
			}
		}
		if (text == null) {
			return body;
		}
		String trimmed = text.trim();
		if (!(trimmed.startsWith("{") || trimmed.startsWith("["))) {
			return body;
		}
		try {
			// Gson's `Object.class` target parses EVERY number as Double
			// (858383 → 858383.0), corrupting integral values flowing into
			// transformers/sinks. Parse to JsonElement and materialize each
			// number by its LEXICAL form instead (integral → Integer/Long,
			// otherwise Double).
			return materializeNumbers(com.google.gson.JsonParser.parseString(trimmed));
		} catch (Exception e) {
			return body;
		}
	}

	private static Object materializeNumbers(com.google.gson.JsonElement element) {
		if (element.isJsonObject()) {
			Map<String, Object> map = new LinkedHashMap<>();
			for (Map.Entry<String, com.google.gson.JsonElement> entry : element.getAsJsonObject().entrySet()) {
				map.put(entry.getKey(), materializeNumbers(entry.getValue()));
			}
			return map;
		}
		if (element.isJsonArray()) {
			List<Object> list = new ArrayList<>();
			for (com.google.gson.JsonElement item : element.getAsJsonArray()) {
				list.add(materializeNumbers(item));
			}
			return list;
		}
		if (element.isJsonPrimitive()) {
			com.google.gson.JsonPrimitive primitive = element.getAsJsonPrimitive();
			if (primitive.isNumber()) {
				String lexical = primitive.getAsString();
				if (lexical.indexOf('.') < 0 && lexical.indexOf('e') < 0 && lexical.indexOf('E') < 0) {
					try {
						return Integer.valueOf(lexical);
					} catch (NumberFormatException integerRange) {
						try {
							return Long.valueOf(lexical);
						} catch (NumberFormatException longRange) {
							return new java.math.BigDecimal(lexical);
						}
					}
				}
				return Double.valueOf(lexical);
			}
			return primitive.isBoolean() ? Boolean.valueOf(primitive.getAsString())
					: primitive.getAsString();
		}
		return null; // JSON null
	}

	/**
	 * Resolves the AIMC SourceId for a datasource id: the source config's
	 * {@code sourceId} when set, else the datasource id itself (SPEC §6.4).
	 */
	private String resolveSourceId(String dataSourceId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(dataSourceId);
		if (config != null && config.getSourceId() != null && !config.getSourceId().isBlank()) {
			return config.getSourceId();
		}
		return dataSourceId;
	}
}
