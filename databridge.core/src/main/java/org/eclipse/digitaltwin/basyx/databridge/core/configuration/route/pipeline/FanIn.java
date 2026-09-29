package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.util.JsonPointer;

/**
 * Fan-in: the mirror image of {@link FanOut} (SPEC §6.4).
 *
 * <p>Both halves of a mapping share one shape and one vocabulary:
 *
 * <pre>
 *   fan-in    N source contributions ─▶ ONE {@link KeyedSnapshot} keyed by SourceId
 *             (disassembled at the source, gated on completeness)
 *   transform ONE evaluation of the shared snapshot
 *   fan-out   the shared result      ─▶ ONE {@link KeyedSnapshot} keyed by SinkId
 *             (parsed once)            ─▶ N independent payloads
 * </pre>
 *
 * <p>So the two obey the same three rules: the shared value is materialised
 * exactly once, it is never mutated afterwards, and every per-entity payload
 * derives from that one immutable instance. A source can never observe a
 * sibling's payload, and a sink can never observe a sibling's output.
 *
 * <p>The difference is the direction of completion: fan-in has to WAIT for
 * every source before it can emit (a partial sources map is never produced),
 * whereas fan-out already has the complete result and only has to slice it.
 * That asymmetry is the all-sources gate below.
 */
public final class FanIn {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanIn.class);

	/** Exchange property flagging a source that actually contributed a value. */
	public static final String CONTRIBUTION_PROPERTY = "dmp.sourceContribution";

	private final RoutesConfiguration routesConfiguration;
	private final Map<String, String> sourceIdByEndpoint = new LinkedHashMap<>();
	private final SourceCache cache = new SourceCache();

	public FanIn(RouteConfiguration routeConfig, RoutesConfiguration routesConfiguration) {
		this.routesConfiguration = routesConfiguration;
		for (String endpointId : routeConfig.getDatasources()) {
			sourceIdByEndpoint.put(endpointId, resolveSourceId(routesConfiguration, endpointId));
		}
	}

	/** Every SourceId this mapping requires before it may evaluate. */
	public Set<String> expectedSourceIds() {
		return sourceIdByEndpoint.values().stream().collect(java.util.stream.Collectors.toSet());
	}

	/**
	 * The raw SourceId an endpoint publishes under: its explicit
	 * {@code sourceId}, else the endpoint's own unique id.
	 */
	public static String resolveSourceId(RoutesConfiguration routesConfiguration, String endpointId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
		String sourceId = config == null ? null : config.getSourceId();
		return sourceId == null || sourceId.isBlank() ? endpointId : sourceId;
	}

	/**
	 * The per-source step: normalise the payload, apply this source's declared
	 * {@code valuePath} (fine granularity, ADR-020) at the DMP boundary, and
	 * record the value under its SourceId. The mirror of {@code FanOut}'s
	 * per-sink shaping — both are the only place an entity's payload is
	 * derived.
	 *
	 * <p>No contribution is recorded for an empty or failed read, so "never
	 * delivered" and "delivered nothing" are treated alike by the gate.
	 */
	public Processor disassemble(String dataSourceId) {
		String sourceId = sourceIdByEndpoint.get(dataSourceId);
		return exchange -> {
			exchange.setProperty(CONTRIBUTION_PROPERTY, false);
			Object body = exchange.getMessage().getBody();
			LOGGER.debug("source {}: raw body type={} value={}", sourceId,
					body == null ? "null" : body.getClass().getName(),
					body instanceof String ? ((String) body) : body);
			Object value = disassemble(exchange, dataSourceId);
			if (value == null) {
				return;
			}
			cache.put(sourceId, value);
			exchange.setProperty(CONTRIBUTION_PROPERTY, true);
		};
	}

	/** Route filter: only a contributing exchange is worth collecting. */
	public boolean contributed(Exchange exchange) {
		return Boolean.TRUE.equals(exchange.getProperty(CONTRIBUTION_PROPERTY));
	}

	/**
	 * The completeness gate, as a Camel predicate for {@code filter(...)}.
	 * Evaluation proceeds only once EVERY source of the mapping has delivered
	 * at least once; a partial sources map is never emitted. A {@code false}
	 * result silently drops this evaluation — the next arrival (or poll tick)
	 * re-runs the gate.
	 */
	public org.apache.camel.Predicate gatePredicate() {
		Set<String> required = expectedSourceIds();
		return exchange -> {
			List<String> missing = new ArrayList<>();
			for (String id : required) {
				if (!cache.has(id)) {
					missing.add(id);
				}
			}
			if (missing.isEmpty()) {
				return true;
			}
			LOGGER.debug("fan-in: waiting for sources {} (have {})", missing, cache.ids());
			return false;
		};
	}

	/**
	 * Materialises the one shared value of this cycle. The transformer chain
	 * contract is a JSON STRING of the sources map (the Lua/JSONata processors
	 * parse it; keys are the AIMC SourceIds) — blobs return tables keyed by
	 * SinkId.
	 */
	public Processor merge() {
		return exchange -> exchange.getMessage().setBody(cache.snapshot().toJson());
	}

	/**
	 * Normalises the payload and applies the source's {@code valuePath}.
	 * JSON text/bytes are parsed so the merged map carries real values (never
	 * double-encoded strings), and a pointer reduces the payload to the pinned
	 * leaf. Absent pointer → coarse (whole payload); a pointer on a missing path
	 * is a clear route error (SPEC §8.4 #8).
	 */
	private Object disassemble(Exchange exchange, String dataSourceId) {
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
		Object json = KeyedSnapshot.toStructured(body);
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
	private static Object unwrapCanonicalEnvelope(Object json, String dataSourceId) {
		if (json instanceof Map<?, ?> map && map.size() == 1
				&& map.containsKey(dataSourceId)) {
			return map.get(dataSourceId);
		}
		return json;
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

		Set<String> ids() {
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
}
