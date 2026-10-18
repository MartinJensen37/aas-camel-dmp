package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SourceEventAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;

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
 * <p>The difference is the direction of completion: fan-in waits for every
 * required source before it can emit (a partial sources map is never produced),
 * but a contribution only drives evaluation when its SourceId is selected by
 * the route's execution semantics (or legacy any-source behavior applies).
 */
public final class FanIn implements ManagedRouteResource {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanIn.class);
	public static final String SOURCE_ID_HEADER = "AIMC_SourceId";

	/** Exchange property flagging a source that actually contributed a value. */
	public static final String CONTRIBUTION_PROPERTY = SourceEventAdapter.CONTRIBUTION_PROPERTY;
	private static final String ACCEPTED_SNAPSHOT_PROPERTY = FanIn.class.getName() + ".acceptedSnapshot";

	private final RoutesConfiguration routesConfiguration;
	private final Map<String, String> sourceIdByEndpoint = new LinkedHashMap<>();
	private final SourceCache cache = new SourceCache();
	private final Map<String, AffordanceValidation> validators = new LinkedHashMap<>();

	@Override
	public void bind(org.apache.camel.CamelContext context) { }

	@Override
	public void close() { cache.clear(); }

	public FanIn(RouteConfiguration routeConfig, RoutesConfiguration routesConfiguration) {
		this(routeConfig, routesConfiguration, AffordanceValidation.configuredMode());
	}

	FanIn(RouteConfiguration routeConfig, RoutesConfiguration routesConfiguration, AffordanceValidation.Mode mode) {
		this.routesConfiguration = routesConfiguration;
		for (String endpointId : routeConfig.getDatasources()) {
			sourceIdByEndpoint.put(endpointId, resolveSourceId(routesConfiguration, endpointId));
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			validators.put(endpointId, new AffordanceValidation(
					config == null ? null : config.getSchema(), "source " + endpointId, mode));
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
			if (!validators.get(dataSourceId).incoming(
					value == JsonPointer.JSON_NULL ? null : value)) {
				return;
			}
		exchange.setProperty(ACCEPTED_SNAPSHOT_PROPERTY, cache.acceptAndSnapshot(sourceId, value));
			exchange.setProperty(CONTRIBUTION_PROPERTY, true);
		};
	}

	/** Route filter: only a contributing exchange is worth collecting. */
	public boolean contributed(Exchange exchange) {
		return Boolean.TRUE.equals(exchange.getProperty(CONTRIBUTION_PROPERTY));
	}

	/** True when this contribution is selected to trigger evaluation. */
	public boolean selectedTrigger(Exchange exchange, List<String> selectedSourceIds) {
		if (selectedSourceIds == null) return true;
		Object sourceId = exchange.getMessage().getHeader(SOURCE_ID_HEADER);
		return sourceId != null && selectedSourceIds.contains(String.valueOf(sourceId));
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
			KeyedSnapshot accepted = exchange.getProperty(ACCEPTED_SNAPSHOT_PROPERTY, KeyedSnapshot.class);
			if (accepted == null) return false;
			List<String> missing = new ArrayList<>();
			for (String id : required) {
				if (!accepted.has(id)) {
					missing.add(id);
				}
			}
			if (missing.isEmpty()) {
				return true;
			}
			LOGGER.debug("fan-in: waiting for sources {} (have {})", missing, accepted.ids());
			return false;
		};
	}

	/**
	 * Materialises a decoded, keyed value tree for the transformer chain. Keys
	 * are AIMC SourceIds; Lua returns values keyed by SinkId.
	 */
	public Processor merge() {
		return exchange -> {
			KeyedSnapshot accepted = exchange.getProperty(ACCEPTED_SNAPSHOT_PROPERTY, KeyedSnapshot.class);
			if (accepted == null) throw new IllegalStateException("fan-in exchange has no accepted source snapshot");
			exchange.getMessage().setBody(accepted.copyValues());
		};
	}

	/**
	 * Decodes incoming text/bytes only when content type or the declared JSON
	 * schema identifies a JSON payload, then applies the source's {@code valuePath}.
	 * An absent pointer keeps the whole typed value; a pointer on a missing path
	 * is a clear route error (SPEC §8.4 #8).
	 */
	private Object disassemble(Exchange exchange, String dataSourceId) {
		DataSourceConfiguration config = routesConfiguration.getDatasources().get(dataSourceId);
		String pointer = config == null ? null : config.getValuePath();
		Object body = exchange.getMessage().getBody();
		if (body == JsonPointer.JSON_NULL) {
			return JsonPointer.JSON_NULL;
		}
		// Connector POJOs have no JSON meaning — run them through registered
		// Camel type converters, which yield the connector's canonical form.
		if (!Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY))
				&& !(body instanceof String || body instanceof Number || body instanceof Boolean
				|| body instanceof byte[] || body instanceof java.io.InputStream
				|| body instanceof org.apache.camel.StreamCache
				|| body instanceof java.util.Map || body instanceof java.util.List)) {
			Object converted = exchange.getContext().getTypeConverter().tryConvertTo(String.class, exchange, body);
			if (converted instanceof String s) {
				body = s;
			}
		}
		Object json = decodeIncoming(exchange, config, body);
		checkDeviceSchema(exchange, config, dataSourceId, pointer);
		if (pointer == null || pointer.isBlank()) {
			// Connector canonical forms may wrap the payload in a single-key
			// envelope keyed by the endpoint id. Strip it: the cached source
			// payload is the message value.
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

	/**
	 * The AID describes the device: what the source's AID schema says about its value must exist
	 * on the device with a compatible type, as the connector declares it. A source the AID
	 * misdescribes contributes nothing; each value fails with the differences.
	 */
	private static void checkDeviceSchema(Exchange exchange, DataSourceConfiguration config, String dataSourceId, String pointer) {
		Object device = exchange.getMessage().getHeader(SourceEventAdapter.DEVICE_SCHEMA_HEADER);
		if (device == null || config == null || config.getSchema() == null) return;
		Object member = pointer == null || pointer.isBlank() ? device
				: org.eclipse.digitaltwin.basyx.dmp.core.util.SchemaCompatibility.member(device, JsonPointer.parse(pointer).tokens());
		if (member == null)
			throw new IllegalArgumentException("Source '" + dataSourceId + "': the AID references " + pointer
					+ ", which the device's type does not have");
		java.util.List<String> differences = org.eclipse.digitaltwin.basyx.dmp.core.util.SchemaCompatibility.differences(config.getSchema(), member);
		if (!differences.isEmpty())
			throw new IllegalArgumentException("Source '" + dataSourceId + "': the AID schema"
					+ (pointer == null || pointer.isBlank() ? "" : " of " + pointer) + " does not match the device's type: "
					+ String.join("; ", differences));
	}

	/** Strips a single-key envelope whose key is the endpoint id itself. */
	private static Object unwrapCanonicalEnvelope(Object json, String dataSourceId) {
		if (json instanceof Map<?, ?> map && map.size() == 1
				&& map.containsKey(dataSourceId)) {
			return map.get(dataSourceId);
		}
		return json;
	}

	private Object decodeIncoming(Exchange exchange, DataSourceConfiguration config, Object body) {
		if (Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY))) {
			return JsonCodec.copyValue(body);
		}
		if (body instanceof Map<?, ?> || body instanceof List<?> || body == JsonPointer.JSON_NULL) {
			return JsonCodec.copyValue(body);
		}
		String text = null;
		if (body instanceof String value) {
			text = value;
		} else if (body instanceof org.apache.camel.StreamCache streamCache) {
			try {
				java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
				streamCache.reset();
				streamCache.writeTo(bytes);
				if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return bytes.toByteArray();
				text = bytes.toString(java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source stream cache", e);
			}
		} else if (body instanceof byte[] bytes) {
			if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return bytes.clone();
			text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		} else if (body instanceof java.io.InputStream stream) {
			try {
				byte[] bytes = stream.readAllBytes();
				if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return bytes;
				text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read incoming source payload", e);
			}
		}
		if (text == null) {
			return JsonCodec.copyValue(body);
		}
		if (!jsonContent(exchange) && !jsonSchema(config.getSchema())) return text;
		Object decoded = JsonCodec.parseJson(text);
		return decoded == null ? JsonPointer.JSON_NULL : decoded;
	}

	private static boolean jsonContent(Exchange exchange) {
		String contentType = exchange.getMessage().getHeader(Exchange.CONTENT_TYPE, String.class);
		if (contentType == null) return false;
		String mediaType = contentType.split(";", 2)[0].trim().toLowerCase(java.util.Locale.ROOT);
		return "application/json".equals(mediaType) || mediaType.endsWith("+json");
	}

	private static boolean jsonSchema(Object rawSchema) {
		if (!(rawSchema instanceof Map<?, ?> schema)) return false;
		Object type = schema.get("type");
		if ("object".equals(type) || "array".equals(type)) return true;
		return schema.containsKey("properties") || schema.containsKey("items");
	}

	/**
	 * Thread-safe latest-value cache of SourceId → payload. The fan-in
	 * counterpart of the fan-out sink snapshot: both accumulate keyed
	 * contributions and hand out exactly one immutable {@link KeyedSnapshot}.
	 */
	static final class SourceCache {
		private final Map<String, Object> latest = new LinkedHashMap<>();

		synchronized KeyedSnapshot acceptAndSnapshot(String sourceId, Object value) {
			latest.put(sourceId, value == null ? JsonPointer.JSON_NULL : JsonCodec.copyValue(value));
			Map<String, Object> values = new LinkedHashMap<>();
			latest.forEach((key, cachedValue) -> values.put(key,
					cachedValue == JsonPointer.JSON_NULL ? null : cachedValue));
			return KeyedSnapshot.of(values);
		}

		synchronized void clear() { latest.clear(); }
	}
}
