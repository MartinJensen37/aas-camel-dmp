package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An immutable, id-keyed payload — the single unit that flows between the two
 * halves of a mapping route.
 *
 * <p>It is deliberately shared by BOTH directions so the route reads as one
 * symmetric pipeline:
 *
 * <pre>
 *   fan-in   N source contributions ──▶ {@code KeyedSnapshot} keyed by SourceId
 *            (one parse per source, one merge, gate on completeness)      ──▶ transform
 *   fan-out  transform result         ──▶ {@code KeyedSnapshot} keyed by SinkId
 *            (one parse of the shared result, then N independent payloads)
 * </pre>
 *
 * <p>Both halves therefore obey the same three rules: the shared value is
 * materialised exactly once, it is never mutated afterwards, and every
 * per-entity payload is derived from that one immutable instance. A sink can
 * never observe a sibling's output, and a source can never observe another's.
 *
 * <p>The keyed view is optional: a transform may legitimately return a scalar
 * or an array (a single coarse sink), so {@link #root()} carries the whole
 * parsed value and the id lookups only apply when the root is an object.
 */
public final class KeyedSnapshot {

	/**
	 * Decoding uses LONG_OR_DOUBLE so an integral JSON number stays a Long
	 * instead of being widened to a double (which would both render it as
	 * 858383.0 and silently lose precision past 2^53).
	 */
	private static final com.google.gson.Gson GSON =
			new com.google.gson.GsonBuilder()
					.setObjectToNumberStrategy(com.google.gson.ToNumberPolicy.LONG_OR_DOUBLE)
					.serializeNulls()
					.create();

	private final Object root;

	private KeyedSnapshot(Object root) {
		this.root = root;
	}

	/** A snapshot over an already-structured object (a Map is kept as-is). */
	public static KeyedSnapshot of(Object root) {
		return new KeyedSnapshot(root);
	}

	/** The empty keyed snapshot. */
	public static KeyedSnapshot empty() {
		return new KeyedSnapshot(new LinkedHashMap<String, Object>());
	}

	/**
	 * Parses a route payload into a snapshot. JSON text/bytes are parsed to a
	 * structured value; an already-structured body (Map/List/connector POJO) is
	 * taken as-is. A JSON scalar or array is a valid, non-keyed snapshot.
	 */
	public static KeyedSnapshot parse(Object body) {
		if (body == null) {
			return empty();
		}
		return new KeyedSnapshot(toStructured(body));
	}

	/** Largest integer a Java double represents exactly: 2^53 - 1. */
	private static final double MAX_EXACT_DOUBLE_INTEGER = 9007199254740991.0;

	/**
	 * Restores the integer/floating distinction that JSON parsing collapses.
	 *
	 * <p>Gson decodes every JSON number to a {@code Double}, so an integral
	 * value (a PackML property, an operation Uuid, a device counter) re-encodes
	 * as {@code 858383.0}. Both transformation engines then inherit the damage:
	 * jsonata echoes the Double it is handed, and LuaJ stores every number as a
	 * double too. This is the same rule the Lua boundary applies on the way out
	 * ({@code LuaJsonCodec.fromLua}) — one rule, enforced on both sides of the
	 * transformation, so an integer stays an integer end to end.
	 *
	 * <p>Only exactly-representable integral doubles are narrowed; genuine
	 * fractions and anything past 2^53-1 are left alone.
	 */
	@SuppressWarnings("unchecked")
	public static Object normalizeNumbers(Object value) {
		if (value instanceof Double d) {
			if (Double.isFinite(d) && d == Math.rint(d) && Math.abs(d) <= MAX_EXACT_DOUBLE_INTEGER) {
				return (long) d.doubleValue();
			}
			return d;
		}
		if (value instanceof Float f) {
			return normalizeNumbers(f.doubleValue());
		}
		if (value instanceof List<?> list) {
			List<Object> normalized = new java.util.ArrayList<>(list.size());
			for (Object element : list) {
				normalized.add(normalizeNumbers(element));
			}
			return normalized;
		}
		if (value instanceof Map<?, ?> map) {
			Map<Object, Object> normalized = new LinkedHashMap<>();
			map.forEach((key, element) -> normalized.put(key, normalizeNumbers(element)));
			return normalized;
		}
		return value;
	}

	/** JSON text/bytes become a structured object; anything else passes through. */
	public static Object toStructured(Object body) {
		String text = null;
		if (body instanceof String s) {
			text = s;
		} else if (body instanceof byte[] bytes) {
			text = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		} else if (body instanceof java.io.InputStream stream) {
			try {
				text = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				return body;
			}
		}
		if (text == null) {
			return body;
		}
		try {
			return normalizeNumbers(GSON.fromJson(text, Object.class));
		} catch (Exception e) {
			// a plain (non-JSON) device payload is a legitimate value
			return text;
		}
	}

	/** The whole parsed value, whether keyed or not. */
	public Object root() {
		return root;
	}

	/** True when the value is a JSON object and therefore addressable by id. */
	public boolean isKeyed() {
		return root instanceof Map;
	}

	/** The value stored under {@code id}, or {@code null} when absent. */
	public Object value(String id) {
		if (!(root instanceof Map<?, ?> map)) {
			return null;
		}
		return map.get(id);
	}

	/** Whether {@code id} is present — a stored {@code null} counts as present. */
	public boolean has(String id) {
		return root instanceof Map<?, ?> map && map.containsKey(id);
	}

	@SuppressWarnings("unchecked")
	public Set<String> ids() {
		return root instanceof Map<?, ?> map
				? Collections.unmodifiableSet(((Map<String, Object>) map).keySet())
				: Collections.emptySet();
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> asMap() {
		return root instanceof Map<?, ?> map
				? Collections.unmodifiableMap((Map<String, Object>) map)
				: Collections.emptyMap();
	}

	/**
	 * Serialises back to the JSON string the transformer chain and the protocol
	 * components exchange. Nulls are preserved: a delivered JSON {@code null}
	 * must stay a key in the map rather than being dropped.
	 */
	public String toJson() {
		return GSON.toJson(root);
	}

	/** A defensive, ordered copy of the keyed values (empty when not keyed). */
	public Map<String, Object> copyValues() {
		return new LinkedHashMap<>(asMap());
	}

	/** Convenience for diagnostics. */
	public List<String> idList() {
		return List.copyOf(ids());
	}

	@Override
	public String toString() {
		return isKeyed() ? "KeyedSnapshot" + ids() : "KeyedSnapshot[" + root + "]";
	}
}
