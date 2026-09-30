package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

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


	private final Object root;
	private final boolean structured;

	private KeyedSnapshot(Object root, boolean structured) {
		this.root = root;
		this.structured = structured;
	}

	/** A snapshot over an already-structured object (a Map is kept as-is). */
	public static KeyedSnapshot of(Object root) {
		return new KeyedSnapshot(root, true);
	}

	/** The empty keyed snapshot. */
	public static KeyedSnapshot empty() {
		return new KeyedSnapshot(new LinkedHashMap<String, Object>(), true);
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
		String text = asText(body);
		if (text == null) {
			return new KeyedSnapshot(body, true);
		}
		// remember whether this really was JSON: a JSON string ("103.55") and a
		// plain device payload (IDLE) both parse to a Java String, but only the
		// first must be re-serialised - otherwise the JSON quoting a transform
		// deliberately produced is lost on the way out.
		if (!JsonCodec.looksLikeJson(text)) {
			return new KeyedSnapshot(text, false);
		}
		try {
			return new KeyedSnapshot(JsonCodec.normalizeNumbers(JsonCodec.gson().fromJson(text, Object.class)), true);
		} catch (Exception e) {
			return new KeyedSnapshot(text, false);
		}
	}

	/** True when the payload was JSON and therefore has to be re-serialised. */
	public boolean isStructured() {
		return structured;
	}

	private static String asText(Object body) {
		if (body instanceof String s) {
			return s;
		}
		if (body instanceof byte[] bytes) {
			return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
		}
		if (body instanceof java.io.InputStream stream) {
			try {
				return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
			} catch (java.io.IOException e) {
				return null;
			}
		}
		return null;
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
		// A plain (non-JSON) device payload is a legitimate value; JsonCodec
		// hands it back unchanged.
		return JsonCodec.parse(text);
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
		return JsonCodec.toJson(root);
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
