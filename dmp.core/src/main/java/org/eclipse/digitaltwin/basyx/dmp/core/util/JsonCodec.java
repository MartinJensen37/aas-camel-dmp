package org.eclipse.digitaltwin.basyx.dmp.core.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.ToNumberPolicy;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

/**
 * JSON wire codec, and with it the single numeric contract.
 *
 * <p>Gson's default decoding widens every JSON number to {@code Double}. That
 * is lossy in both directions: an integral value re-encodes as
 * {@code 858383.0}, and anything past 2^53-1 loses precision outright. It also
 * silently breaks schemas that declare a numeric-looking field as a string
 * and the number round-trip turns it into a double.
 *
 * <p>This codec is used only at JSON wire/validation boundaries. Internal route
 * payloads remain decoded Java values. Decoding uses {@link ToNumberPolicy#LONG_OR_DOUBLE}
 * and any integral {@code Double} that
 * still arrives is narrowed back to {@code Long}, using the same 2^53-1 bound
 * and the same rule on every boundary. An integer stays an integer end to end,
 * and a quoted numeric string stays a string.
 */
public final class JsonCodec {

	/** Largest integer a Java double represents exactly: 2^53 - 1. */
	public static final double MAX_EXACT_DOUBLE_INTEGER = 9007199254740991.0;

	private static final Gson GSON = new GsonBuilder()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.serializeNulls()
			.create();

	private JsonCodec() {
	}

	/**
	 * Restores the integer/floating distinction that JSON parsing collapses.
	 * Only exactly-representable integral doubles are narrowed; genuine
	 * fractions, and anything past 2^53-1, are left alone.
	 */
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

	/** Parses a payload that its connector has identified as JSON. */
	public static Object parseJson(String text) {
		if (text == null) return null;
		try (JsonReader reader = new JsonReader(new StringReader(text))) {
			reader.setLenient(false);
			// Gson.fromJson temporarily enables leniency even on a strict reader.
			Object value = GSON.getAdapter(Object.class).read(reader);
			if (reader.peek() != JsonToken.END_DOCUMENT) {
				throw new JsonSyntaxException("JSON payload has trailing content");
			}
			return normalizeNumbers(value);
		} catch (IOException | IllegalStateException e) {
			throw new JsonSyntaxException(e);
		}
	}

	/** Converts an in-memory Gson tree to ordinary Java values without text serialization. */
	public static Object fromJsonTree(JsonElement tree) {
		return tree == null ? null : normalizeNumbers(GSON.fromJson(tree, Object.class));
	}

	/** Defensive copy for decoded payload trees retained across route stages. */
	public static Object copyValue(Object value) {
		if (value instanceof byte[] bytes) return bytes.clone();
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> copy = new LinkedHashMap<>();
			map.forEach((key, member) -> copy.put(String.valueOf(key), copyValue(member)));
			return copy;
		}
		if (value instanceof List<?> list) {
			List<Object> copy = new ArrayList<>(list.size());
			list.forEach(member -> copy.add(copyValue(member)));
			return copy;
		}
		if (value != null && value.getClass().isArray()) {
			int length = java.lang.reflect.Array.getLength(value);
			Object copy = java.lang.reflect.Array.newInstance(value.getClass().getComponentType(), length);
			for (int i = 0; i < length; i++) {
				java.lang.reflect.Array.set(copy, i, copyValue(java.lang.reflect.Array.get(value, i)));
			}
			return copy;
		}
		return value;
	}

	/** Serialises to JSON with nulls preserved and integral numbers integral. */
	public static String toJson(Object value) {
		return GSON.toJson(toJsonCompatibleValue(value));
	}

	/** Returns a defensive JSON-compatible view for validators without text encoding. */
	public static Object toJsonCompatibleValue(Object value) {
		if (value == JsonPointer.JSON_NULL) return null;
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> converted = new LinkedHashMap<>();
			map.forEach((key, member) -> converted.put(String.valueOf(key), toJsonCompatibleValue(member)));
			return converted;
		}
		if (value instanceof List<?> list) {
			List<Object> converted = new ArrayList<>(list.size());
			list.forEach(member -> converted.add(toJsonCompatibleValue(member)));
			return converted;
		}
		if (value != null && value.getClass().isArray() && !(value instanceof byte[])) {
			int length = java.lang.reflect.Array.getLength(value);
			List<Object> converted = new ArrayList<>(length);
			for (int i = 0; i < length; i++) {
				converted.add(toJsonCompatibleValue(java.lang.reflect.Array.get(value, i)));
			}
			return converted;
		}
		return value;
	}
}
