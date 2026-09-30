package org.eclipse.digitaltwin.basyx.dmp.core.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

/**
 * The DMP's single JSON boundary, and with it the single numeric contract.
 *
 * <p>Gson's default decoding widens every JSON number to {@code Double}. That
 * is lossy in both directions: an integral value re-encodes as
 * {@code 858383.0}, and anything past 2^53-1 loses precision outright. It also
 * silently breaks the operation contract, where a device echoes a Uuid the
 * declared type {@code xs:string} and the number round-trip turns it into a
 * double.
 *
 * <p>So every path that reads or writes route payload JSON - fan-in, fan-out,
 * and the operation request/reply legs - goes through here. Decoding uses
 * {@link ToNumberPolicy#LONG_OR_DOUBLE} and any integral {@code Double} that
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

	/** The configured, number-preserving Gson. */
	public static Gson gson() {
		return GSON;
	}

	/**
	 * Restores the integer/floating distinction that JSON parsing collapses.
	 * Only exactly-representable integral doubles are narrowed; genuine
	 * fractions, and anything past 2^53-1, are left alone.
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

	/**
	 * Whether the text is a JSON document rather than a plain device payload.
	 *
	 * <p>Decided from the text, NOT from whether Gson parses it: Gson is lenient
	 * and happily reads the bare word {@code IDLE} as a JSON string, which would
	 * make every plain payload look structured and get it quoted on the way out.
	 * A JSON document starts with an object/array, a quoted string, a number, or
	 * one of the literals.
	 */
	public static boolean looksLikeJson(String text) {
		if (text == null) {
			return false;
		}
		int i = 0;
		while (i < text.length() && Character.isWhitespace(text.charAt(i))) {
			i++;
		}
		if (i >= text.length()) {
			return false;
		}
		char c = text.charAt(i);
		if (c == '{' || c == '[' || c == '"' || c == '-' || (c >= '0' && c <= '9')) {
			return true;
		}
		String rest = text.substring(i);
		return "true".equals(rest) || "false".equals(rest) || "null".equals(rest);
	}

	/** Parses JSON text; returns it unchanged when it is not JSON. */
	public static Object parse(String text) {
		if (text == null) {
			return null;
		}
		try {
			return normalizeNumbers(GSON.fromJson(text, Object.class));
		} catch (Exception e) {
			return text;
		}
	}

	/** Serialises to JSON with nulls preserved and integral numbers integral. */
	public static String toJson(Object value) {
		return GSON.toJson(value);
	}
}
