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
package org.eclipse.digitaltwin.basyx.dmp.core.operation;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 * Shared payload helpers for operation delegation (DMP v3, ADR-022): Lua
 * blobs return {@code { <SinkId>: <payload> }} (AIMC contract) — unwrap the
 * action key before building the native command or answering the caller.
 * Used by the Uuid-correlated MQTT bridge AND the synchronous protocol
 * processors (http/opcua/plc4x) alike.
 *
 * @author aas-camel-dmp
 */
public final class OperationPayloads {

	/** Exchange property marking a failed delegation (writeback skipped). */
	public static final String OPERATION_DELEGATION_FAILED = "operationDelegationFailed";

	/** Exchange property marking a BaSyx OperationVariable[] caller. */
	public static final String BASYX_DELEGATION_BODY = "basyxDelegationBody";

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationPayloads.class);
	/**
	 * The shared, number-preserving codec: a device that echoes a Uuid whose declared
	 * type is xs:string must not have it widen to a double here. Parses and
	 * serialises through JsonCodec so an integer stays integral and a quoted
	 * numeric string stays a string.
	 */
	private static final Gson GSON = JsonCodec.gson();
	private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() { }.getType();

	private OperationPayloads() {
	}

	/**
	 * Deterministic Uuid string parse: the caller's OperationVariable is
	 * declared {@code xs:string}, but REST/UI clients may pass a bare JSON
	 * number (e.g. {@code 1}) which the delegation pipeline delivers as a
	 * Double. Native stations validate {@code Uuid} against the command
	 * schema (string) and drop the message otherwise, so the value MUST be
	 * stringified before it reaches the wire — integral numbers keep their
	 * plain form ({@code 1.0 -> "1"}), blank/absent values mean "none".
	 */
	public static String uuidString(Object raw) {
		Object coerced = coerceTyped(raw, "string");
		String s = coerced == null ? null : String.valueOf(coerced);
		return (s == null || s.isBlank()) ? null : s;
	}

	/**
	 * Deterministic value parse per a DECLARED JSON-schema type (the AID
	 * action's embedded input/output DataSchema). The declared type — not a
	 * payload convention — decides the wire representation:
	 * <ul>
	 * <li>{@code string}: stringified (integral numbers keep their plain
	 * form, {@code 1.0 -> "1"});</li>
	 * <li>{@code integer}: integral value or parseable string → Long;</li>
	 * <li>{@code number}/{@code float}/{@code double}: numeric value or
	 * parseable string → Double;</li>
	 * <li>{@code boolean}: boolean or parseable string → Boolean;</li>
	 * <li>anything else (object/array/unknown): passed through unchanged.</li>
	 * </ul>
	 */
	public static Object coerceTyped(Object raw, String jsonType) {
		if (raw == null) {
			return null;
		}
		String type = jsonType == null ? "" : jsonType.trim().toLowerCase();
		switch (type) {
			case "string":
				if (raw instanceof String s) {
					return s;
				}
				if (raw instanceof Number n) {
					return numericString(n);
				}
				return String.valueOf(raw);
			case "integer":
			case "int":
			case "long":
				if (raw instanceof Number n) {
					try {
						BigInteger value = new BigInteger(numericString(n));
						return value.bitLength() < 63 ? value.longValue() : value;
					} catch (NumberFormatException ignored) {
						return raw;
					}
				}
				if (raw instanceof String s) {
					try {
						BigInteger value = new BigInteger(s.trim());
						return value.bitLength() < 63 ? value.longValue() : value;
					} catch (NumberFormatException ignored) {
						return raw;
					}
				}
				return raw;
			case "number":
			case "float":
			case "double":
				if (raw instanceof Number n) {
					return n.doubleValue();
				}
				if (raw instanceof String s) {
					try {
						return Double.parseDouble(s.trim());
					} catch (NumberFormatException ignored) {
						return raw;
					}
				}
				return raw;
			case "boolean":
			case "bool":
				if (raw instanceof Boolean b) {
					return b;
				}
				if (raw instanceof String s) {
					if (s.equalsIgnoreCase("true")) {
						return Boolean.TRUE;
					}
					if (s.equalsIgnoreCase("false")) {
						return Boolean.FALSE;
					}
				}
				return raw;
			default:
				return raw;
		}
	}

	/**
	 * Coerces every DECLARED parameter of a command map in place (the
	 * declared type decides the wire representation — see
	 * {@link #coerceTyped}). Fields without a declaration pass through.
	 */
	public static void coerceFields(Map<String, Object> command, java.util.Map<String, String> types) {
		if (command == null || types == null || types.isEmpty()) {
			return;
		}
		for (Map.Entry<String, String> e : types.entrySet()) {
			if (command.containsKey(e.getKey())) {
				command.put(e.getKey(), coerceTyped(command.get(e.getKey()), e.getValue()));
			}
		}
	}

	/**
	 * The BaSyx value type for a DECLARED JSON-schema type (reply envelope
	 * building): string → xs:string, integer → xs:integer, number →
	 * xs:double, boolean → xs:boolean; unknown → xs:string (BaSyx's trivial
	 * cast answers a string).
	 */
	public static String basyxValueType(String jsonType) {
		String type = jsonType == null ? "" : jsonType.trim().toLowerCase();
		switch (type) {
			case "integer":
			case "int":
			case "long":
				return "xs:integer";
			case "number":
			case "float":
			case "double":
				return "xs:double";
			case "boolean":
			case "bool":
				return "xs:boolean";
			default:
				return "xs:string";
		}
	}

	/**
	 * The body for a command sink. Structured commands ride as JSON —
	 * camel-http sends it with the application/json content type, and the
	 * milo-client UA Call wraps it as a single String variant (the device
	 * method parses the command blob, ADR-022 §Lua-shaped input). Scalar
	 * values (UA node / Modbus writes) stay native. camel-plc4x requires a
	 * Map body (tag → value), so plc4x keeps the map as-is.
	 */
	public static Object sinkBody(String protocol, Map<String, Object> command) {
		if ("plc4x".equalsIgnoreCase(protocol)) {
			return command;
		}
		return GSON.toJson(command);
	}

	/** The delegation caller body is a JSON array of OperationVariables. */
	public static boolean isJsonArrayBody(Object body) {
		String text = body instanceof String s ? s : String.valueOf(body);
		String trimmed = text == null ? "" : text.trim();
		return trimmed.startsWith("[") && trimmed.endsWith("]");
	}

	/** Flattens OperationVariable[] into the map consumed by request Lua. */
	public static String flattenOperationVariables(String body) {
		java.util.List<?> variables;
		try {
			variables = GSON.fromJson(body, java.util.List.class);
		} catch (RuntimeException e) {
			return body;
		}
		if (variables == null) {
			return body;
		}
		Map<String, Object> flat = new java.util.LinkedHashMap<>();
		for (Object element : variables) {
			Map<String, Object> variable = GSON.fromJson(GSON.toJson(element), MAP_TYPE);
			Object value = variable.get("value");
			if (value instanceof Map valueMap && valueMap.get("idShort") != null) {
				Object scalar = valueMap.get("value");
				flat.put(String.valueOf(valueMap.get("idShort")), scalar);
			}
		}
		return GSON.toJson(flat);
	}

	/** Maps a flat reply into the BaSyx OperationVariable[] envelope. */
	public static java.util.List<Map<String, Object>> toOperationVariables(
			Map<String, Object> map) {
		return toOperationVariables(map, java.util.Map.of());
	}

	/** Maps a flat reply into OperationVariables using declared reply types. */
	public static java.util.List<Map<String, Object>> toOperationVariables(
			Map<String, Object> map, java.util.Map<String, String> declaredTypes) {
		if (map == null) {
			return java.util.List.of();
		}
		java.util.Map<String, String> types = declaredTypes == null ? java.util.Map.of() : declaredTypes;
		java.util.List<Map<String, Object>> variables = new java.util.ArrayList<>();
		for (Map.Entry<String, Object> entry : map.entrySet()) {
			String type = types.getOrDefault(entry.getKey(), "string");
			Map<String, Object> property = new java.util.LinkedHashMap<>();
			property.put("modelType", "Property");
			property.put("idShort", entry.getKey());
			property.put("valueType", basyxValueType(type));
			Object value = coerceTyped(entry.getValue(), type);
			property.put("value", value == null ? null : String.valueOf(value));
			variables.add(Map.of("value", property));
		}
		return variables;
	}

	/**
	 * Unwrap the action (SinkId) key from a transformed Lua payload; if absent
	 * (or not JSON), fall back to the payload as-is.
	 */
	public static Map<String, Object> unwrap(Object payload, String action) {
		if (payload instanceof Map) {
			return unwrapMap(payload, action);
		}
		try {
			Map<String, Object> map = GSON.fromJson(String.valueOf(payload), MAP_TYPE);
			if (map == null) {
				return new HashMap<>();
			}
			return unwrapMap(map, action);
		} catch (JsonSyntaxException e) {
			LOGGER.warn("operation '{}': unable to parse transformed payload, "
					+ "using it as-is", action);
			return new HashMap<>();
		}
	}

	/** Unwraps only an outer action envelope while preserving scalar/array replies. */
	public static Object unwrapValue(Object payload, String action) {
		Object parsed = payload;
		if (!(parsed instanceof Map) && !(parsed instanceof java.util.List)) {
			try {
				parsed = GSON.fromJson(String.valueOf(payload), Object.class);
			} catch (JsonSyntaxException ignored) {
				return payload;
			}
		}
		if (parsed instanceof Map<?, ?> map && map.containsKey(action)) {
			return map.get(action);
		}
		return parsed;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> unwrapMap(Object payload, String action) {
		Map<String, Object> map = (Map<String, Object>) payload;
		Object inner = map.get(action);
		if (inner instanceof Map) {
			return new HashMap<>((Map<String, Object>) inner);
		}
		return new HashMap<>(map);
	}

	private static String numericString(Number number) {
		String text = number.toString();
		try {
			java.math.BigDecimal decimal = new java.math.BigDecimal(text);
			if (decimal.stripTrailingZeros().scale() <= 0) {
				return decimal.toBigIntegerExact().toString();
			}
			return decimal.toPlainString();
		} catch (NumberFormatException | ArithmeticException ignored) {
			return text;
		}
	}
}
