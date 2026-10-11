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
package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Shared payload helpers for operation delegation (DMP v3, ADR-022): Lua
 * blobs return {@code { <SinkId>: <payload> }} (AIMC contract) — unwrap the
 * action key before building the named command or answering the AAS Operation
 * caller. Connector modules own transport encoding and correlation.
 *
 * @author aas-camel-dmp
 */
public final class OperationPayloads {

	/** Exchange property marking a failed delegation (writeback skipped). */
	public static final String OPERATION_DELEGATION_FAILED = "operationDelegationFailed";

	/** Exchange property marking a BaSyx OperationVariable[] caller. */
	public static final String BASYX_DELEGATION_BODY = "basyxDelegationBody";

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationPayloads.class);
	private OperationPayloads() {
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
		if (raw == JsonPointer.JSON_NULL) return raw;
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

	/** True for the BaSyx OperationVariable envelope, not arbitrary JSON arrays. */
	public static boolean isOperationVariables(Object body) {
		Object decoded = decodeOperationBody(body);
		if (!(decoded instanceof List<?> variables)) return false;
		if (variables.isEmpty()) return true;
		return variables.stream().allMatch(OperationPayloads::isOperationVariable);
	}

	/** Flattens decoded OperationVariable[] into the request value tree. */
	public static Map<String, Object> flattenOperationVariables(Object body) {
		Object decoded = decodeOperationBody(body);
		if (!(decoded instanceof List<?> variables) || !variables.isEmpty()
				&& variables.stream().anyMatch(variable -> !isOperationVariable(variable))) {
			throw new IllegalArgumentException("BaSyx Operation request is not an OperationVariable array");
		}
		Map<String, Object> flat = new java.util.LinkedHashMap<>();
		for (Object element : variables) {
			Map<?, ?> variable = (Map<?, ?>) element;
			Map<?, ?> property = (Map<?, ?>) variable.get("value");
			flat.put(String.valueOf(property.get("idShort")), JsonCodec.copyValue(property.get("value")));
		}
		return flat;
	}

	private static Object decodeOperationBody(Object body) {
		if (body instanceof String text) return JsonCodec.parseJson(text);
		if (body instanceof byte[] bytes) {
			return JsonCodec.parseJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
		}
		if (body instanceof java.io.InputStream stream) {
			try {
				return JsonCodec.parseJson(new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
			} catch (java.io.IOException e) {
				throw new IllegalArgumentException("could not read BaSyx Operation request", e);
			}
		}
		return body;
	}

	private static boolean isOperationVariable(Object value) {
		if (!(value instanceof Map<?, ?> variable) || !(variable.get("value") instanceof Map<?, ?> property)) {
			return false;
		}
		return property.get("idShort") instanceof String && property.containsKey("value");
	}

	/** Maps a flat reply into the BaSyx OperationVariable[] envelope. */
	public static java.util.List<Map<String, Object>> toOperationVariables(
			Map<String, Object> map) {
		return toOperationVariables(map, java.util.Map.of());
	}

	/** Maps a flat reply into OperationVariables using declared reply types. */
	public static java.util.List<Map<String, Object>> toOperationVariables(
			Map<String, Object> map, java.util.Map<String, String> declaredTypes) {
		return toOperationVariables(map, declaredTypes, java.util.List.of());
	}

	/** Maps a flat reply to caller OperationVariables in the CCI-declared order. */
	public static java.util.List<Map<String, Object>> toOperationVariables(
			Map<String, Object> map, java.util.Map<String, String> declaredTypes,
			java.util.List<String> callerOrder) {
		if (map == null) {
			return java.util.List.of();
		}
		java.util.Map<String, String> types = declaredTypes == null ? java.util.Map.of() : declaredTypes;
		java.util.List<Map<String, Object>> variables = new java.util.ArrayList<>();
		java.util.LinkedHashSet<String> order = new java.util.LinkedHashSet<>();
		if (callerOrder != null && !callerOrder.isEmpty()) order.addAll(callerOrder);
		else order.addAll(map.keySet());
		for (String name : order) {
			if (!map.containsKey(name)) continue;
			Object rawValue = map.get(name);
			String type = types.getOrDefault(name, "string");
			Map<String, Object> property = new java.util.LinkedHashMap<>();
			property.put("modelType", "Property");
			property.put("idShort", name);
			property.put("valueType", basyxValueType(type));
			Object value = rawValue == null || rawValue == JsonPointer.JSON_NULL
					? JsonPointer.JSON_NULL : coerceTyped(rawValue, type);
			property.put("value", value == JsonPointer.JSON_NULL ? value : String.valueOf(value));
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
		LOGGER.warn("operation '{}': Lua result is not a SinkId object", action);
		return new HashMap<>();
	}

	/** Builds one declared affordance payload from the Lua values keyed by SinkId. */
	public static Map<String, Object> assembleCommand(Object transformed,
			java.util.List<String> sinkIds, Map<String, String> valuePaths, String legacySinkId) {
		if (sinkIds == null || sinkIds.isEmpty()) return unwrap(transformed, legacySinkId);
		Map<String, Object> sinkValues = keyedValues(transformed);
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		boolean hasPath = false;
		for (String sinkId : sinkIds) {
			Object value = requireCommandSinkValue(sinkValues, sinkId);
			String path = valuePaths == null ? null : valuePaths.get(sinkId);
			if (path == null || path.isBlank()) {
				if (sinkIds.size() != 1) {
					throw new IllegalArgumentException("multiple command SinkIds require member valuePaths");
				}
				if (!(value instanceof Map<?, ?> map)) {
					throw new IllegalArgumentException("whole-affordance command must be an object");
				}
				Map<String, Object> wholePayload = new java.util.LinkedHashMap<>();
				map.forEach((key, member) -> wholePayload.put(String.valueOf(key), member));
				return wholePayload;
			}
			hasPath = true;
			putAt(result, org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.parse(path).tokens(), value);
		}
		return hasPath ? result : unwrap(transformed, legacySinkId);
	}

	/** Parses a Lua result as an authored SinkId map without guessing missing keys. */
	public static Map<String, Object> keyedValues(Object payload) {
		if (!(payload instanceof Map<?, ?> map)) {
			throw new IllegalArgumentException("request Lua did not return a SinkId object");
		}
		Map<String, Object> values = new java.util.LinkedHashMap<>();
		map.forEach((key, value) -> values.put(String.valueOf(key), value));
		return values;
	}

	public static Object requireCommandSinkValue(Object payload, String sinkId) {
		Map<String, Object> values = keyedValues(payload);
		if (!values.containsKey(sinkId)) {
			throw new IllegalArgumentException("request mapping omitted command SinkId " + sinkId);
		}
		return values.get(sinkId);
	}

	@SuppressWarnings("unchecked")
	private static void putAt(Map<String, Object> root, java.util.List<String> tokens, Object value) {
		if (tokens.isEmpty()) {
			if (!(value instanceof Map<?, ?> map)) {
				throw new IllegalArgumentException("root command contribution must be an object");
			}
			mergeInto(root, (Map<String, Object>) map);
			return;
		}
		Map<String, Object> current = root;
		for (int i = 0; i < tokens.size(); i++) {
			String token = tokens.get(i);
			if (token.startsWith("[") && token.endsWith("]")) {
				throw new IllegalArgumentException("array-index command valuePaths are not supported for grouped sends");
			}
			if (i == tokens.size() - 1) {
				if (current.containsKey(token)) mergeValue(current.get(token), value, token);
				else current.put(token, value);
			} else {
				Object child = current.computeIfAbsent(token, ignored -> new java.util.LinkedHashMap<String, Object>());
				if (!(child instanceof Map<?, ?>)) {
					throw new IllegalArgumentException("overlapping command valuePaths at " + token);
				}
				current = (Map<String, Object>) child;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static void mergeValue(Object oldValue, Object newValue, String key) {
		if (oldValue instanceof Map<?, ?> oldMap && newValue instanceof Map<?, ?> newMap) {
			mergeInto((Map<String, Object>) oldMap, (Map<String, Object>) newMap);
			return;
		}
		if (!java.util.Objects.equals(oldValue, newValue)) {
			throw new IllegalArgumentException("conflicting command values at " + key);
		}
	}

	private static void mergeInto(Map<String, Object> target, Map<String, Object> source) {
		source.forEach((key, value) -> {
			if (target.containsKey(key)) mergeValue(target.get(key), value, key);
			else target.put(key, value);
		});
	}

	/** Unwraps only an outer action envelope while preserving scalar/array replies. */
	public static Object unwrapValue(Object payload, String action) {
		if (payload instanceof Map<?, ?> map && map.containsKey(action)) {
			return map.get(action);
		}
		return payload;
	}

	/** Requires Lua to emit the authored caller-reply SinkId; never return the whole SinkId map by accident. */
	public static Object requireSinkValue(Object payload, String sinkId) {
		if (!(payload instanceof Map<?, ?> map) || !map.containsKey(sinkId)) {
			throw new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
					"Response Lua omitted caller reply SinkId " + sinkId, java.util.List.of());
		}
		return map.get(sinkId);
	}

	/** Restricts a device reply to the caller Operation's declared output variables. */
	public static Map<String, Object> selectCallerOutputs(Map<String, Object> values,
			java.util.List<String> callerOrder) {
		if (values == null || callerOrder == null || callerOrder.isEmpty()) return values;
		Map<String, Object> selected = new java.util.LinkedHashMap<>();
		for (String name : callerOrder) {
			if (values.containsKey(name)) selected.put(name, values.get(name));
		}
		return selected;
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
