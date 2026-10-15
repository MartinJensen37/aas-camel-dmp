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

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;

/**
 * AAS Operation invocation route. All
 * interaction specifics ride in {@code triggerData} — the DERIVED caller
 * path ({@code restPath}/{@code restMethod}, DMP v3: the descriptor derives
 * the REST surface by convention and the bridge emits the consumer from it),
 * {@code responseTransformer}, action name and opaque target connector options
 * — exactly what the descriptor emits; no name
 * sniffing anywhere.
 */
public class ProtocolBridgeRouteConfiguration extends RouteConfiguration {

	public static final String ROUTE_TRIGGER = "bridge";
	public static final String REST_PATH = "restPath";
	public static final String REST_METHOD = "restMethod";
	public static final String RESPONSE_TRANSFORMER = "responseTransformer";
	/** DECLARED command parameter types (AID input DataSchema: name → JSON type). */
	public static final String PARAMETER_TYPES = "parameterTypes";
	/** DECLARED reply field types (AID output DataSchema: name → JSON type). */
	public static final String RESPONSE_TYPES = "responseTypes";
	public static final String CALLER_RESPONSE_TYPES = "callerResponseTypes";
	/** DECLARED positional reply order (CCI operation output_variable order). */
	public static final String RESPONSE_ORDER = "responseOrder";
	public static final String OPERATION_SOURCE_ID = "operationSourceId";
	public static final String COMMAND_SINK_ID = "commandSinkId";
	public static final String CALLER_REPLY_SINK_ID = "callerReplySinkId";
	public static final String COMMAND_SINK_IDS = "commandSinkIds";
	public static final String COMMAND_SINK_BINDINGS = "commandSinkBindings";
	public static final String RESPONSE_SCHEMA = "responseSchema";
	public static final String RESPONSE_SOURCE_BINDINGS = "responseSourceBindings";
	public static final String RESPONSE_SINK_BINDINGS = "responseSinkBindings";
	public static final String EXPLICIT_RESPONSE_MAPPING = "explicitResponseMapping";
	public static final String DEFAULT_REST_METHOD = "POST";

	public ProtocolBridgeRouteConfiguration(RouteConfiguration configuration) {
		super(configuration);
	}

	@Override
	public String getRouteTrigger() {
		return ROUTE_TRIGGER;
	}

	public String getRestPath() {
		Object value = getTriggerData().get(REST_PATH);
		return value == null ? null : String.valueOf(value);
	}

	public String getRestMethod() {
		Object value = getTriggerData().get(REST_METHOD);
		return value == null ? DEFAULT_REST_METHOD : String.valueOf(value);
	}

	public String getAction() {
		Object value = getTriggerData().get("action");
		return value == null ? null : String.valueOf(value);
	}

	public String getResponseTransformer() {
		return (String) getTriggerData().get(RESPONSE_TRANSFORMER);
	}

	/** DECLARED command parameter types (name → JSON-schema type); never null. */
	public java.util.Map<String, String> getParameterTypes() {
		return typeMap(PARAMETER_TYPES);
	}

	/** DECLARED reply field types (name → JSON-schema type); never null. */
	public java.util.Map<String, String> getResponseTypes() {
		return typeMap(RESPONSE_TYPES);
	}

	/** Old standalone descriptors used the device types for both boundaries. */
	public java.util.Map<String, String> getCallerResponseTypes() {
		return getTriggerData().containsKey(CALLER_RESPONSE_TYPES)
				? typeMap(CALLER_RESPONSE_TYPES) : getResponseTypes();
	}

	public java.util.List<String> getParameterRequired() {
		return stringList("parameterRequired");
	}

	public java.util.List<String> getResponseRequired() {
		return stringList("responseRequired");
	}

	public java.util.Map<String, Object> getTargetOptions() {
		Object value = getTriggerData().get("targetOptions");
		if (!(value instanceof java.util.Map<?, ?> map)) return java.util.Map.of();
		java.util.Map<String, Object> options = new java.util.LinkedHashMap<>();
		map.forEach((key, option) -> options.put(String.valueOf(key), option));
		return java.util.Collections.unmodifiableMap(options);
	}

	private java.util.List<String> stringList(String key) {
		Object value = getTriggerData().get(key);
		if (!(value instanceof java.util.List<?> list)) return java.util.List.of();
		return list.stream().filter(java.util.Objects::nonNull).map(String::valueOf).toList();
	}

	/** Legacy `responseOrder` key used before caller and server order were separated. */
	public java.util.List<String> getResponseOrder() {
		Object value = getTriggerData().get(RESPONSE_ORDER);
		if (!(value instanceof java.util.List<?> list)) {
			return java.util.List.of();
		}
		java.util.List<String> out = new java.util.ArrayList<>();
		for (Object entry : list) {
			if (entry != null) {
				out.add(String.valueOf(entry));
			}
		}
		return out;
	}

	public java.util.List<String> getCallerOutputOrder() {
		Object value = getTriggerData().get("callerOutputOrder");
		if (!(value instanceof java.util.List<?> list)) {
			return getResponseOrder();
		}
		return list.stream().filter(java.util.Objects::nonNull).map(String::valueOf).toList();
	}

	public String getOperationSourceId() {
		return stringValue(OPERATION_SOURCE_ID);
	}

	public String getCommandSinkId() {
		return stringValue(COMMAND_SINK_ID);
	}

	public String getCallerReplySinkId() {
		String id = stringValue(CALLER_REPLY_SINK_ID);
		return id == null ? null : id;
	}

	public java.util.List<String> getCommandSinkIds() {
		Object value = getTriggerData().get(COMMAND_SINK_IDS);
		if (!(value instanceof java.util.List<?> list)) {
			return java.util.List.of();
		}
		return list.stream().filter(java.util.Objects::nonNull)
				.map(String::valueOf).toList();
	}

	public java.util.List<java.util.Map<String, Object>> getCommandSinkBindings() {
		return mapList(COMMAND_SINK_BINDINGS);
	}

	public Object getResponseSchema() {
		return getTriggerData().get(RESPONSE_SCHEMA);
	}


	public boolean isExplicitResponseMapping() {
		return Boolean.TRUE.equals(getTriggerData().get(EXPLICIT_RESPONSE_MAPPING));
	}

	public java.util.List<java.util.Map<String, Object>> getResponseSourceBindings() {
		return mapList(RESPONSE_SOURCE_BINDINGS);
	}

	public java.util.List<java.util.Map<String, Object>> getResponseSinkBindings() {
		return mapList(RESPONSE_SINK_BINDINGS);
	}

	private java.util.List<java.util.Map<String, Object>> mapList(String key) {
		Object value = getTriggerData().get(key);
		if (!(value instanceof java.util.List<?> list)) return java.util.List.of();
		java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
		for (Object item : list) {
			if (item instanceof java.util.Map<?, ?> map) {
				java.util.Map<String, Object> entry = new java.util.LinkedHashMap<>();
				map.forEach((k, v) -> entry.put(String.valueOf(k), v));
				out.add(java.util.Collections.unmodifiableMap(entry));
			}
		}
		return java.util.List.copyOf(out);
	}

	private String stringValue(String key) {
		Object value = getTriggerData().get(key);
		return value == null ? null : String.valueOf(value);
	}

	private java.util.Map<String, String> typeMap(String key) {
		Object value = getTriggerData().get(key);
		java.util.Map<String, String> out = new java.util.LinkedHashMap<>();
		if (value instanceof java.util.Map<?, ?> map) {
			for (java.util.Map.Entry<?, ?> e : map.entrySet()) {
				out.put(String.valueOf(e.getKey()),
						e.getValue() == null ? "string" : String.valueOf(e.getValue()));
			}
		}
		return out;
	}
}
