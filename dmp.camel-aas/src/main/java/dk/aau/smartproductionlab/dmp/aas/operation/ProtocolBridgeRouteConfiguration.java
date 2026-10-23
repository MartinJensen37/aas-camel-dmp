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
package dk.aau.smartproductionlab.dmp.aas.operation;

import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;

/**
 * AAS Operation invocation route. All interaction specifics ride in {@code triggerData}: the caller's
 * path ({@code restPath}, served with POST), {@code responseTransformer}, the action name and opaque
 * target connector options, exactly what the compiler emits; nothing is matched by name.
 */
public class ProtocolBridgeRouteConfiguration extends RouteConfiguration {

	/** Fan-in: AAS elements read at invocation, each {@code sourceId} and {@code dataSourceId}. */
	public static final String FAN_IN_SOURCES = "fanInSources";
	public static final String ROUTE_TRIGGER = "bridge";
	public static final String REST_PATH = "restPath";
	public static final String RESPONSE_TRANSFORMER = RouteConfiguration.RESPONSE_TRANSFORMER;
	/** DECLARED command parameter types (AID input DataSchema: name → JSON type). */
	public static final String PARAMETER_TYPES = "parameterTypes";
	/** DECLARED reply field types (AID output DataSchema: name → JSON type). */
	public static final String RESPONSE_TYPES = "responseTypes";
	public static final String CALLER_RESPONSE_TYPES = "callerResponseTypes";
	public static final String CALLER_INPUT_TEMPLATES = "callerInputTemplates";
	public static final String OPERATION_SUBMODEL_ID = "operationSubmodelId";
	public static final String OPERATION_PATH = "operationPath";
	public static final String CALLER_OUTPUT_TEMPLATES = "callerOutputTemplates";
	public static final String CALLER_INPUT_SOURCE_PATHS = "callerInputSourcePaths";
	public static final String CALLER_REPLY_BINDINGS = "callerReplyBindings";
	public static final String CALLER_REPLY_SINK_ID = "callerReplySinkId";
	public static final String COMMAND_SINK_IDS = "commandSinkIds";
	public static final String COMMAND_SINK_BINDINGS = "commandSinkBindings";
	public static final String RESPONSE_SCHEMA = "responseSchema";
	public static final String RESPONSE_SOURCE_BINDINGS = RouteConfiguration.RESPONSE_SOURCE_BINDINGS;
	public static final String RESPONSE_SINK_BINDINGS = RouteConfiguration.RESPONSE_SINK_BINDINGS;
	public static final String EXPLICIT_RESPONSE_MAPPING = "explicitResponseMapping";

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

	public String getAction() {
		Object value = getTriggerData().get("action");
		return value == null ? null : String.valueOf(value);
	}

	/** DECLARED command parameter types (name → JSON-schema type); never null. */
	public java.util.Map<String, String> getParameterTypes() {
		return typeMap(PARAMETER_TYPES);
	}

	/** DECLARED reply field types (name → JSON-schema type); never null. */
	public java.util.Map<String, String> getResponseTypes() {
		return typeMap(RESPONSE_TYPES);
	}

	/** The Operation's identity: submodel ID, idShort path, REST path, skill and AAS. */
	public java.util.Map<String, String> getOperationIdentity() {
		java.util.Map<String, String> tags = new java.util.LinkedHashMap<>();
		tags.put("submodel", stringValue(OPERATION_SUBMODEL_ID));
		tags.put("path", stringValue(OPERATION_PATH));
		tags.put("skill", stringValue("action"));
		String restPath = getRestPath();
		tags.put("restPath", restPath);
		if (restPath != null && restPath.matches("/operations/[^/]+/[^/]+"))
			tags.put("aas", java.net.URLDecoder.decode(restPath.split("/")[2], java.nio.charset.StandardCharsets.UTF_8));
		tags.values().removeIf(java.util.Objects::isNull);
		return tags;
	}

	/** Declared caller input and InOut variables (AAS JSON by idShort). */
	public java.util.Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> getCallerInputTemplates() {
		return templates(CALLER_INPUT_TEMPLATES);
	}

	/** Declared caller InOut and output variables (AAS JSON by idShort). */
	public java.util.Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> getCallerOutputTemplates() {
		return templates(CALLER_OUTPUT_TEMPLATES);
	}

	public java.util.Map<String, String> getCallerInputSourcePaths() { return stringMap(CALLER_INPUT_SOURCE_PATHS); }

	public java.util.Map<String, String> getCallerReplyBindings() { return stringMap(CALLER_REPLY_BINDINGS); }

	private java.util.Map<String, String> stringMap(String key) {
		if (!(getTriggerData().get(key) instanceof java.util.Map<?, ?> map)) return java.util.Map.of();
		java.util.Map<String, String> result = new java.util.LinkedHashMap<>();
		map.forEach((name, value) -> result.put(String.valueOf(name), value == null ? null : String.valueOf(value)));
		return java.util.Collections.unmodifiableMap(result);
	}

	private java.util.Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> templates(String key) {
		if (!(getTriggerData().get(key) instanceof java.util.Map<?, ?> map)) return java.util.Map.of();
		java.util.Map<String, org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement> out = new java.util.LinkedHashMap<>();
		map.forEach((name, json) -> out.put(String.valueOf(name), dk.aau.smartproductionlab.dmp.aas.model.Aas.read(
				String.valueOf(json), org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement.class)));
		return out;
	}

	public java.util.Map<String, String> getCallerResponseTypes() {
		return typeMap(CALLER_RESPONSE_TYPES);
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

	public java.util.List<String> getCallerOutputOrder() {
		return stringList("callerOutputOrder");
	}

	public String getCallerReplySinkId() {
		return stringValue(CALLER_REPLY_SINK_ID);
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

	/** AAS elements read at invocation and passed to the request transformation (fan-in). */
	public java.util.List<java.util.Map<String, Object>> getFanInSources() {
		return mapList(FAN_IN_SOURCES);
	}
}
