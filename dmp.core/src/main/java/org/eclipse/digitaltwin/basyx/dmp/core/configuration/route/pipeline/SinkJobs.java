/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns a transformer result keyed by SinkId into write jobs: members of one
 * device affordance are assembled into one payload, values and payloads are
 * validated against their declared schemas. Shared by the best-effort
 * response writes and the acknowledged writes of an operation mapping.
 */
public final class SinkJobs {
	private static final Logger LOGGER = LoggerFactory.getLogger(SinkJobs.class);
	private final String correlationId;
	private final List<SinkBinding> bindings;
	private final Map<String, AffordanceValidation> memberValidators = new LinkedHashMap<>();
	private final Map<String, AffordanceValidation> payloadValidators = new LinkedHashMap<>();
	private final Map<String, AffordanceValidation> singlePayloadValidators = new LinkedHashMap<>();

	public SinkJobs(String correlationId, List<Map<String, Object>> rawBindings) {
		this.correlationId = correlationId;
		List<SinkBinding> resolvedBindings = new ArrayList<>();
		for (Map<String, Object> raw : rawBindings) {
			SinkBinding binding = SinkBinding.from(raw);
			resolvedBindings.add(binding);
			if (binding.schema != null) {
				memberValidators.put(binding.sinkId,
						new AffordanceValidation(binding.schema, "response sink " + binding.sinkId));
			}
			if (binding.payloadGroup != null && binding.payloadSchema != null) {
				payloadValidators.putIfAbsent(binding.payloadGroup,
						new AffordanceValidation(binding.payloadSchema,
								"response payload group " + binding.payloadGroup));
			}
			if (binding.payloadGroup == null && binding.payloadSchema != null) {
				singlePayloadValidators.put(binding.sinkId,
						new AffordanceValidation(binding.payloadSchema,
								"response sink payload " + binding.sinkId));
			}
		}
		this.bindings = List.copyOf(resolvedBindings);
	}

	/**
	 * The write jobs for one transformer result keyed by SinkId. Without
	 * {@code required}, a missing SinkId or an invalid payload skips that sink
	 * (best-effort response writes); with it, they fail the whole preparation.
	 */
	public List<ResponseSinkFanOutProcessor.DispatchJob> prepare(Object parsed, boolean required) {
		if (!(parsed instanceof Map<?, ?> rawValues)) {
			throw new IllegalArgumentException("transformer returned no SinkId map");
		}
		Map<String, Object> values = new LinkedHashMap<>();
		rawValues.forEach((key, value) -> values.put(String.valueOf(key), value));
		Map<String, Group> groups = new LinkedHashMap<>();
		List<ResponseSinkFanOutProcessor.DispatchJob> jobs = new ArrayList<>();
		java.util.Set<String> incompleteGroups = new java.util.HashSet<>();
		for (SinkBinding binding : bindings) {
			if (!values.containsKey(binding.sinkId)) {
				if (required) throw new PayloadValidationException(PayloadValidationException.Stage.COMMAND,
						"transformer omitted SinkId " + binding.sinkId, List.of());
				LOGGER.warn("mapping {} response Lua omitted SinkId {}; skipping response sink",
						correlationId, binding.sinkId);
				if (binding.payloadGroup != null) {
					incompleteGroups.add(binding.payloadGroup);
				}
				continue;
			}
			String groupKey = binding.payloadGroup != null
					? "payload:" + binding.payloadGroup : "sink:" + binding.sinkId;
			groups.computeIfAbsent(groupKey, ignored -> new Group(binding.payloadGroup, binding.jsonPayload))
					.add(binding, values.get(binding.sinkId));
		}
		for (Group group : groups.values()) {
			if (group.payloadGroup != null && incompleteGroups.contains(group.payloadGroup)) {
				LOGGER.warn("mapping {} response sink group {} is incomplete; skipping group",
					correlationId, group.payloadGroup);
				continue;
			}
			ResponseSinkFanOutProcessor.DispatchJob job = dispatch(group, required);
			if (job != null) jobs.add(job);
		}
		return List.copyOf(jobs);
	}

	private ResponseSinkFanOutProcessor.DispatchJob dispatch(Group group, boolean required) {
		SinkBinding first = group.bindings.get(0);
		Object payload;
		boolean assembleMembers = group.bindings.size() > 1 || first.valuePath != null;
		try {
			if (assembleMembers) {
				Map<String, Object> keyed = new LinkedHashMap<>();
				Map<String, String> paths = new LinkedHashMap<>();
				for (SinkBinding binding : group.bindings) {
					keyed.put(binding.sinkId, group.values.get(binding.sinkId));
					if (binding.valuePath != null) paths.put(binding.sinkId, binding.valuePath);
					validateMember(binding, group.values.get(binding.sinkId));
				}
				payload = PayloadAssembler.assemble(keyed,
						group.bindings.stream().map(b -> b.sinkId).toList(), paths);
				AffordanceValidation validator = group.payloadGroup == null
						? null : payloadValidators.get(group.payloadGroup);
				if (validator != null) validator.outgoing(JsonCodec.toJsonCompatibleValue(payload));
			} else {
				payload = group.values.get(first.sinkId);
				validateMember(first, payload);
				AffordanceValidation validator = first.payloadGroup == null
						? singlePayloadValidators.get(first.sinkId)
						: payloadValidators.get(first.payloadGroup);
				if (validator != null) validator.outgoing(JsonCodec.toJsonCompatibleValue(payload));
			}
		} catch (RuntimeException e) {
			if (required) throw new PayloadValidationException(PayloadValidationException.Stage.COMMAND,
					"sink payload violates its declared schema", List.of());
			LOGGER.warn("mapping {} response sink group {} rejected before send",
				correlationId, group.payloadGroup == null ? first.sinkId : group.payloadGroup);
			return null;
		}

		String endpoint = first.endpoint;
		String sinkLabel = group.payloadGroup == null ? first.sinkId : group.payloadGroup;
		return new ResponseSinkFanOutProcessor.DispatchJob(endpoint, JsonCodec.copyValue(JsonCodec.toJsonCompatibleValue(payload)),
				sinkLabel, group.jsonPayload);
	}

	private static boolean isJsonSchema(Map<String, Object> schema) {
		if (schema == null || schema.isEmpty()) return false;
		return schema.containsKey("type") || schema.containsKey("properties") || schema.containsKey("items")
				|| schema.containsKey("const") || schema.containsKey("enum") || schema.containsKey("oneOf")
				|| schema.containsKey("anyOf") || schema.containsKey("allOf");
	}

	private void validateMember(SinkBinding binding, Object value) {
		AffordanceValidation validator = memberValidators.get(binding.sinkId);
		if (validator != null) validator.outgoing(JsonCodec.toJsonCompatibleValue(value));
	}

	private static final class Group {
		private final String payloadGroup;
		private final boolean jsonPayload;
		private final List<SinkBinding> bindings = new ArrayList<>();
		private final Map<String, Object> values = new LinkedHashMap<>();

		private Group(String payloadGroup, boolean jsonPayload) {
			this.payloadGroup = payloadGroup;
			this.jsonPayload = jsonPayload;
		}

		private void add(SinkBinding binding, Object value) {
			bindings.add(binding);
			values.put(binding.sinkId, value == null ? JsonPointer.JSON_NULL : value);
		}
	}

	private record SinkBinding(String sinkId, String endpoint, String payloadGroup,
			String valuePath, Map<String, Object> payloadSchema, Map<String, Object> schema,
			boolean jsonPayload) {
		private static SinkBinding from(Map<String, Object> raw) {
			Map<String, Object> payloadSchema = asSchema(raw.get("payloadSchema"));
			Map<String, Object> schema = asSchema(raw.get("schema"));
			boolean jsonPayload = raw.containsKey("jsonPayload")
					? Boolean.TRUE.equals(raw.get("jsonPayload"))
					: isJsonSchema(payloadSchema) || isJsonSchema(schema);
			return new SinkBinding(string(raw.get("sinkId")), string(raw.get("endpoint")),
				string(raw.get("payloadGroup")), string(raw.get("valuePath")),
						payloadSchema, schema, jsonPayload);
		}

		private static String string(Object value) {
			return value == null ? null : String.valueOf(value);
		}

		private static Map<String, Object> asSchema(Object value) {
			if (!(value instanceof Map<?, ?> map)) return null;
			Map<String, Object> result = new LinkedHashMap<>();
			map.forEach((key, schemaValue) -> result.put(String.valueOf(key), schemaValue));
			return result;
		}
	}
}
