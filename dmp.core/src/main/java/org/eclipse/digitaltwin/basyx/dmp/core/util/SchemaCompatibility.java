/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Whether an AID data schema describes what the device declares for the same value (a
 * connector's device schema, e.g. from an OPC UA DataTypeDefinition). Both are JSON-schema
 * maps ({@code type}, {@code properties}, {@code items}). The AID may describe less than the
 * device (a field it leaves out is still delivered), but everything it describes must exist
 * on the device with a compatible type: {@code number} accepts {@code integer}; an absent
 * type on either side is not checked.
 */
public final class SchemaCompatibility {
	private SchemaCompatibility() {
	}

	/** The AID schema's differences from the device schema, each as {@code path: problem}; empty when they agree. */
	public static List<String> differences(Object aid, Object device) {
		List<String> differences = new ArrayList<>();
		compare(aid, device, "", differences);
		return differences;
	}

	/** The device schema of the member a source's value path selects; null when the path leaves the device schema. */
	public static Object member(Object device, List<String> tokens) {
		Object current = device;
		for (String token : tokens) {
			if (!(current instanceof Map<?, ?> schema)) return null;
			if ("array".equals(schema.get("type")) && token.chars().allMatch(Character::isDigit)) current = schema.get("items");
			else if (schema.get("properties") instanceof Map<?, ?> properties && properties.containsKey(token)) current = properties.get(token);
			else return null;
		}
		return current;
	}

	private static void compare(Object aid, Object device, String path, List<String> differences) {
		if (!(aid instanceof Map<?, ?> expected) || !(device instanceof Map<?, ?> actual)) return;
		Object aidType = expected.get("type");
		Object deviceType = actual.get("type");
		if (aidType != null && deviceType != null && !aidType.equals(deviceType)
				&& !("number".equals(aidType) && "integer".equals(deviceType))) {
			differences.add(label(path) + ": the AID says " + aidType + ", the device " + deviceType);
			return;
		}
		if (expected.get("properties") instanceof Map<?, ?> fields && "object".equals(deviceType)) {
			Map<?, ?> deviceFields = actual.get("properties") instanceof Map<?, ?> map ? map : Map.of();
			for (Map.Entry<?, ?> field : fields.entrySet()) {
				String name = String.valueOf(field.getKey());
				if (!deviceFields.containsKey(name)) differences.add(label(path + "/" + name) + ": the device has no such field");
				else compare(field.getValue(), deviceFields.get(name), path + "/" + name, differences);
			}
		}
		if (expected.get("items") != null && "array".equals(deviceType))
			compare(expected.get("items"), actual.get("items"), path + "/items", differences);
	}

	private static String label(String path) {
		return path.isEmpty() ? "the value" : path.substring(1);
	}
}
