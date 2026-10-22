/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class SchemaCompatibilityTest {
	private static final Map<String, Object> DEVICE = Map.of("type", "object", "properties", Map.of(
			"Name", Map.of("type", "string"),
			"Speed", Map.of("type", "number"),
			"Mode", Map.of("type", "integer"),
			"Steps", Map.of("type", "array", "items", Map.of("type", "integer")),
			"Limits", Map.of("type", "object", "properties", Map.of("Low", Map.of("type", "number")))));

	@Test
	void anAidDescribingPartOfTheDeviceWithCompatibleTypesAgrees() {
		assertEquals(List.of(), SchemaCompatibility.differences(Map.of("type", "object", "properties", Map.of(
				"Name", Map.of("type", "string"), "Mode", Map.of("type", "number"),
				"Steps", Map.of("type", "array", "items", Map.of("type", "integer")),
				"Limits", Map.of("properties", Map.of("Low", Map.of())))), DEVICE));
		assertEquals(List.of(), SchemaCompatibility.differences(Map.of(), DEVICE), "an AID without a type checks nothing");
	}

	@Test
	void missingFieldsAndIncompatibleTypesAreReportedByPath() {
		assertEquals(List.of("Colour: the device has no such field", "Speed: the AID says integer, the device number",
				"Limits/High: the device has no such field", "Steps/items: the AID says string, the device integer"),
				SchemaCompatibility.differences(aid(), DEVICE));
		assertEquals(List.of("the value: the AID says string, the device object"),
				SchemaCompatibility.differences(Map.of("type", "string"), DEVICE));
	}

	@Test
	void aValuePathSelectsTheDeviceMember() {
		assertEquals(Map.of("type", "number"), SchemaCompatibility.member(DEVICE, List.of("Limits", "Low")));
		assertEquals(Map.of("type", "integer"), SchemaCompatibility.member(DEVICE, List.of("Steps", "0")));
		assertNull(SchemaCompatibility.member(DEVICE, List.of("Colour")));
	}

	private static Map<String, Object> aid() {
		var fields = new java.util.LinkedHashMap<String, Object>();
		fields.put("Colour", Map.of("type", "string"));
		fields.put("Speed", Map.of("type", "integer"));
		fields.put("Limits", Map.of("type", "object", "properties", Map.of("High", Map.of("type", "number"))));
		fields.put("Steps", Map.of("type", "array", "items", Map.of("type", "string")));
		return Map.of("type", "object", "properties", fields);
	}
}
