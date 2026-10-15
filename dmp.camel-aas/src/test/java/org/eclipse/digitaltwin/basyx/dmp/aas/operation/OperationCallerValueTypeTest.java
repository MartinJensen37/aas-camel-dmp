/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class OperationCallerValueTypeTest {
	@Test
	void callerEnvelopePreservesSupportedAuthoredXsdAliases() {
		for (String type : List.of("xs:byte", "xs:short", "xs:int", "xs:integer", "xs:long",
				"xs:unsignedByte", "xs:unsignedShort", "xs:unsignedInt", "xs:unsignedLong")) {
			assertEnvelope(type, "007", "7");
		}
		for (String type : List.of("xs:float", "xs:double", "xs:decimal")) assertEnvelope(type, "1.25", "1.25");
		for (String type : List.of("xs:string", "xs:anyURI")) assertEnvelope(type, "007", "007");
		assertEnvelope("xs:boolean", "true", "true");
	}

	@Test
	void existingDescriptorJsonCategoriesKeepTheirCanonicalEnvelopeTypes() {
		assertEquals("xs:integer", OperationPayloads.basyxValueType("integer"));
		assertEquals("xs:double", OperationPayloads.basyxValueType("number"));
		assertEquals("xs:boolean", OperationPayloads.basyxValueType("boolean"));
		assertEquals("xs:string", OperationPayloads.basyxValueType("string"));
	}

	private static void assertEnvelope(String type, String raw, String expected) {
		var variables = OperationPayloads.toOperationVariables(Map.of("value", raw), Map.of("value", type));
		assertEquals(Map.of("value", Map.of("modelType", "Property", "idShort", "value", "valueType", type,
				"value", expected)), ((java.util.List<?>) org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec
				.parseJson(OperationPayloads.encodeReply(variables))).get(0), type);
	}
}
