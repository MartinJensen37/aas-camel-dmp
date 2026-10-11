package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadValidationException;
import org.junit.jupiter.api.Test;

class OpcUaMethodArgumentsTest {
	@Test
	void mapsNamedLuaPayloadToServerArgumentOrder() {
		var signature = signature(
				List.of(argument("Speed", "i=6", -1), argument("Uuid", "i=12", -1)),
				List.of(argument("Outcome", "i=1", -1), argument("State", "i=12", -1)));
		assertEquals(List.of(2L, "asset-7"), OpcUaMethodArguments.toPositional(
				Map.of("Uuid", "asset-7", "Speed", 2L), signature));
		assertEquals(Map.of("Outcome", true, "State", "READY"),
				OpcUaMethodArguments.fromPositional(List.of(true, "READY"), signature));
	}

	@Test
	void retainsLegacySingleJsonStringMethodMode() {
		assertTrue(OpcUaMethodArguments.isLegacyJsonString(signature(
				List.of(argument("Command", "i=12", -1)), List.of())));
		assertFalse(OpcUaMethodArguments.isLegacyJsonString(signature(
				List.of(argument("Command", "ns=2;i=12", -1), argument("Count", "i=6", -1)), List.of())));
		assertFalse(OpcUaMethodArguments.isLegacyJsonString(signature(
				List.of(argument("Command", "i=12", 1)), List.of())));
	}

	@Test
	void oneArrayValuedArgumentIsDistinctFromMultipleArguments() {
		var signature = signature(List.of(argument("Positions", "i=11", 1)), List.of());
		assertEquals(List.of(List.of(1.0, 2.0)), OpcUaMethodArguments.toPositional(
				Map.of("Positions", List.of(1.0, 2.0)), signature));
		assertThrows(PayloadValidationException.class, () ->
				OpcUaMethodArguments.toPositional(Map.of("Positions", 1), signature));
	}

	@Test
	void rejectsMissingExtraAndDuplicateArgumentNamesInsteadOfGuessing() {
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.toPositional(
				Map.of("Uuid", "asset"), signature(List.of(argument("Speed", "i=6", -1)), List.of())));
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.toPositional(
				Map.of("Uuid", "asset", "Extra", 1),
				signature(List.of(argument("Uuid", "i=12", -1)), List.of()),
				java.util.Set.of("Uuid"), java.util.Set.of()));
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.toPositional(
				Map.of("Uuid", "asset"), signature(List.of(argument("Uuid", "i=12", -1),
						argument("Uuid", "i=12", -1)), List.of())));
	}

	@Test
	void optionalAIDMembersMayBeAbsentFromMethodButRequiredMembersMayNot() {
		var signature = signature(List.of(argument("Uuid", "i=12", -1)), List.of());
		var values = OpcUaMethodArguments.toPositional(Map.of("Uuid", "u-1", "OptionalHint", 2),
				signature, java.util.Set.of("Uuid", "OptionalHint"), java.util.Set.of("Uuid"));
		assertEquals(List.of("u-1"), values);
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.toPositional(
				Map.of("OptionalHint", 2), signature,
					java.util.Set.of("Uuid", "OptionalHint"), java.util.Set.of("Uuid")));
	}

	@Test
	void validatesNamesTypesAndArrayRankAgainstAidSchema() {
		var signature = signature(List.of(argument("Speed", "i=6", -1)),
				List.of(argument("State", "i=12", -1)));
		OpcUaMethodArguments.validateSchemaSignature(signature,
				Map.of("Speed", "integer", "Optional", "string"), java.util.Set.of("Speed"),
				Map.of("State", "string", "OptionalReply", "integer"), java.util.Set.of("State"), false);
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.validateSchemaSignature(
				signature, Map.of("Count", "integer"), java.util.Set.of("Count"),
					Map.of("State", "string"), java.util.Set.of("State"), false));
		var arraySignature = signature(List.of(argument("Samples", "i=6", 1)), List.of());
		OpcUaMethodArguments.validateSchemaSignature(arraySignature,
				Map.of("Samples", "array<integer>"), java.util.Set.of(), Map.of(), java.util.Set.of(), false);
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.validateSchemaSignature(
				arraySignature, Map.of("Samples", "array<string>"), java.util.Set.of(),
					Map.of(), java.util.Set.of(), false));
		assertThrows(PayloadValidationException.class, () -> OpcUaMethodArguments.validateSchemaSignature(
				signature(List.of(argument("Samples", "i=6", 2)), List.of()),
					Map.of("Samples", "array<integer>"), java.util.Set.of(),
					Map.of(), java.util.Set.of(), false));
	}

	private static OpcUaMethodMetadataProvider.MethodSignature signature(
			List<OpcUaMethodMetadataProvider.Argument> inputs,
			List<OpcUaMethodMetadataProvider.Argument> outputs) {
		return new OpcUaMethodMetadataProvider.MethodSignature(inputs, outputs);
	}

	private static OpcUaMethodMetadataProvider.Argument argument(String name, String type, int rank) {
		return new OpcUaMethodMetadataProvider.Argument(name, type, rank, List.of());
	}
}
