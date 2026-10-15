package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadValidationException;

/** Name-based mapping from logical Lua/AID objects to UA's positional Call arrays. */
public final class OpcUaMethodArguments {
	private OpcUaMethodArguments() {}

	public static boolean isLegacyJsonString(OpcUaMethodMetadataProvider.MethodSignature signature) {
		return signature.inputs().size() == 1
				&& signature.inputs().get(0).valueRank() == -1
				&& isUaString(signature.inputs().get(0).dataType());
	}

	public static List<Object> toPositional(Map<String, Object> namedValues,
			OpcUaMethodMetadataProvider.MethodSignature signature) {
		return toPositional(namedValues, signature, namedValues.keySet(), java.util.Set.of());
	}

	public static List<Object> toPositional(Map<String, Object> namedValues,
			OpcUaMethodMetadataProvider.MethodSignature signature,
			java.util.Set<String> declaredMembers, java.util.Set<String> requiredMembers) {
		Set<String> seen = new LinkedHashSet<>();
		List<Object> ordered = new ArrayList<>(signature.inputs().size());
		for (OpcUaMethodMetadataProvider.Argument argument : signature.inputs()) {
			if (!seen.add(argument.name())) {
				throw mismatch("duplicate UA InputArgument name " + argument.name());
			}
			if (!declaredMembers.contains(argument.name())) {
				throw mismatch("UA InputArgument " + argument.name() + " is absent from the AID input schema");
			}
			if (!namedValues.containsKey(argument.name())) {
				throw mismatch("AID command has no member named UA argument " + argument.name());
			}
			Object value = namedValues.get(argument.name());
			boolean array = value instanceof List<?> || value != null && value.getClass().isArray();
			if (value == null && argument.valueRank() >= 1) {
				throw mismatch("UA array argument " + argument.name() + " is null");
			}
			if (argument.valueRank() >= 1 && !array) {
				throw mismatch("UA argument " + argument.name() + " requires an array value");
			}
			if (argument.valueRank() == -1 && array) {
				throw mismatch("UA argument " + argument.name() + " requires a scalar value");
			}
			ordered.add(value);
		}
		for (String required : requiredMembers) {
			if (!seen.contains(required)) throw mismatch("required AID input member " + required
					+ " has no UA InputArgument");
		}
		for (String key : namedValues.keySet()) {
			if (!declaredMembers.contains(key)) throw mismatch("Lua command member " + key + " is absent from the AID input schema");
		}
		return List.copyOf(ordered);
	}

	public static void validateSchemaSignature(OpcUaMethodMetadataProvider.MethodSignature signature,
			Map<String, String> inputTypes, java.util.Set<String> inputRequired,
			Map<String, String> outputTypes, java.util.Set<String> outputRequired,
			boolean legacyJsonString) {
		if (!legacyJsonString) validateDirection(signature.inputs(), inputTypes, inputRequired, "input");
		validateDirection(signature.outputs(), outputTypes, outputRequired, "output");
	}

	private static void validateDirection(List<OpcUaMethodMetadataProvider.Argument> arguments,
			Map<String, String> schemaTypes, java.util.Set<String> required, String direction) {
		Set<String> names = new LinkedHashSet<>();
		for (OpcUaMethodMetadataProvider.Argument argument : arguments) {
			if (!names.add(argument.name())) throw mismatch("duplicate UA " + direction
					+ " Argument name " + argument.name());
			String schemaType = schemaTypes.get(argument.name());
			if (schemaType == null) throw mismatch("UA " + direction + " Argument " + argument.name()
					+ " is absent from the AID schema");
			String uaType = jsonType(argument.dataType());
			boolean schemaArray = schemaType.startsWith("array<");
			int schemaRank = schemaArray ? arrayRank(schemaType) : 0;
			int uaRank = argument.valueRank();
			if (!rankCompatible(schemaArray, schemaRank, uaRank)) {
				throw mismatch("UA " + direction + " Argument " + argument.name()
						+ " array rank does not match AID type " + schemaType);
			}
			String expectedType = schemaArray ? arrayLeafType(schemaType) : schemaType;
			if (uaType != null && !"unknown".equals(expectedType)
					&& !compatibleSchemaType(expectedType, uaType)) {
				throw mismatch("UA " + direction + " Argument " + argument.name() + " type " + uaType
					+ " does not match AID type " + expectedType);
			}
			if (uaRank > 0 && !argument.arrayDimensions().isEmpty()
					&& argument.arrayDimensions().size() != uaRank) {
				throw mismatch("UA " + direction + " Argument " + argument.name()
						+ " declares dimensions inconsistent with valueRank " + uaRank);
			}
		}
		for (String name : required) {
			if (!names.contains(name)) throw mismatch("required AID " + direction + " member " + name
					+ " is absent from the UA Method signature");
		}
	}

	private static boolean compatibleSchemaType(String schemaType, String uaType) {
		if ("object".equals(schemaType)) return "object".equals(uaType);
		return schemaType.equals(uaType);
	}

	private static boolean rankCompatible(boolean schemaArray, int schemaRank, int uaRank) {
		if (!schemaArray) return uaRank == -1;
		if (uaRank == -2 || uaRank == 0) return true;
		if (uaRank == -3) return schemaRank == 1;
		return uaRank >= 1 && uaRank == schemaRank;
	}

	private static int arrayRank(String type) {
		int rank = 0;
		String current = type;
		while (current.startsWith("array<") && current.endsWith(">")) {
			rank++;
			current = arrayItemType(current);
		}
		return rank;
	}

	private static String arrayItemType(String type) {
		return type.substring("array<".length(), type.length() - 1);
	}

	private static String arrayLeafType(String type) {
		String item = arrayItemType(type);
		while (item.startsWith("array<") && item.endsWith(">")) item = arrayItemType(item);
		return item;
	}

	private static String jsonType(String dataType) {
		Integer id = builtinTypeId(dataType);
		if (id == null) return null; // custom UA datatype: validate rank/name, retain AID value coercion
		return switch (id) {
		case 1 -> "boolean";
		case 2, 3, 4, 5, 6, 7, 8, 9 -> "integer";
		case 10, 11 -> "number";
		case 12 -> "string";
		case 15 -> "string"; // ByteString is base64-encoded in JSON
		default -> null;
		};
	}

	public static Map<String, Object> fromPositional(List<?> values,
			OpcUaMethodMetadataProvider.MethodSignature signature) {
		if (values.size() != signature.outputs().size()) {
			throw mismatch("UA returned " + values.size() + " output arguments but the server signature has "
					+ signature.outputs().size());
		}
		Map<String, Object> named = new java.util.LinkedHashMap<>();
		for (int i = 0; i < values.size(); i++) {
			var argument = signature.outputs().get(i);
			String name = argument.name();
			if (named.containsKey(name)) throw mismatch("duplicate UA OutputArgument name " + name);
			validateOutputValue(values.get(i), argument.valueRank(), jsonType(argument.dataType()),
					Integer.valueOf(15).equals(builtinTypeId(argument.dataType())), name);
			named.put(name, values.get(i));
		}
		return named;
	}

	private static void validateOutputValue(Object value, int rank, String type, boolean byteString, String name) {
		if (value == null) return; // UA null Variants are legal; required/null schema rules are separate.
		boolean array = value instanceof List<?> || value.getClass().isArray();
		if ((rank == -1 && array) || (rank >= 0 && !array))
			throw mismatch("UA OutputArgument " + name + " value does not match its declared rank");
		if (array) {
			int next = rank > 1 ? rank - 1 : rank == 1 || rank == -3 ? -1 : -2;
			if (value instanceof List<?> list) {
				for (Object item : list) validateOutputValue(item, next, type, byteString, name);
			} else {
				for (int i = 0; i < java.lang.reflect.Array.getLength(value); i++)
					validateOutputValue(java.lang.reflect.Array.get(value, i), next, type, byteString, name);
			}
			return;
		}
		boolean valid = type == null || switch (type) {
			case "boolean" -> value instanceof Boolean;
			case "string" -> byteString || value instanceof String;
			case "number" -> value instanceof Number number && Double.isFinite(number.doubleValue());
			case "integer" -> value instanceof Number number && Double.isFinite(number.doubleValue())
					&& new java.math.BigDecimal(number.toString()).stripTrailingZeros().scale() <= 0;
			default -> true;
		};
		if (!valid) throw mismatch("UA OutputArgument " + name + " value does not match its declared type");
	}

	private static boolean isUaString(String dataType) {
		return "String".equalsIgnoreCase(dataType) || Integer.valueOf(12).equals(builtinTypeId(dataType));
	}

	private static Integer builtinTypeId(String dataType) {
		if (dataType == null) return null;
		java.util.regex.Matcher matcher = java.util.regex.Pattern
				.compile("(?:ns=0;)?i=(\\d+)").matcher(dataType);
		if (!matcher.matches()) return null;
		try { return Integer.valueOf(matcher.group(1)); }
		catch (NumberFormatException e) { return null; }
	}

	private static PayloadValidationException mismatch(String message) {
		return new PayloadValidationException(PayloadValidationException.Stage.MAPPING,
				message, List.of());
	}
}
