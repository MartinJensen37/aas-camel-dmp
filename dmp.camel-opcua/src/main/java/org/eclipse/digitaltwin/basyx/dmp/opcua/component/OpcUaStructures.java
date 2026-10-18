/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.opcua.canonical.OpcUaCanonical;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.core.types.DynamicEnumType;
import org.eclipse.milo.opcua.sdk.core.types.DynamicStructType;
import org.eclipse.milo.opcua.sdk.core.types.DynamicUnionType;
import org.eclipse.milo.opcua.sdk.core.typetree.DataType;
import org.eclipse.milo.opcua.sdk.core.typetree.DataTypeTree;
import org.eclipse.milo.opcua.stack.core.OpcUaDataType;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.encoding.DataTypeCodec;
import org.eclipse.milo.opcua.stack.core.encoding.EncodingContext;
import org.eclipse.milo.opcua.stack.core.types.UaEnumeratedType;
import org.eclipse.milo.opcua.stack.core.types.UaStructuredType;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.Matrix;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.XmlElement;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UByte;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UShort;
import org.eclipse.milo.opcua.stack.core.types.enumerated.StructureType;
import org.eclipse.milo.opcua.stack.core.types.structured.EnumDefinition;
import org.eclipse.milo.opcua.stack.core.types.structured.EnumField;
import org.eclipse.milo.opcua.stack.core.types.structured.StructureDefinition;
import org.eclipse.milo.opcua.stack.core.types.structured.StructureField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OPC UA structured values as the objects Lua sees, and back (SPEC §5.2). A structure
 * travels as an ExtensionObject in UA Binary; how it is encoded is the server's to say:
 * the client reads the server's DataType nodes and their {@code DataTypeDefinition}s once
 * per session ({@link OpcUaClient#getDynamicEncodingContext()}), only when a structure is
 * first met. A structure becomes an object keyed by field name, an array of structures an
 * array, a union {@code {field: value}}, an enumeration its Int32 value
 * ({@link OpcUaCanonical#jsonValue}). Custom encodings the server does not describe stay
 * undecoded.
 */
public final class OpcUaStructures {
	private static final Logger LOGGER = LoggerFactory.getLogger(OpcUaStructures.class);

	/** A value to be encoded as {@code dataType} once the client is at hand (a Method input argument). */
	public record Pending(String dataType, Object value) { }

	private OpcUaStructures() {
	}

	// ---- decoding ----

	/**
	 * The value with each ExtensionObject in it (in a DataValue, Variant, array, list, matrix
	 * or pending Read) replaced by its decoded structure as a JSON-ready object. A value
	 * without one is returned as it is.
	 */
	public static Object decoded(Object value, OpcUaClient client) {
		if (value instanceof CompletionStage<?> pending) return pending.thenApply(result -> decoded(result, client));
		if (!containsExtensionObject(value)) return value;
		EncodingContext context;
		try {
			context = client.getDynamicEncodingContext();
		} catch (UaException | RuntimeException unavailable) {
			LOGGER.warn("OPC UA structures cannot be decoded: the server's DataTypes are unreadable ({})", unavailable.getMessage());
			return value;
		}
		return decoded(value, context);
	}

	static Object decoded(Object value, EncodingContext context) {
		if (value instanceof DataValue data) {
			Variant variant = data.getValue();
			return variant == null ? data : data.copy(builder -> builder.setValue(new Variant(decoded(variant.getValue(), context))));
		}
		if (value instanceof Variant variant) return new Variant(decoded(variant.getValue(), context));
		if (value instanceof Variant[] variants) {
			Variant[] copy = new Variant[variants.length];
			for (int i = 0; i < variants.length; i++) copy[i] = variants[i] == null ? null : (Variant) decoded(variants[i], context);
			return copy;
		}
		if (value instanceof ExtensionObject xo) return decode(xo, context);
		if (value instanceof Matrix matrix) return matrix.transform(element -> decoded(element, context));
		if (value instanceof List<?> list) return list.stream().map(element -> decoded(element, context)).toList();
		if (value instanceof Object[] array) {
			Object[] copy = new Object[array.length];
			for (int i = 0; i < array.length; i++) copy[i] = decoded(array[i], context);
			return copy;
		}
		return value;
	}

	private static Object decode(ExtensionObject xo, EncodingContext context) {
		if (xo.isNull()) return null;
		try {
			return OpcUaCanonical.jsonValue(xo.decode(context));
		} catch (RuntimeException unknown) {
			LOGGER.debug("OPC UA ExtensionObject {} is not decodable: {}", xo.getEncodingOrTypeId(), unknown.getMessage());
			return xo;
		}
	}

	private static boolean containsExtensionObject(Object value) {
		if (value instanceof ExtensionObject) return true;
		if (value instanceof DataValue data) return data.getValue() != null && containsExtensionObject(data.getValue().getValue());
		if (value instanceof Variant variant) return containsExtensionObject(variant.getValue());
		if (value instanceof Matrix matrix) return containsExtensionObject(matrix.getElements());
		if (value instanceof Collection<?> values) return values.stream().anyMatch(OpcUaStructures::containsExtensionObject);
		if (value instanceof Object[] array) return Arrays.stream(array).anyMatch(OpcUaStructures::containsExtensionObject);
		return false;
	}

	// ---- the device's schema ----

	/**
	 * What the server declares for a sampled or read value, as a JSON-schema map: a structure
	 * as an object of its fields (from its DataTypeDefinition, nested), an array of structures
	 * as an array, a built-in value by its type. Null when the value says nothing (a null or
	 * an undecodable value); the AID schema is then not checked.
	 */
	public static Map<String, Object> deviceSchema(Object value, OpcUaClient client) {
		Object raw = value instanceof DataValue data ? (data.getValue() == null ? null : data.getValue().getValue()) : value;
		if (raw instanceof Variant variant) raw = variant.getValue();
		if (raw == null) return null;
		try {
			if (raw instanceof ExtensionObject || raw instanceof ExtensionObject[]) {
				EncodingContext context = client.getDynamicEncodingContext();
				DataTypeTree tree = client.getDataTypeTree();
				if (raw instanceof ExtensionObject xo) return structureSchema(xo.decode(context), tree, context);
				ExtensionObject[] array = (ExtensionObject[]) raw;
				Map<String, Object> schema = new LinkedHashMap<>();
				schema.put("type", "array");
				if (array.length > 0 && !array[0].isNull()) schema.put("items", structureSchema(array[0].decode(context), tree, context));
				return schema;
			}
		} catch (UaException | RuntimeException unknown) {
			return null;
		}
		boolean array = raw.getClass().isArray();
		Class<?> type = array ? raw.getClass().getComponentType() : raw.getClass();
		String json = builtinJsonType(OpcUaDataType.fromBackingClass(type));
		if (json == null) return null;
		return array ? Map.of("type", "array", "items", Map.of("type", json)) : Map.of("type", json);
	}

	private static Map<String, Object> structureSchema(UaStructuredType struct, DataTypeTree tree, EncodingContext context) {
		NodeId type = struct.getTypeId().toNodeId(context.getNamespaceTable()).orElse(null);
		return type == null ? null : typeSchema(type, tree, 0);
	}

	private static Map<String, Object> typeSchema(NodeId type, DataTypeTree tree, int depth) {
		if (OpcUaDataType.isBuiltin(type)) {
			String json = builtinJsonType(OpcUaDataType.fromNodeId(type));
			return json == null ? Map.of() : Map.of("type", json);
		}
		if (tree.isEnumType(type)) return Map.of("type", "integer");
		if (tree.isStructType(type)) {
			DataType dataType = tree.getDataType(type);
			if (depth > 16 || dataType == null || !(dataType.getDataTypeDefinition() instanceof StructureDefinition definition))
				return Map.of("type", "object");
			Map<String, Object> fields = new LinkedHashMap<>();
			if (definition.getFields() != null)
				for (StructureField field : definition.getFields()) {
					Map<String, Object> schema = typeSchema(field.getDataType(), tree, depth + 1);
					int rank = field.getValueRank() == null ? -1 : field.getValueRank();
					fields.put(field.getName(), rank == -1 ? schema : rank == 1 ? Map.of("type", "array", "items", schema)
							: Map.of("type", "array"));
				}
			return Map.of("type", "object", "properties", fields);
		}
		OpcUaDataType alias = tree.getBuiltinType(type);
		String json = alias == null ? null : builtinJsonType(alias);
		return json == null ? Map.of() : Map.of("type", json);
	}

	/** The JSON type {@link OpcUaCanonical#jsonValue} gives a built-in type; null for Variant and other open types. */
	private static String builtinJsonType(OpcUaDataType type) {
		if (type == null) return null;
		return switch (type) {
			case Boolean -> "boolean";
			case SByte, Byte, Int16, UInt16, Int32, UInt32, Int64, UInt64, StatusCode -> "integer";
			case Float, Double -> "number";
			case String, DateTime, Guid, ByteString, XmlElement, NodeId, ExpandedNodeId, QualifiedName, LocalizedText -> "string";
			default -> null;
		};
	}

	// ---- encoding ----

	/**
	 * A JSON value as a Variant's value of the DataType {@code dataType}: a structure (an
	 * object) as an ExtensionObject, an enumeration as its Int32, a built-in or alias type as
	 * its Java type, a list element by element as an array.
	 *
	 * @throws IllegalArgumentException when the value does not fit the DataType
	 */
	public static Object encode(Object json, NodeId dataType, OpcUaClient client) throws UaException {
		return new Encoder(client.getDataTypeTree(), client.getDynamicEncodingContext()).variantValue(json, dataType);
	}

	/** Whether {@code dataType} is one of the scalar built-in types (Boolean … String) written without the type tree. */
	public static boolean simpleBuiltin(NodeId dataType) {
		return dataType != null && dataType.getNamespaceIndex().intValue() == 0
				&& dataType.getIdentifier() instanceof Number id && id.intValue() >= 1 && id.intValue() <= 12;
	}

	record Encoder(DataTypeTree tree, EncodingContext context) {
		Object variantValue(Object json, NodeId type) {
			if (json == null || json == JsonPointer.JSON_NULL) return null;
			if (json instanceof List<?> list) {
				if (tree.isStructType(type)) return list.stream().map(item -> ExtensionObject.encode(context, structure(item, type)))
						.toArray(ExtensionObject[]::new);
				if (tree.isEnumType(type)) return list.stream().map(item -> enumeration(item, type).getValue()).toArray(Integer[]::new);
				OpcUaDataType builtin = builtinOf(type);
				Object array = Array.newInstance(builtin.getBackingClass(), list.size());
				for (int i = 0; i < list.size(); i++) Array.set(array, i, builtin(list.get(i), builtin));
				return array;
			}
			if (tree.isStructType(type)) return ExtensionObject.encode(context, structure(json, type));
			if (tree.isEnumType(type)) return enumeration(json, type).getValue();
			OpcUaDataType builtin = builtinOf(type);
			return builtin == OpcUaDataType.Variant ? json : builtin(json, builtin); // BaseDataType: the Variant holds it
		}

		private UaStructuredType structure(Object json, NodeId type) {
			DataType dataType = tree.getDataType(type);
			if (dataType == null || !(dataType.getDataTypeDefinition() instanceof StructureDefinition definition))
				throw new IllegalArgumentException("the server describes no structure " + type.toParseableString());
			String name = dataType.getBrowseName().getName();
			if (!(json instanceof Map<?, ?> object)) throw new IllegalArgumentException(name + " requires an object, not " + json);
			NodeId encodingId = dataType.getBinaryEncodingId();
			DataTypeCodec codec = encodingId == null ? null : context.getDataTypeManager().getCodec(encodingId);
			if (codec == null) throw new IllegalArgumentException("the server has no binary encoding for " + name);
			StructureField[] fields = definition.getFields() == null ? new StructureField[0] : definition.getFields();
			for (Object key : object.keySet())
				if (Arrays.stream(fields).noneMatch(field -> field.getName().equals(String.valueOf(key))))
					throw new IllegalArgumentException(name + " has no field " + key);
			StructureType kind = definition.getStructureType();
			if (kind == StructureType.Union || kind == StructureType.UnionWithSubtypedValues) {
				if (object.size() > 1) throw new IllegalArgumentException("union " + name + " takes one field, not " + object.keySet());
				if (object.isEmpty()) return new DynamicUnionType(dataType);
				StructureField field = field(fields, String.valueOf(object.keySet().iterator().next()));
				return new DynamicUnionType(dataType, new DynamicUnionType.UnionValue(field.getName(),
						fieldValue(object.get(field.getName()), field, name)));
			}
			LinkedHashMap<String, Object> members = new LinkedHashMap<>();
			for (StructureField field : fields) {
				if (!object.containsKey(field.getName())) {
					if (Boolean.TRUE.equals(field.getIsOptional())) continue;
					throw new IllegalArgumentException(name + " requires field " + field.getName());
				}
				members.put(field.getName(), fieldValue(object.get(field.getName()), field, name));
			}
			if (codec.getType() == DynamicStructType.class) return new DynamicStructType(dataType, members);
			return generated(codec.getType(), fields, members, name);
		}

		private Object fieldValue(Object json, StructureField field, String structure) {
			if (json == null || json == JsonPointer.JSON_NULL) return null;
			int rank = field.getValueRank() == null ? -1 : field.getValueRank();
			if (rank == -1) return fieldScalar(json, field.getDataType());
			if (rank == 1) {
				if (!(json instanceof List<?> list))
					throw new IllegalArgumentException(structure + "." + field.getName() + " requires an array");
				Object array = Array.newInstance(fieldComponent(field.getDataType()), list.size());
				for (int i = 0; i < list.size(); i++) Array.set(array, i, fieldScalar(list.get(i), field.getDataType()));
				return array;
			}
			throw new IllegalArgumentException(structure + "." + field.getName() + " has valueRank " + rank + ", which is not supported");
		}

		private Object fieldScalar(Object json, NodeId type) {
			if (OpcUaDataType.isBuiltin(type)) return builtin(json, OpcUaDataType.fromNodeId(type));
			if (tree.isEnumType(type)) return enumeration(json, type);
			if (tree.isStructType(type)) return structure(json, type);
			return builtin(json, builtinOf(type));
		}

		private Class<?> fieldComponent(NodeId type) {
			if (OpcUaDataType.isBuiltin(type)) return OpcUaDataType.fromNodeId(type).getBackingClass();
			if (tree.isEnumType(type)) return UaEnumeratedType.class;
			if (tree.isStructType(type)) return UaStructuredType.class;
			return builtinOf(type).getBackingClass();
		}

		private DynamicEnumType enumeration(Object json, NodeId type) {
			DataType dataType = tree.getDataType(type);
			if (json instanceof String name && dataType.getDataTypeDefinition() instanceof EnumDefinition definition
					&& definition.getFields() != null) {
				for (EnumField field : definition.getFields())
					if (name.equals(field.getName())) return new DynamicEnumType(dataType, field.getValue().intValue());
				throw new IllegalArgumentException(dataType.getBrowseName().getName() + " has no value " + name);
			}
			return new DynamicEnumType(dataType, ((Number) integral(json, Integer.MIN_VALUE, Integer.MAX_VALUE)).intValue());
		}

		private OpcUaDataType builtinOf(NodeId type) {
			OpcUaDataType builtin = OpcUaDataType.isBuiltin(type) ? OpcUaDataType.fromNodeId(type) : tree.getBuiltinType(type);
			if (builtin == null) throw new IllegalArgumentException("the server describes no DataType " + type.toParseableString());
			return builtin;
		}
	}

	/** A JSON value as the Java value of a built-in type; JSON renderings follow {@link OpcUaCanonical#jsonValue}. */
	static Object builtin(Object json, OpcUaDataType type) {
		if (json == null || json == JsonPointer.JSON_NULL) return null;
		return switch (type) {
			case Boolean -> {
				if (json instanceof Boolean flag) yield flag;
				throw new IllegalArgumentException("Boolean requires a boolean, not " + json);
			}
			case SByte -> integral(json, Byte.MIN_VALUE, Byte.MAX_VALUE).byteValue();
			case Byte -> UByte.valueOf(integral(json, 0, 0xFF).intValue());
			case Int16 -> integral(json, Short.MIN_VALUE, Short.MAX_VALUE).shortValue();
			case UInt16 -> UShort.valueOf(integral(json, 0, 0xFFFF).intValue());
			case Int32 -> integral(json, Integer.MIN_VALUE, Integer.MAX_VALUE).intValue();
			case UInt32 -> UInteger.valueOf(integral(json, 0, 0xFFFFFFFFL).longValue());
			case Int64 -> integral(json, Long.MIN_VALUE, Long.MAX_VALUE).longValue();
			case UInt64 -> ULong.valueOf(integral(json, BigInteger.ZERO, new BigInteger("18446744073709551615")).toString());
			case Float -> number(json).floatValue();
			case Double -> number(json).doubleValue();
			case String -> text(json, type);
			case DateTime -> new DateTime(Instant.parse(text(json, type)));
			case Guid -> UUID.fromString(text(json, type));
			case ByteString -> ByteString.of(Base64.getDecoder().decode(text(json, type)));
			case XmlElement -> new XmlElement(text(json, type));
			case NodeId -> NodeId.parse(text(json, type));
			case ExpandedNodeId -> ExpandedNodeId.parse(text(json, type));
			case StatusCode -> new StatusCode(integral(json, 0, 0xFFFFFFFFL).longValue());
			case QualifiedName -> QualifiedName.parse(text(json, type));
			case LocalizedText -> new LocalizedText(text(json, type));
			case Variant -> new Variant(json);
			default -> throw new IllegalArgumentException("a " + type + " cannot be written from JSON");
		};
	}

	private static BigInteger integral(Object json, long min, long max) {
		return integral(json, BigInteger.valueOf(min), BigInteger.valueOf(max));
	}

	private static BigInteger integral(Object json, BigInteger min, BigInteger max) {
		BigInteger value;
		try {
			value = new BigDecimal(number(json).toString()).toBigIntegerExact();
		} catch (ArithmeticException | NumberFormatException fraction) {
			throw new IllegalArgumentException("an integer is required, not " + json);
		}
		if (value.compareTo(min) < 0 || value.compareTo(max) > 0)
			throw new IllegalArgumentException(json + " is outside [" + min + ", " + max + "]");
		return value;
	}

	private static Number number(Object json) {
		if (json instanceof Number number) return number;
		throw new IllegalArgumentException("a number is required, not " + json);
	}

	private static String text(Object json, OpcUaDataType type) {
		if (json instanceof String text) return text;
		throw new IllegalArgumentException(type + " requires a string, not " + json);
	}

	private static StructureField field(StructureField[] fields, String name) {
		return Arrays.stream(fields).filter(field -> field.getName().equals(name)).findFirst().orElseThrow();
	}

	/** A structure the stack defines (namespace 0): its constructor takes the fields in definition order. */
	private static UaStructuredType generated(Class<?> type, StructureField[] fields, Map<String, Object> members, String name) {
		for (Constructor<?> constructor : type.getConstructors()) {
			if (constructor.getParameterCount() != fields.length) continue;
			Class<?>[] parameters = constructor.getParameterTypes();
			Object[] arguments = new Object[fields.length];
			try {
				for (int i = 0; i < fields.length; i++) arguments[i] = adapt(members.get(fields[i].getName()), parameters[i]);
				return (UaStructuredType) constructor.newInstance(arguments);
			} catch (ReflectiveOperationException | IllegalArgumentException | ClassCastException mismatch) {
				throw new IllegalArgumentException(name + " cannot be built from its fields: " + mismatch.getMessage());
			}
		}
		throw new IllegalArgumentException(name + " cannot be built from its fields");
	}

	/** A field value as the generated constructor's parameter type: enumerations and arrays are converted. */
	private static Object adapt(Object value, Class<?> parameter) throws ReflectiveOperationException {
		if (value == null || parameter.isInstance(value)) return value;
		if (value instanceof UaEnumeratedType enumerated && parameter.isEnum())
			return parameter.getMethod("from", int.class).invoke(null, enumerated.getValue());
		if (parameter.isArray() && value.getClass().isArray()) {
			int length = Array.getLength(value);
			Object array = Array.newInstance(parameter.getComponentType(), length);
			for (int i = 0; i < length; i++) Array.set(array, i, adapt(Array.get(value, i), parameter.getComponentType()));
			return array;
		}
		return value;
	}
}
