/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons whom the Software is furnished to do so, subject to
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
package dk.aau.smartproductionlab.dmp.opcua.canonical;

import java.util.Base64;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.milo.opcua.sdk.core.types.DynamicStructType;
import org.eclipse.milo.opcua.sdk.core.types.DynamicUnionType;
import org.eclipse.milo.opcua.stack.core.types.UaEnumeratedType;
import org.eclipse.milo.opcua.stack.core.types.UaStructuredType;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExtensionObject;
import org.eclipse.milo.opcua.stack.core.types.builtin.ByteString;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.ULong;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UNumber;

/**
 * The JSON form of OPC UA values (ADR-007: normalize at the protocol boundary). A source hands Lua the
 * variable's value itself; status codes and timestamps are not passed on (SPEC §5.2).
 *
 * @author aas-camel-dmp
 */
public final class OpcUaCanonical {

	private OpcUaCanonical() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * The JSON value of an OPC UA built-in value: unsigned integers become
	 * numbers, LocalizedText its text, NodeIds and QualifiedNames their
	 * parseable string, DateTime an ISO-8601 instant, ByteString and byte
	 * arrays base64, StatusCode its numeric code; arrays and collections map
	 * member by member. A decoded structure is an object keyed by field name (an absent
	 * optional field is left out), a union {@code {field: value}} (null when it holds none), an
	 * enumeration its Int32 value. An undecoded ExtensionObject falls back to its string.
	 */
	public static Object jsonValue(Object raw) {
		if (raw == null) {
			return null;
		}
		if (raw instanceof Variant variant) {
			return jsonValue(variant.getValue());
		}
		if (raw instanceof ULong unsigned) {
			return unsigned.toBigInteger();
		}
		if (raw instanceof UNumber unsigned) {
			return unsigned.longValue();
		}
		if (raw instanceof Number || raw instanceof Boolean || raw instanceof String) {
			return raw;
		}
		if (raw instanceof LocalizedText text) {
			return text.getText();
		}
		if (raw instanceof QualifiedName name) {
			return name.toParseableString();
		}
		if (raw instanceof NodeId id) {
			return id.toParseableString();
		}
		if (raw instanceof ExpandedNodeId id) {
			return id.toParseableString();
		}
		if (raw instanceof DateTime time) {
			return time.isNull() ? null : time.getJavaInstant().toString();
		}
		if (raw instanceof ByteString bytes) {
			return bytes.isNull() ? null : Base64.getEncoder().encodeToString(bytes.bytesOrEmpty());
		}
		if (raw instanceof StatusCode status) {
			return status.getValue();
		}
		if (raw instanceof byte[]) {
			return Base64.getEncoder().encodeToString((byte[]) raw);
		}
		if (raw instanceof UaEnumeratedType enumerated) {
			return enumerated.getValue();
		}
		if (raw instanceof DynamicStructType struct) {
			Map<String, Object> fields = new LinkedHashMap<>();
			struct.getMembers().forEach((name, value) -> fields.put(name, jsonValue(value)));
			return fields;
		}
		if (raw instanceof DynamicUnionType union) {
			return union.getValue().map(selected -> {
				Map<String, Object> field = new LinkedHashMap<>();
				field.put(selected.fieldName(), jsonValue(selected.fieldValue()));
				return field;
			}).orElse(null);
		}
		if (raw instanceof UaStructuredType struct && !(raw instanceof ExtensionObject)) {
			Map<String, Object> fields = generatedFields(struct);
			if (fields != null) return fields;
		}
		if (raw instanceof Map<?, ?> map) {
			Map<String, Object> copy = new LinkedHashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				copy.put(String.valueOf(entry.getKey()), jsonValue(entry.getValue()));
			}
			return copy;
		}
		if (raw instanceof Collection<?> collection) {
			java.util.List<Object> copy = new java.util.ArrayList<>(collection.size());
			for (Object value : collection) {
				copy.add(jsonValue(value));
			}
			return copy;
		}
		if (raw.getClass().isArray()) {
			int length = Array.getLength(raw);
			java.util.List<Object> copy = new java.util.ArrayList<>(length);
			for (int i = 0; i < length; i++) {
				copy.add(jsonValue(Array.get(raw, i)));
			}
			return copy;
		}
		return raw.toString();
	}

	/** Field getters of the structures the stack defines, in the order of their StructureDefinition. */
	private static final ClassValue<java.util.List<Map.Entry<String, java.lang.reflect.Method>>> GENERATED_FIELDS = new ClassValue<>() {
		@Override protected java.util.List<Map.Entry<String, java.lang.reflect.Method>> computeValue(Class<?> type) {
			try {
				var definition = (org.eclipse.milo.opcua.stack.core.types.structured.StructureDefinition)
						type.getMethod("definition", org.eclipse.milo.opcua.stack.core.NamespaceTable.class)
								.invoke(null, new org.eclipse.milo.opcua.stack.core.NamespaceTable());
				java.util.List<Map.Entry<String, java.lang.reflect.Method>> getters = new java.util.ArrayList<>();
				if (definition.getFields() != null)
					for (var field : definition.getFields())
						getters.add(Map.entry(field.getName(), type.getMethod("get" + field.getName())));
				return java.util.List.copyOf(getters);
			} catch (ReflectiveOperationException | RuntimeException notGenerated) {
				return null;
			}
		}
	};

	/** A structure the stack defines (namespace 0, e.g. Range) as an object keyed by field name; null when unknown. */
	private static Map<String, Object> generatedFields(UaStructuredType struct) {
		var getters = GENERATED_FIELDS.get(struct.getClass());
		if (getters == null) return null;
		Map<String, Object> fields = new LinkedHashMap<>();
		try {
			for (var getter : getters) fields.put(getter.getKey(), jsonValue(getter.getValue().invoke(struct)));
		} catch (ReflectiveOperationException unreadable) {
			return null;
		}
		return fields;
	}
}
