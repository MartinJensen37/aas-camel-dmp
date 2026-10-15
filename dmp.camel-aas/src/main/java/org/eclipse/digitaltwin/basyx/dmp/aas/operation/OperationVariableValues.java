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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;

/**
 * Converts AAS Operation variables to and from decoded values. A Property is a
 * typed scalar, a SubmodelElementCollection a map keyed by child idShort, and a
 * SubmodelElementList a list; nesting is unrestricted. Replies are rebuilt from
 * the declared variable (the template), so the caller receives exactly the
 * declared structure, value types and semantic IDs.
 */
public final class OperationVariableValues {

	private OperationVariableValues() {
	}

	/** True for the element kinds an Operation variable may use. */
	public static boolean supported(SubmodelElement element) {
		return element instanceof Property || element instanceof SubmodelElementCollection
				|| element instanceof SubmodelElementList;
	}

	/** The decoded value of a caller-supplied variable, typed by its own value types. */
	public static Object toValue(SubmodelElement element) {
		if (element instanceof Property property) {
			if (property.getValue() == null) return null;
			return OperationPayloads.coerceTyped(property.getValue(), Aas.xsd(property.getValueType()));
		}
		if (element instanceof SubmodelElementCollection collection) {
			Map<String, Object> values = new LinkedHashMap<>();
			for (SubmodelElement child : Aas.nonNull(collection.getValue())) {
				if (child.getIdShort() == null || values.containsKey(child.getIdShort()))
					throw invalidRequest("collection members need unique idShorts");
				values.put(child.getIdShort(), toValue(child));
			}
			return values;
		}
		if (element instanceof SubmodelElementList list) {
			List<Object> values = new ArrayList<>();
			for (SubmodelElement item : Aas.nonNull(list.getValue())) values.add(toValue(item));
			return values;
		}
		throw invalidRequest("unsupported Operation variable type " + Aas.modelType(element));
	}

	/** Applies the declared value types to a decoded value; shapes that do not match pass through unchanged. */
	public static Object coerce(SubmodelElement template, Object value) {
		if (template == null || value == null || value == JsonPointer.JSON_NULL) return value;
		if (template instanceof Property property)
			return value instanceof Map || value instanceof List ? value
					: OperationPayloads.coerceTyped(value, Aas.xsd(property.getValueType()));
		if (template instanceof SubmodelElementCollection collection && value instanceof Map<?, ?> map) {
			Map<String, Object> coerced = new LinkedHashMap<>();
			map.forEach((key, member) -> coerced.put(String.valueOf(key),
					coerce(Aas.child(collection, String.valueOf(key)), member)));
			return coerced;
		}
		if (template instanceof SubmodelElementList list && value instanceof List<?> items) {
			SubmodelElement item = itemTemplate(list);
			List<Object> coerced = new ArrayList<>();
			for (Object member : items) coerced.add(coerce(item, member));
			return coerced;
		}
		return value;
	}

	/**
	 * Builds the reply element for one declared variable. Members the template
	 * does not declare are dropped and missing members are omitted, like
	 * top-level outputs; a value whose shape contradicts the template fails.
	 */
	public static SubmodelElement toElement(SubmodelElement template, Object value) {
		return toElement(template, template.getIdShort(), value, template.getIdShort());
	}

	private static SubmodelElement toElement(SubmodelElement template, String idShort, Object value, String path) {
		boolean absent = value == null || value == JsonPointer.JSON_NULL;
		if (template instanceof Property property) {
			if (value instanceof Map || value instanceof List) throw shapeMismatch(path, "a scalar");
			Object typed = absent ? null : OperationPayloads.coerceTyped(value, Aas.xsd(property.getValueType()));
			return new DefaultProperty.Builder().idShort(idShort).semanticId(property.getSemanticId())
					.valueType(property.getValueType()).value(typed == null ? null : String.valueOf(typed)).build();
		}
		if (template instanceof SubmodelElementCollection collection) {
			if (!absent && !(value instanceof Map)) throw shapeMismatch(path, "an object");
			Map<?, ?> members = absent ? Map.of() : (Map<?, ?>) value;
			List<SubmodelElement> children = new ArrayList<>();
			for (SubmodelElement child : Aas.nonNull(collection.getValue())) {
				if (members.containsKey(child.getIdShort()))
					children.add(toElement(child, child.getIdShort(), members.get(child.getIdShort()),
							path + "/" + child.getIdShort()));
			}
			return new DefaultSubmodelElementCollection.Builder().idShort(idShort)
					.semanticId(collection.getSemanticId()).value(children).build();
		}
		if (template instanceof SubmodelElementList list) {
			if (!absent && !(value instanceof List)) throw shapeMismatch(path, "an array");
			SubmodelElement item = itemTemplate(list);
			if (item == null && !absent && !((List<?>) value).isEmpty())
				throw new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
						"Operation output " + path + " declares no list member type", List.of());
			List<SubmodelElement> items = new ArrayList<>();
			int index = 0;
			// AASd-120: list members carry no idShort.
			for (Object member : absent ? List.of() : (List<?>) value)
				items.add(toElement(item, null, member, path + "/" + index++));
			return new DefaultSubmodelElementList.Builder().idShort(idShort).semanticId(list.getSemanticId())
					.typeValueListElement(list.getTypeValueListElement())
					.valueTypeListElement(list.getValueTypeListElement())
					.semanticIdListElement(list.getSemanticIdListElement())
					.orderRelevant(list.getOrderRelevant()).value(items).build();
		}
		throw new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
				"Operation output " + path + " has an unsupported declared type", List.of());
	}

	/**
	 * The declared shape of every list member: the first authored member, or a
	 * Property of {@code valueTypeListElement} for an empty Property list.
	 */
	public static SubmodelElement itemTemplate(SubmodelElementList list) {
		List<SubmodelElement> members = Aas.nonNull(list.getValue());
		if (!members.isEmpty()) return members.get(0);
		if (list.getTypeValueListElement() == AasSubmodelElements.PROPERTY && list.getValueTypeListElement() != null)
			return new DefaultProperty.Builder().valueType(list.getValueTypeListElement())
					.semanticId(list.getSemanticIdListElement()).build();
		return null;
	}

	private static OperationValidationException invalidRequest(String message) {
		return new OperationValidationException("DMP_OPERATION_VALIDATION_FAILED", 500,
				"Operation request: " + message, List.of());
	}

	private static OperationValidationException shapeMismatch(String path, String expected) {
		return new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
				"Operation output " + path + " must be " + expected + " as declared", List.of());
	}
}
