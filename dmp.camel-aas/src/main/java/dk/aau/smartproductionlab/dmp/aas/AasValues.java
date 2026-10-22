/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.aas.operation.OperationPayloads;

/**
 * The one form in which AAS elements reach Lua, whether polled, read for fan-in or
 * carried by a change event (SPEC §5.3): a Property is its value typed by its
 * {@code valueType}, a collection an object keyed by idShort, a list an array.
 */
public final class AasValues {
	private AasValues() {
	}

	/** The value of an AAS element decoded from its JSON serialization; other input is returned unchanged. */
	public static Object elementValue(Object element) {
		if (!(element instanceof Map<?, ?> map) || !(map.get("modelType") instanceof String modelType)) return element;
		switch (modelType) {
		case "Property": {
			Object value = map.get("value");
			return value == null ? null : OperationPayloads.coerceTyped(value, String.valueOf(map.get("valueType")));
		}
		case "SubmodelElementCollection": {
			Map<String, Object> values = new LinkedHashMap<>();
			if (map.get("value") instanceof List<?> children)
				for (Object child : children)
					if (child instanceof Map<?, ?> childMap && childMap.get("idShort") != null)
						values.put(String.valueOf(childMap.get("idShort")), elementValue(child));
			return values;
		}
		case "SubmodelElementList": {
			List<Object> values = new ArrayList<>();
			if (map.get("value") instanceof List<?> children) for (Object child : children) values.add(elementValue(child));
			return values;
		}
		default:
			return element;
		}
	}
}
