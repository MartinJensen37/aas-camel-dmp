/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.aas;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.aas.operation.OperationPayloads;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditLog;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditPoint;

/** Interaction-log points for AAS element reads, writes and change events. */
public final class AasAudit {
	private AasAudit() {
	}

	static void read(AASEndpoint endpoint, Object element, long startedNanos, Throwable failure) {
		AuditLog.emit(() -> {
			AuditPoint point = point("aas_read", endpoint, startedNanos, failure);
			return failure == null ? point.value(elementValue(element)) : point;
		});
	}

	static void write(AASEndpoint endpoint, Object value, long startedNanos, Throwable failure) {
		AuditLog.emit(() -> point("aas_write", endpoint, startedNanos, failure).value(value));
	}

	public static void event(String submodelId, String path, Object value, String eventType, String eventId, String eventTime) {
		AuditLog.emit(() -> AuditPoint.of("aas_event").tag("submodel", submodelId).tag("path", path)
				.tag("eventType", eventType).field("eventId", eventId).field("eventTime", eventTime).value(value));
	}

	private static AuditPoint point(String measurement, AASEndpoint endpoint, long startedNanos, Throwable failure) {
		return AuditPoint.of(measurement).tag("submodel", submodelId(endpoint.getSubmodelEndpoint()))
				.tag("path", endpoint.getPropertyPath()).tag("outcome", failure == null ? "ok" : "failed")
				.field("durationMs", (System.nanoTime() - startedNanos) / 1_000_000d)
				.field("error", failure == null ? null : failure.getClass().getSimpleName());
	}

	/** The submodel ID from a {@code …/submodels/<base64url(id)>} endpoint, or the endpoint itself. */
	static String submodelId(String submodelEndpoint) {
		if (submodelEndpoint == null) return null;
		int marker = submodelEndpoint.lastIndexOf("/submodels/");
		if (marker < 0) return submodelEndpoint;
		String encoded = submodelEndpoint.substring(marker + "/submodels/".length()).replaceAll("/+$", "");
		try {
			return new String(Base64.getUrlDecoder().decode(encoded.replace("=", "")), StandardCharsets.UTF_8);
		} catch (IllegalArgumentException notBase64) {
			return encoded;
		}
	}

	/** The plain value of an AAS element read as JSON: typed for Properties, maps and lists for containers. */
	static Object elementValue(Object element) {
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
