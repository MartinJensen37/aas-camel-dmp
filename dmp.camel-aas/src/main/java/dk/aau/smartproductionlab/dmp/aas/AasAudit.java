/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.aas;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import dk.aau.smartproductionlab.dmp.core.audit.AuditLog;
import dk.aau.smartproductionlab.dmp.core.audit.AuditPoint;

/** Interaction-log points for AAS element reads, writes and change events. */
public final class AasAudit {
	private AasAudit() {
	}

	static void read(AASEndpoint endpoint, Object element, long startedNanos, Throwable failure) {
		AuditLog.emit(() -> {
			AuditPoint point = point("aas_read", endpoint, startedNanos, failure);
			return failure == null ? point.value(AasValues.elementValue(element)) : point;
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
}
