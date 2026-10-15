/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.component;

import java.time.Instant;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditPoint;

/** InfluxDB line protocol encoding: {@code measurement,tag=v field=v timestamp-ns}. */
final class LineProtocol {
	private LineProtocol() {
	}

	/** Appends one line; a point without fields is skipped, since InfluxDB rejects it. */
	static void append(StringBuilder out, AuditPoint point) {
		if (point.fields().isEmpty()) return;
		out.append(escape(point.measurement(), ", "));
		for (Map.Entry<String, String> tag : point.tags().entrySet())
			out.append(',').append(escape(tag.getKey(), ",= ")).append('=').append(escape(tag.getValue(), ",= "));
		char separator = ' ';
		for (Map.Entry<String, Object> field : point.fields().entrySet()) {
			out.append(separator).append(escape(field.getKey(), ",= ")).append('=').append(fieldValue(field.getValue()));
			separator = ',';
		}
		Instant time = point.time();
		out.append(' ').append(time.getEpochSecond() * 1_000_000_000L + time.getNano()).append('\n');
	}

	private static String fieldValue(Object value) {
		if (value instanceof Boolean bool) return bool ? "true" : "false";
		if (value instanceof Long number) return number + "i";
		if (value instanceof Double number && Double.isFinite(number)) return number.toString();
		return '"' + String.valueOf(value).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
				.replace("\r", "\\r") + '"';
	}

	/** Backslash-escapes {@code special} characters; newlines, which line protocol cannot carry, become spaces. */
	private static String escape(String text, String special) {
		StringBuilder out = new StringBuilder(text.length());
		for (char c : text.toCharArray()) {
			if (c == '\n' || c == '\r') { out.append(' '); continue; }
			if (c == '\\' || special.indexOf(c) >= 0) out.append('\\');
			out.append(c);
		}
		return out.toString();
	}
}
