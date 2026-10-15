/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.core.audit;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;

/**
 * One logged AAS interaction: a measurement, identity tags and typed fields.
 * Values go into a field per kind ({@code value_num}, {@code value_bool},
 * {@code value_str}, {@code value_json}) because InfluxDB fixes a field's type
 * per measurement.
 */
public final class AuditPoint {
	private final String measurement;
	private final Instant time;
	private final Map<String, String> tags = new TreeMap<>();
	private final Map<String, Object> fields = new TreeMap<>();

	private AuditPoint(String measurement, Instant time) {
		this.measurement = measurement;
		this.time = time;
	}

	public static AuditPoint of(String measurement) {
		return new AuditPoint(measurement, Instant.now());
	}

	/** Adds a tag; blank values are left out. */
	public AuditPoint tag(String key, String value) {
		if (value != null && !value.isBlank()) tags.put(key, value);
		return this;
	}

	/** Adds a field: a String, Boolean, Long (integer) or Double; null is left out. */
	public AuditPoint field(String key, Object value) {
		if (value instanceof Integer || value instanceof Short || value instanceof Byte) value = ((Number) value).longValue();
		if (value != null) fields.put(key, value);
		return this;
	}

	/** Adds a logged value in its type-specific field. */
	public AuditPoint value(Object value) {
		if (value == null || value == JsonPointer.JSON_NULL) return field("value_null", Boolean.TRUE);
		if (value instanceof Boolean bool) return field("value_bool", bool);
		if (value instanceof Number number) {
			double decimal = number instanceof BigDecimal big ? big.doubleValue()
					: number instanceof BigInteger big ? big.doubleValue() : number.doubleValue();
			return Double.isFinite(decimal) ? field("value_num", decimal) : field("value_str", number.toString());
		}
		if (value instanceof String text) return field("value_str", text);
		if (value instanceof Map<?, ?> || value instanceof List<?>) return field("value_json", JsonCodec.toJson(value));
		return field("value_str", String.valueOf(value));
	}

	public String measurement() { return measurement; }
	public Instant time() { return time; }
	public Map<String, String> tags() { return Collections.unmodifiableMap(tags); }
	public Map<String, Object> fields() { return Collections.unmodifiableMap(fields); }

	@Override public String toString() {
		return "AuditPoint[" + measurement + ", tags=" + tags + ", fields=" + fields.keySet() + "]";
	}
}
