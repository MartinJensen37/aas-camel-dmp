/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

/**
 * AAS valueTypes as InfluxDB field types. InfluxDB fixes a field's type per measurement, so a value is written as
 * the type its Record field declares, never as whatever type Lua happened to return.
 */
public final class InfluxValues {
	/** The InfluxDB field type a valueType is stored as. */
	public enum Kind { STRING, INTEGER, FLOAT, BOOLEAN }

	private InfluxValues() { }

	/** The field type of an AAS valueType, or {@code null} when a Record field cannot have it. */
	public static Kind kind(String valueType) {
		if (valueType == null) return null;
		return switch (valueType) {
			case "xs:string", "xs:normalizedString", "xs:token", "xs:anyURI", "xs:dateTime", "xs:date", "xs:time",
					"xs:duration", "xs:gYear", "xs:gYearMonth", "xs:gMonth", "xs:gMonthDay", "xs:gDay" -> Kind.STRING;
			case "xs:integer", "xs:long", "xs:int", "xs:short", "xs:byte", "xs:nonNegativeInteger", "xs:positiveInteger",
					"xs:nonPositiveInteger", "xs:negativeInteger", "xs:unsignedLong", "xs:unsignedInt", "xs:unsignedShort",
					"xs:unsignedByte" -> Kind.INTEGER;
			case "xs:double", "xs:float", "xs:decimal" -> Kind.FLOAT;
			case "xs:boolean" -> Kind.BOOLEAN;
			default -> null;
		};
	}

	/** {@code value} as the field type of {@code column}; a value of another type is rejected. */
	public static Object field(RecordColumn column, Object value) {
		return switch (kind(column.valueType())) {
			case STRING -> {
				if (value instanceof String text) yield text;
				throw mismatch(column, value);
			}
			case BOOLEAN -> {
				if (value instanceof Boolean flag) yield flag;
				throw mismatch(column, value);
			}
			case FLOAT -> {
				if (value instanceof Number number && !(value instanceof BigInteger)) yield number.doubleValue();
				if (value instanceof BigInteger number) yield number.doubleValue();
				throw mismatch(column, value);
			}
			case INTEGER -> {
				if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
					yield ((Number) value).longValue();
				if (value instanceof BigInteger number) yield number.longValueExact();
				if (value instanceof Number number) {
					BigDecimal decimal = new BigDecimal(number.toString());
					if (decimal.stripTrailingZeros().scale() <= 0) yield decimal.longValueExact();
				}
				throw mismatch(column, value);
			}
		};
	}

	/**
	 * A value read back from InfluxDB as the Lua value of its Record field. InfluxDB answers with the type the
	 * point was written as; a value of another type than the field declares was not written by this profile.
	 */
	public static Object read(RecordColumn column, Object value) {
		Kind kind = kind(column.valueType());
		boolean fits = switch (kind) {
			case STRING -> value instanceof String;
			case BOOLEAN -> value instanceof Boolean;
			case INTEGER -> value instanceof Long || value instanceof Integer;
			case FLOAT -> value instanceof Double || value instanceof Float;
		};
		if (!fits) throw mismatch(column, value);
		return switch (kind) {
			case INTEGER -> ((Number) value).longValue();
			case FLOAT -> ((Number) value).doubleValue();
			default -> value;
		};
	}

	/** A Flux string literal; {@code ${} is escaped, so nothing is interpolated. */
	public static String flux(String text) {
		return '"' + text.replace("\\", "\\\\").replace("\"", "\\\"").replace("${", "\\${") + '"';
	}

	/** A row's time from its Record {@code Time} (xs:dateTime with an offset, e.g. {@code 2026-10-21T12:00:00Z}). */
	public static Instant time(Object value) {
		if (!(value instanceof String text))
			throw new IllegalArgumentException("the row's Time must be an xs:dateTime string, not " + typeName(value));
		try { return OffsetDateTime.parse(text).toInstant(); }
		catch (DateTimeParseException invalid) {
			throw new IllegalArgumentException("the row's Time is not an xs:dateTime with an offset: " + text);
		}
	}

	private static IllegalArgumentException mismatch(RecordColumn column, Object value) {
		return new IllegalArgumentException("Record field " + column.path() + " is " + column.valueType()
				+ ", not " + typeName(value));
	}

	private static String typeName(Object value) {
		return value == null ? "null" : value.getClass().getSimpleName();
	}
}
