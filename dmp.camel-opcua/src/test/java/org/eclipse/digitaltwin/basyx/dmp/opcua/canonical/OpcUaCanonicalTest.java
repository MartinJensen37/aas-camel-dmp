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
package org.eclipse.digitaltwin.basyx.dmp.opcua.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Map;

import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the canonical OPC UA payload form (SPEC §10.6).
 */
class OpcUaCanonicalTest {

	@Test
	void doubleValueYieldsCanonicalMapWithValueStatusTimestamp() {
		DataValue dataValue = new DataValue(new Variant(42.5), StatusCode.GOOD,
				new DateTime(Instant.parse("2026-01-02T03:04:05Z")), null);

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, "ns=2;s=State");

		assertEquals("ns=2;s=State", canonical.get("nodeId"));
		assertEquals(42.5, canonical.get("value"));
		assertEquals(0L, canonical.get("status"));
		assertEquals("2026-01-02T03:04:05Z", canonical.get("timestamp"));
	}

	@Test
	void nodeIdIsOmittedWhenNull() {
		DataValue dataValue = new DataValue(new Variant("idle"), StatusCode.GOOD, null, null);

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertFalse(canonical.containsKey("nodeId"));
		assertEquals("idle", canonical.get("value"));
	}

	@Test
	void badStatusCodeIsPreserved() {
		DataValue dataValue = new DataValue(new Variant(0), new StatusCode(StatusCodes.Bad_NoData), null, null);

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertEquals((long) StatusCodes.Bad_NoData, canonical.get("status"));
	}

	@Test
	void booleanValueIsPreserved() {
		DataValue dataValue = new DataValue(new Variant(true), StatusCode.GOOD, null, null);

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertEquals(Boolean.TRUE, canonical.get("value"));
	}

	@Test
	void byteArrayValueBecomesBase64() {
		DataValue dataValue = new DataValue(new Variant(new byte[] { 1, 2, 3 }), StatusCode.GOOD, null, null);

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertEquals("AQID", canonical.get("value"));
	}

	@Test
	void canonicalJsonIsParseableAndComplete() throws Exception {
		DataValue dataValue = new DataValue(new Variant(7), StatusCode.GOOD,
				new DateTime(Instant.parse("2026-01-02T03:04:05Z")), null);

		String json = OpcUaCanonical.toCanonicalJson(dataValue, "ns=2;s=Counter");

		@SuppressWarnings("unchecked")
		Map<String, Object> parsed = new com.fasterxml.jackson.databind.ObjectMapper()
				.readValue(json, Map.class);
		assertEquals("ns=2;s=Counter", parsed.get("nodeId"));
		assertEquals(7, parsed.get("value"));
		assertEquals(0, parsed.get("status"));
		assertEquals("2026-01-02T03:04:05Z", parsed.get("timestamp"));
	}

	@Test
	void toVariantWrapsScalarAndNull() {
		assertEquals(42.5, OpcUaCanonical.toVariant(42.5).getValue());
		assertEquals("start", OpcUaCanonical.toVariant("start").getValue());
		assertTrue(OpcUaCanonical.toVariant(null).isNull());
	}

	@Test
	void sourceTimeTakesPrecedenceOverServerTime() {
		Instant source = Instant.parse("2026-01-02T03:04:05Z");
		Instant server = Instant.parse("2026-05-06T07:08:09Z");
		DataValue dataValue = new DataValue(new Variant(1), StatusCode.GOOD,
				new DateTime(source), new DateTime(server));

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertEquals("2026-01-02T03:04:05Z", canonical.get("timestamp"));
	}

	@Test
	void serverTimeUsedWhenSourceTimeInvalid() {
		DataValue dataValue = new DataValue(new Variant(1), StatusCode.GOOD, DateTime.NULL_VALUE,
				new DateTime(Instant.parse("2026-05-06T07:08:09Z")));

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertEquals("2026-05-06T07:08:09Z", canonical.get("timestamp"));
	}

	@Test
	void nullVariantYieldsNullValue() {
		DataValue dataValue = new DataValue(Variant.NULL_VALUE, StatusCode.GOOD, null, null);

		Map<String, Object> canonical = OpcUaCanonical.toCanonical(dataValue, null);

		assertNull(canonical.get("value"));
	}
}
