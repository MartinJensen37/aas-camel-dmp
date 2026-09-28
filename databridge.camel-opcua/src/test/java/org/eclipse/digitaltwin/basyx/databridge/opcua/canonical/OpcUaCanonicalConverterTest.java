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
package org.eclipse.digitaltwin.basyx.databridge.opcua.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@code DataValue -> String / Map} canonical type converters,
 * including discovery through
 * {@code META-INF/services/org/apache/camel/TypeConverter} and the
 * {@code CamelMiloNodeId} header enrichment.
 */
class OpcUaCanonicalConverterTest {

	@Test
	void camelTypeConverterRegistryDiscoversDataValueToStringConverter() throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.start();
			Exchange exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(new DataValue(new Variant(3.14), StatusCode.GOOD,
					new DateTime(), null));

			// The very call the Lua component performs:
			// exchange.getMessage().getBody(String.class)
			String json = exchange.getMessage().getBody(String.class);

			assertNotNull(json);
			@SuppressWarnings("unchecked")
			Map<String, Object> parsed = new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, Map.class);
			assertEquals(3.14, parsed.get("value"));
			assertEquals(0, parsed.get("status"));
		}
	}

	@Test
	void camelTypeConverterRegistryDiscoversDataValueToMapConverter() throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.start();
			Exchange exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(new DataValue(new Variant(false), StatusCode.GOOD, null, null));

			Map<String, Object> canonical = exchange.getMessage().getBody(Map.class);

			assertNotNull(canonical);
			assertEquals(Boolean.FALSE, canonical.get("value"));
		}
	}

	@Test
	void camelTypeConverterRegistryDecodesAwaitedCallMethodResult() throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.start();
			Exchange exchange = new DefaultExchange(context);
			// the exact shape camel-milo leaves on the body when the method
			// call is awaited (CamelMiloAwait=true): the raw CallMethodResult
			exchange.getMessage().setBody(new org.eclipse.milo.opcua.stack.core.types.structured.CallMethodResult(
					StatusCode.GOOD,
					new org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode[] { StatusCode.GOOD },
					new org.eclipse.milo.opcua.stack.core.types.builtin.DiagnosticInfo[] { null },
					new Variant[] { new Variant("SUCCESS"), new Variant(0) }));

			// the very call SyncOperationProcessor performs:
			// typeConverter.convertTo(Map.class, reply)
			Map<String, Object> canonical = context.getTypeConverter()
					.convertTo(Map.class, exchange.getMessage().getBody());

			assertNotNull(canonical, "the awaited CallMethodResult must decode canonically");
			@SuppressWarnings("unchecked")
			java.util.List<Object> outputs = (java.util.List<Object>) canonical.get("outputArguments");
			assertEquals(java.util.List.of("SUCCESS", 0), outputs);
			@SuppressWarnings("unchecked")
			Map<String, Object> status = (Map<String, Object>) canonical.get("statusCode");
			assertEquals(Boolean.TRUE, status.get("good"));
		}
	}

	@Test
	void nodeIdHeaderIsIncludedInCanonicalJson() throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.start();
			Exchange exchange = new DefaultExchange(context);
			exchange.getMessage().setHeader(OpcUaCanonical.NODE_ID_HEADER, "ns=2;s=State");
			exchange.getMessage().setBody(new DataValue(new Variant(1), StatusCode.GOOD, null, null));

			@SuppressWarnings("unchecked")
			Map<String, Object> parsed = new com.fasterxml.jackson.databind.ObjectMapper()
					.readValue(exchange.getMessage().getBody(String.class), Map.class);

			assertEquals("ns=2;s=State", parsed.get("nodeId"));
			assertEquals(1, parsed.get("value"));
		}
	}

	@Test
	void nodeIdOmittedWithoutHeader() throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.start();
			Exchange exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(new DataValue(new Variant(1), StatusCode.GOOD, null, null));

			@SuppressWarnings("unchecked")
			Map<String, Object> parsed = new com.fasterxml.jackson.databind.ObjectMapper()
					.readValue(exchange.getMessage().getBody(String.class), Map.class);

			assertFalse(parsed.containsKey("nodeId"));
		}
	}

	@Test
	void staticConverterMethodsWorkWithoutExchange() {
		DataValue dataValue = new DataValue(new Variant("running"), StatusCode.GOOD, null, null);

		String json = OpcUaCanonicalConverter.toCanonicalJson(dataValue, null);
		assertTrue(json.contains("\"running\""));
	}
}
