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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the write-direction normalizer (SPEC §10.2, MQTT → OPC UA):
 * canonical JSON blob output → Milo {@link Variant} body for milo-client.
 */
class OpcUaNormalizerTest {

	private final OpcUaNormalizer normalizer = new OpcUaNormalizer();

	private Exchange exchangeWithBody(Object body) {
		Exchange exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(body);
		return exchange;
	}

	@Test
	void canonicalJsonValueBecomesVariantBody() throws Exception {
		Exchange exchange = exchangeWithBody("{\"nodeId\":\"ns=2;s=State\",\"value\":42.5}");

		normalizer.process(exchange);

		Object body = exchange.getMessage().getBody();
		assertTrue(body instanceof Variant, "body must be a Milo Variant, got " + body);
		assertEquals(42.5, ((Variant) body).getValue());
	}

	@Test
	void canonicalJsonNodeIdCopiedToHeader() throws Exception {
		Exchange exchange = exchangeWithBody("{\"nodeId\":\"ns=2;s=State\",\"value\":\"start\"}");

		normalizer.process(exchange);

		assertEquals("ns=2;s=State", exchange.getMessage().getHeader(OpcUaCanonical.NODE_ID_HEADER, String.class));
		assertEquals("start", ((Variant) exchange.getMessage().getBody()).getValue());
	}

	@Test
	void canonicalJsonWithoutNodeIdLeavesHeaderUnset() throws Exception {
		Exchange exchange = exchangeWithBody("{\"value\":3}");

		normalizer.process(exchange);

		assertEquals(null, exchange.getMessage().getHeader(OpcUaCanonical.NODE_ID_HEADER));
		assertEquals(3, ((Variant) exchange.getMessage().getBody()).getValue());
	}

	@Test
	void booleanValueIsPreserved() throws Exception {
		Exchange exchange = exchangeWithBody("{\"value\":true}");

		normalizer.process(exchange);

		assertEquals(Boolean.TRUE, ((Variant) exchange.getMessage().getBody()).getValue());
	}

	@Test
	void nonJsonStringBodyIsPassedThroughAsTheValue() throws Exception {
		Exchange exchange = exchangeWithBody("just a string");

		normalizer.process(exchange);

		assertEquals("just a string", ((Variant) exchange.getMessage().getBody()).getValue());
	}

	@Test
	void mapBodyIsHandled() throws Exception {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("nodeId", "ns=2;s=State");
		map.put("value", 11);
		Exchange exchange = exchangeWithBody(map);

		normalizer.process(exchange);

		assertEquals(11, ((Variant) exchange.getMessage().getBody()).getValue());
		assertEquals("ns=2;s=State", exchange.getMessage().getHeader(OpcUaCanonical.NODE_ID_HEADER, String.class));
	}

	@Test
	void objectValueWithValueKeyIsNotImplicitlyUnwrapped() throws Exception {
		Map<String, Object> objectValue = new LinkedHashMap<>();
		objectValue.put("value", 11);
		objectValue.put("kind", "business");
		Exchange exchange = exchangeWithBody(objectValue);

		normalizer.process(exchange);

		Object value = ((Variant) exchange.getMessage().getBody()).getValue();
		assertTrue(value instanceof Map<?, ?>);
		assertEquals(objectValue, value);
	}

	@Test
	void nullBodyBecomesNullVariant() throws Exception {
		Exchange exchange = exchangeWithBody(null);

		normalizer.process(exchange);

		assertTrue(((Variant) exchange.getMessage().getBody()).isNull());
	}
}
