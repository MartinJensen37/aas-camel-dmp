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

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Write-direction adapter (SPEC §10.1 / §10.2): turns the canonical JSON
 * payload produced by a Lua blob into the Milo {@link Variant} the milo-client
 * producer writes to the OPC UA node.
 *
 * <p>
 * The milo-client producer reads the raw message body ({@code getBody()} — it
 * does <em>not</em> run Camel type converters). The canonical blob output
 *
 * <pre>
 * { "nodeId": "ns=2;s=State", "value": 42 }
 * </pre>
 *
 * is therefore normalized here: the {@code value} field is extracted into a
 * {@link Variant} (set as the new body) and — when present — the
 * {@code nodeId} is copied to the {@code CamelMiloNodeId} header.
 *
 * <p>
 * Route usage:
 *
 * <pre>
 * from("paho:aas/&lt;asset&gt;/cmd?brokerUrl=...")
 *     .to("lua:file:/etc/dmp/mqtt2opc.lua")
 *     .process(new OpcUaNormalizer())
 *     .to("milo-client:opc.tcp://&lt;asset&gt;:4840?node=RAW(ns=2;s=State)")
 * </pre>
 *
 * @author aas-camel-dmp
 */
public class OpcUaNormalizer implements Processor {

	private final ObjectMapper mapper = new ObjectMapper();

	@Override
	public void process(Exchange exchange) throws Exception {
		Object body = exchange.getMessage().getBody();
		if (body == null) {
			exchange.getMessage().setBody(Variant.ofNull());
			return;
		}

		Object value = body;
		if (body instanceof String text) {
			CanonicalParts parts = extractCanonical(text);
			value = parts.value;
			if (parts.nodeId != null) {
				exchange.getMessage().setHeader(OpcUaCanonical.NODE_ID_HEADER, parts.nodeId);
			}
		} else if (body instanceof Map<?, ?> map && map.containsKey("value")) {
			value = map.get("value");
			Object nodeId = map.get("nodeId");
			if (nodeId != null) {
				exchange.getMessage().setHeader(OpcUaCanonical.NODE_ID_HEADER, String.valueOf(nodeId));
			}
		}

		exchange.getMessage().setBody(OpcUaCanonical.toVariant(value));
	}

	private CanonicalParts extractCanonical(String text) {
		try {
			Map<?, ?> map = mapper.readValue(text, Map.class);
			Object nodeId = map.get("nodeId");
			return new CanonicalParts(nodeId != null ? String.valueOf(nodeId) : null, map.get("value"));
		} catch (Exception e) {
			// Not canonical JSON — treat the raw string as the value (e.g. a JSON
			// object that itself is the OPC UA value).
			return new CanonicalParts(null, text);
		}
	}

	private record CanonicalParts(String nodeId, Object value) {
	}
}
