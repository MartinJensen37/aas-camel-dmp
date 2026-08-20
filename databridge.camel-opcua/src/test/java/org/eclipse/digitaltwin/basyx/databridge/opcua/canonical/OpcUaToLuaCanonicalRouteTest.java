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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * M4 route-level test (SPEC §10.1/§10.2): a Milo {@link DataValue} body is
 * normalized to the canonical JSON form by the registered type converter,
 * transformed by the AIMC Lua blob, and emitted as the MQTT-ready payload —
 * exactly the {@code opcua -> lua -> mqtt} interface-transform pattern.
 */
class OpcUaToLuaCanonicalRouteTest extends CamelTestSupport {

	@TempDir
	Path tempDir;

	@Test
	void dataValueThroughLuaBlobYieldsMqttReadyJson() throws Exception {
		Path blob = tempDir.resolve("opc2mqtt.lua");
		Files.writeString(blob, """
				-- AIMC-compliant blob: OPC UA canonical value -> MQTT payload
				-- input:  { "nodeId": ..., "value": <typed>, "status": <code>, "timestamp": <time> }
				-- output: { "state": <value>, "quality": <status> }
				function aimc_main(sources)
				    local src = sources["body"] or {}
				    return {
				        ["state"] = src["value"],
				        ["quality"] = src["status"]
				    }
				end
				""", StandardCharsets.UTF_8);

		String uri = "lua:file:" + blob + "?timeoutSeconds=2";

		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start").to(uri).to("mock:result");
			}
		});
		context.start();

		DataValue dataValue = new DataValue(new Variant("running"), StatusCode.GOOD,
				new DateTime(), null);
		template.sendBody("direct:start", dataValue);

		String result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(String.class);

		assertTrue(result.contains("\"state\":\"running\""), "unexpected output: " + result);
		assertTrue(result.contains("\"quality\":0"), "unexpected output: " + result);
		assertEquals(1, getMockEndpoint("mock:result").getReceivedCounter());
	}
}
