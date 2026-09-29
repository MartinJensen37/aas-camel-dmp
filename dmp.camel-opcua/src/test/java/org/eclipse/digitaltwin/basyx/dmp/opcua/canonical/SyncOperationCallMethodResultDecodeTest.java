/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.opcua.canonical;

import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationContext;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.SyncOperationProcessor;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The awaited UA Call decode END TO END: the {@code SyncOperationProcessor}
 * (core) + the canonical {@code CallMethodResult} converter (this module) +
 * the DECLARED positional reply order — the exact production chain of the
 * opcua method-call bridge route.
 */
class SyncOperationCallMethodResultDecodeTest extends CamelTestSupport {

	private static final String ACTION = "Dispensing";
	private static final String REPLY_ENDPOINT = "direct:fakeMethodSink";
	private static final String REQUEST_LUA = "direct:requestLua";

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				// the request-direction Lua: flat REST body → action envelope
				from(REQUEST_LUA).process((Exchange exchange) -> {
					String flat = exchange.getMessage().getBody(String.class);
					exchange.getMessage().setBody("{\"" + ACTION + "\": " + flat + "}");
				});
				// the awaited UA Call: camel-milo leaves the raw
				// CallMethodResult on the body (CamelMiloAwait=true)
				from(REPLY_ENDPOINT).process((Exchange exchange) -> {
					exchange.getMessage().setBody(new org.eclipse.milo.opcua.stack.core.types.structured.CallMethodResult(
							StatusCode.GOOD,
							new StatusCode[] { StatusCode.GOOD },
							new org.eclipse.milo.opcua.stack.core.types.builtin.DiagnosticInfo[] { null },
							new Variant[] { new Variant("SUCCESS"), new Variant(0) }));
				});
			}
		};
	}

	private SyncOperationProcessor processor(OperationContext context) {
		return new SyncOperationProcessor(context(), context);
	}

	@Test
	void awaitedCallResultZipsPositionallyWithDeclaredOrder() throws Exception {
		OperationContext context = new OperationContext(ACTION,
				REQUEST_LUA, null, REPLY_ENDPOINT, 30L, "POST", "opcua",
				Map.of("Uuid", "string"), Map.of("State", "string", "Outcome", "integer"),
				List.of("State", "Outcome"));

		Exchange exchange = new org.apache.camel.support.DefaultExchange(context());
		exchange.getIn().setBody("{\"Uuid\": \"ua-1\"}");
		processor(context).process(exchange);

		String reply = exchange.getMessage().getBody(String.class);
		assertNotNull(reply);
		assertTrue(reply.contains("\"State\":\"SUCCESS\""), reply);
		assertTrue(reply.contains("\"Outcome\":0"), reply);
	}
}