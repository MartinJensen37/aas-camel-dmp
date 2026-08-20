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
package org.eclipse.digitaltwin.basyx.databridge.core.operation;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.http.HttpStatus;

import com.google.gson.Gson;

/**
 * Delegates one REST operation invocation through the
 * {@link MqttRequestReplyBridge} and maps failures to HTTP status codes
 * (timeout → 504, other errors → 500) — matching the Python
 * operation-delegation reference.
 *
 * @author aas-camel-dmp
 */
public class OperationInvocationProcessor implements Processor {

	private static final Gson GSON = new Gson();

	private final MqttRequestReplyBridge bridge;
	private final OperationContext context;

	public OperationInvocationProcessor(MqttRequestReplyBridge bridge, OperationContext context) {
		this.bridge = bridge;
		this.context = context;
	}

	@Override
	public void process(Exchange exchange) {
		Object body = exchange.getMessage().getBody();
		try {
			Object result = bridge.invoke(context, body);
			exchange.getMessage().setBody(GSON.toJson(result));
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_OK);
		} catch (OperationDelegationTimeoutException e) {
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_GATEWAY_TIMEOUT);
			exchange.getMessage().setBody("{\"error\": \"" + e.getMessage() + "\"}");
		} catch (RuntimeException e) {
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_INTERNAL_SERVER_ERROR);
			exchange.getMessage().setBody("{\"error\": \"" + e.getMessage() + "\"}");
		}
	}
}
