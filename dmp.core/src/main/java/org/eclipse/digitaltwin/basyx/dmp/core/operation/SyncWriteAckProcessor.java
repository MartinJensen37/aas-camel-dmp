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
package org.eclipse.digitaltwin.basyx.dmp.core.operation;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ExchangePattern;
import org.apache.http.HttpStatus;

import com.google.gson.Gson;

/**
 * The synchronous write-delegation leg (W2): request Lua (caller body →
 * device command) → device write (opcua node write / http request) →
 * optional response Lua (device reply → effective value) → ack-then-writeback
 * into the AAS property → the effective-value envelope is the caller
 * response. Failures map to HTTP status codes (errors → 500).
 *
 * @author aas-camel-dmp
 */
public class SyncWriteAckProcessor implements Processor {

	private static final Gson GSON = new Gson();

	private final ProducerTemplate producerTemplate;
	private final OperationContext context;
	private final String writebackSinkUri;
	private final String sinkId;

	public SyncWriteAckProcessor(ProducerTemplate producerTemplate, OperationContext context,
			String writebackSinkUri, String sinkId) {
		this.producerTemplate = producerTemplate;
		this.context = context;
		this.writebackSinkUri = writebackSinkUri;
		this.sinkId = sinkId;
	}

	@Override
	public void process(Exchange exchange) {
		try {
			// 1. request-direction Lua transform (caller body -> device command)
			Object transformed = producerTemplate.requestBody(
					context.requestLuaUri, exchange.getMessage().getBody());

			// 2. unwrap the SinkId key and write through to the device; a
			//    single-key envelope equal to the sink id IS the value — UA
			//    node writes take the native scalar, never a JSON envelope
			Map<String, Object> command = OperationPayloads.unwrap(transformed, sinkId);
			Object deviceBody;
			if (command.size() == 1 && command.containsKey(sinkId)) {
				deviceBody = command.get(sinkId);
			} else {
				deviceBody = OperationPayloads.sinkBody(context.protocol, command);
			}
			// CamelMiloAwait: camel-milo completes the exchange only when the
			// header is true — without it the reply is the unmodified input
			// and the node write is fire-and-forget
			Map<String, Object> sinkHeaders = new java.util.HashMap<>();
			sinkHeaders.put(SyncOperationProcessor.CAMEL_HTTP_METHOD, context.restMethod);
			sinkHeaders.put(SyncOperationProcessor.CAMEL_MILO_AWAIT, Boolean.TRUE);
			Object reply = producerTemplate.requestBodyAndHeaders(
					context.commandSinkUri, deviceBody, sinkHeaders);

			// 3. the effective value: the device reply through the response Lua
			//    when authored, else the accepted command — a WRITE-status
			//    reply is not a value, so the milo node write falls back to
			//    the written (accepted) command
			Object effective = reply;
			if (context.responseLuaUri != null && reply != null) {
				effective = producerTemplate.requestBody(context.responseLuaUri, reply);
			}
			if (effective == null || ("opcua".equalsIgnoreCase(context.protocol)
					&& context.responseLuaUri == null)) {
				effective = command.size() == 1 ? command.values().iterator().next() : command;
			}

			// 4. ack-then-writeback: the effective value envelope into the AAS
			Map<String, Object> envelope = new HashMap<>();
			envelope.put(sinkId, effective);
			String json = GSON.toJson(envelope);
			producerTemplate.requestBody(writebackSinkUri, json);

			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_OK);
			exchange.getMessage().setBody(json);
		} catch (RuntimeException e) {
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_INTERNAL_SERVER_ERROR);
			exchange.getMessage().setBody("{\"error\": \"" + e.getMessage() + "\"}");
		}
	}
}
