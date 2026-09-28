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

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.http.HttpStatus;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Ack-then-writeback tail of a write-delegation route (W2): sends the
 * effective value envelope {@code {SinkId: value}} to the AAS writeback
 * sink and returns the same envelope to the caller. The MQTT leg feeds it
 * the response-direction Lua result after the MQTT 5 InOut exchange.
 *
 * @author aas-camel-dmp
 */
public class WritebackProcessor implements Processor {

	private static final Gson GSON = new Gson();
	private static final java.lang.reflect.Type MAP_TYPE =
			new TypeToken<Map<String, Object>>() { }.getType();

	private final ProducerTemplate producerTemplate;
	private final String writebackSinkUri;
	private final String sinkId;

	public WritebackProcessor(ProducerTemplate producerTemplate, String writebackSinkUri, String sinkId) {
		this.producerTemplate = producerTemplate;
		this.writebackSinkUri = writebackSinkUri;
		this.sinkId = sinkId;
	}

	@Override
	public void process(Exchange exchange) {
		// a failed delegation never writes back (no effective value exists)
		if (Boolean.TRUE.equals(
				exchange.getProperty(OperationPayloads.OPERATION_DELEGATION_FAILED))) {
			return;
		}
		Object body = exchange.getMessage().getBody();
		// the response blob's contract is the single-key envelope
		// {SinkId: effectiveValue} — when the bridge result already IS that
		// envelope, unwrap it (a blind re-wrap stringifies the value)
		Object value = unwrapEnvelope(body, sinkId);
		Map<String, Object> envelope = new HashMap<>();
		envelope.put(sinkId, value == null ? "" : value);
		String json = GSON.toJson(envelope);
		try {
			producerTemplate.requestBody(writebackSinkUri, json);
		} catch (RuntimeException e) {
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_INTERNAL_SERVER_ERROR);
			exchange.getMessage().setBody("{\"error\": \"writeback failed: " + e.getMessage() + "\"}");
			return;
		}
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_OK);
		exchange.getMessage().setBody(json);
	}

	@SuppressWarnings("unchecked")
	private static Object unwrapEnvelope(Object body, String sinkId) {
		Map<String, Object> map = null;
		if (body instanceof Map<?, ?> m) {
			map = (Map<String, Object>) m;
		} else if (body instanceof String s) {
			try {
				map = GSON.fromJson(s, MAP_TYPE);
			} catch (RuntimeException e) {
				return body;
			}
		}
		if (map != null && map.size() == 1 && map.containsKey(sinkId)) {
			return map.get(sinkId);
		}
		return body;
	}
}
