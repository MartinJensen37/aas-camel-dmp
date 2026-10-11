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
package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * First step of the declarative operation-delegation route: the BaSyx
 * operation-delegation client POSTs an {@code OperationVariable[]} (JSON
 * ARRAY) body — flatten it into the {@code {idShort: value}} map the mapping
 * Lua expects; direct REST callers (object bodies) pass through unchanged.
 *
 * @author aas-camel-dmp
 */
public class OperationRequestProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationRequestProcessor.class);
	private final String operationSourceId;

	public OperationRequestProcessor() {
		this(null);
	}

	public OperationRequestProcessor(String operationSourceId) {
		this.operationSourceId = operationSourceId;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Object body = decodeRestJson(exchange.getMessage().getBody());
		if (OperationPayloads.isOperationVariables(body)) {
			LOGGER.debug("BaSyx delegation body detected — flattening OperationVariable[]");
			exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, true);
			body = OperationPayloads.flattenOperationVariables(body);
		}
		if (operationSourceId != null && !operationSourceId.isBlank()) {
			if (body instanceof java.util.Map<?, ?> map && !map.containsKey(operationSourceId)) {
				java.util.Map<String, Object> keyed = new java.util.LinkedHashMap<>();
				keyed.put(operationSourceId, body);
				body = keyed;
			}
		}
		exchange.getMessage().setBody(JsonCodec.copyValue(body));
	}

	private static Object decodeRestJson(Object body) throws java.io.IOException {
		if (body instanceof String text) return JsonCodec.parseJson(text);
		if (body instanceof byte[] bytes) {
			return JsonCodec.parseJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
		}
		if (body instanceof java.io.InputStream stream) {
			return JsonCodec.parseJson(new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
		}
		return body;
	}
}
