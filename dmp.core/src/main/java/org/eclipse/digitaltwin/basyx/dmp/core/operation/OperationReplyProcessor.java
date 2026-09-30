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
package org.eclipse.digitaltwin.basyx.dmp.core.operation;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

/**
 * Final step of the declarative operation-delegation route: after the
 * response-direction Lua transform, unwrap the SinkId (action) key and answer
 * the caller in the shape it invoked with — a BaSyx operation-delegation
 * caller receives the {@code OperationVariable[]} envelope built from the
 * DECLARED reply types (see {@link OperationPayloads#toOperationVariables});
 * a direct REST caller receives the reply JSON as-is.
 *
 * @author aas-camel-dmp
 */
public class OperationReplyProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationReplyProcessor.class);

	private final OperationContext context;

	public OperationReplyProcessor(OperationContext context) {
		this.context = context;
	}

	@Override
	public void process(Exchange exchange) {
		Object reply = exchange.getMessage().getBody();
		Object unwrapped = OperationPayloads.unwrapValue(reply, context.action);
		Map<String, Object> replyMap = unwrapped instanceof Map<?, ?>
				? OperationPayloads.unwrap(unwrapped, context.action) : null;
		if (replyMap == null && context.responseTypes.size() == 1) {
			String name = context.responseTypes.keySet().iterator().next();
			replyMap = new java.util.LinkedHashMap<>();
			replyMap.put(name, unwrapped);
		}
		boolean basyxDelegation = Boolean.TRUE.equals(
				exchange.getProperty(OperationPayloads.BASYX_DELEGATION_BODY, Boolean.class));
		if (basyxDelegation) {
			exchange.getMessage().setBody(JsonCodec.toJson(
					OperationPayloads.toOperationVariables(coerce(replyMap), context.responseTypes)));
			LOGGER.debug("operation {}: delegation reply {}", context.action, replyMap);
		} else {
			exchange.getMessage().setBody(JsonCodec.toJson(coerce(replyMap)));
		}
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_OK);
	}

	/**
	 * Applies the DECLARED types to the reply before it goes back to the caller,
	 * mirroring the request leg exactly: a declared {@code xs:string} field must
	 * come back as a string even when it looks numeric, and the correlation
	 * Uuid must be stringified for the same reason.
	 *
	 * <p>Without this the two legs disagreed: a caller passing a bare {@code 1}
	 * was published to the device as {@code "1"} (string), but the device's
	 * echoed {@code "1"} came back to that caller as {@code 1.0}.
	 */
	private Map<String, Object> coerce(Map<String, Object> replyMap) {
		if (replyMap == null) {
			return null;
		}
		OperationPayloads.coerceFields(replyMap, context.responseTypes);
		if (replyMap.containsKey("Uuid")) {
			String uuid = OperationPayloads.uuidString(replyMap.get("Uuid"));
			if (uuid != null) {
				replyMap.put("Uuid", uuid);
			}
		}
		return replyMap;
	}
}
