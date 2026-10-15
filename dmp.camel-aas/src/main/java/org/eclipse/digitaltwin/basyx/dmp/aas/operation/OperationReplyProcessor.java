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


import java.util.Map;
import java.util.LinkedHashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import java.net.HttpURLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		// Only a response transform/mapping creates a SinkId envelope. Native reply
		// fields may themselves have the action's name and must not be unwrapped.
		Object unwrapped = context.explicitResponseMapping
				? OperationPayloads.requireSinkValue(reply, context.callerReplySinkId)
				: context.responseLuaUri == null ? reply
						: OperationPayloads.unwrapValue(reply, context.callerReplySinkId);
		Map<String, Object> replyMap = null;
		if (unwrapped instanceof Map<?, ?> map) {
			replyMap = new LinkedHashMap<>();
			for (var entry : map.entrySet()) {
				replyMap.put(String.valueOf(entry.getKey()), entry.getValue());
			}
		}
		replyMap = OperationPayloads.selectCallerOutputs(replyMap, context.callerOutputOrder);
		boolean basyxDelegation = Boolean.TRUE.equals(
				exchange.getProperty(OperationPayloads.BASYX_DELEGATION_BODY, Boolean.class));
		if (basyxDelegation) {
			java.util.Set<String> declared = new java.util.LinkedHashSet<>(context.callerResponseTypes.keySet());
			declared.addAll(context.callerOutputTemplates.keySet());
			if (replyMap == null && declared.size() == 1) {
				String name = declared.iterator().next();
				replyMap = new java.util.LinkedHashMap<>();
				replyMap.put(name, unwrapped);
			}
			exchange.getMessage().setBody(
					OperationPayloads.toOperationVariables(coerce(replyMap), context.callerResponseTypes,
							context.callerOutputOrder, context.callerOutputTemplates));
			LOGGER.debug("Operation delegation reply prepared");
		} else {
			exchange.getMessage().setBody(replyMap == null ? unwrapped : coerce(replyMap));
		}
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpURLConnection.HTTP_OK);
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
		OperationPayloads.coerceFields(replyMap, context.callerResponseTypes);
		context.callerOutputTemplates.forEach((name, template) -> {
			if (replyMap.containsKey(name)) replyMap.put(name, OperationVariableValues.coerce(template, replyMap.get(name)));
		});
		return replyMap;
	}
}
