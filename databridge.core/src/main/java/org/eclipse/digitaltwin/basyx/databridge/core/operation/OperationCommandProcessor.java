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
package org.eclipse.digitaltwin.basyx.databridge.core.operation;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Prepares the native command message of the declarative operation-delegation
 * route (after the request-direction Lua transform):
 * <ol>
 * <li>unwrap the SinkId (action) key from the Lua envelope;</li>
 * <li>coerce every DECLARED parameter (the AID action's embedded input
 * DataSchema) per its declared type — the wire representation is data-driven,
 * never name-based;</li>
 * <li>stringify the correlation field deterministically (REST/UI clients may
 * pass a bare number; native stations validate {@code Uuid} against the
 * command schema and drop non-strings) — a CALLER-SUPPLIED value is preserved
 * (the station's occupation gating matches skills against the occupier's
 * registered key), an absent one is filled with a fresh UUID;</li>
 * <li>set the sink body per the protocol encoding.</li>
 * </ol>
 *
 * @author aas-camel-dmp
 */
public class OperationCommandProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationCommandProcessor.class);

	private final OperationContext context;

	public OperationCommandProcessor(OperationContext context) {
		this.context = context;
	}

	@Override
	public void process(Exchange exchange) {
		Object transformed = exchange.getMessage().getBody();
		Map<String, Object> command = OperationPayloads.unwrap(transformed, context.action);
		OperationPayloads.coerceFields(command, context.parameterTypes);

		// Correlation is a declared payload field, not a generic operation
		// policy. Preserve it when authored and do not inject Uuid into an
		// otherwise opaque command.
		if (command.containsKey("Uuid") || context.parameterTypes.containsKey("Uuid")) {
			String uuid = OperationPayloads.uuidString(command.get("Uuid"));
			if (uuid != null) {
				command.put("Uuid", uuid);
			}
		}

		// camel-http: the DECLARED invocation method rides the Camel header
		if ("http".equalsIgnoreCase(context.protocol)) {
			exchange.getMessage().setHeader(SyncOperationProcessor.CAMEL_HTTP_METHOD,
					context.restMethod);
		}
		exchange.getMessage().setBody(OperationPayloads.sinkBody(context.protocol, command));
		LOGGER.info("operation {}: command prepared {}", context.action, command);
	}
}
