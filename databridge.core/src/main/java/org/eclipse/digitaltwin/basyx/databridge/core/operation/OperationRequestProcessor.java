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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
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

	@Override
	public void process(Exchange exchange) {
		String body = exchange.getMessage().getBody(String.class);
		if (OperationPayloads.isJsonArrayBody(body)) {
			LOGGER.debug("BaSyx delegation body detected — flattening OperationVariable[]");
			exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, true);
			body = OperationPayloads.flattenOperationVariables(
					String.valueOf(body));
			exchange.getMessage().setBody(body);
		}
	}
}
