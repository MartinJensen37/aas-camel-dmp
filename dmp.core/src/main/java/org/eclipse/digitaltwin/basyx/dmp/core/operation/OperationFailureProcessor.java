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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Exception mapping of the declarative operation-delegation route
 * ({@code doCatch}): marks the delegation failed (the writeback must NOT
 * store an error payload as the effective value) and answers the caller with
 * the status the route creator wired — a correlation timeout maps to 504,
 * any other failure to 500 (matching the Python operation-delegation
 * reference).
 *
 * @author aas-camel-dmp
 */
public class OperationFailureProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationFailureProcessor.class);
	private static final Gson GSON = new Gson();

	private final OperationContext context;
	private final int status;

	public OperationFailureProcessor(OperationContext context, int status) {
		this.context = context;
		this.status = status;
	}

	@Override
	public void process(Exchange exchange) {
		Exception caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
		String message = caught == null || caught.getMessage() == null
				? "unknown failure" : caught.getMessage();
		LOGGER.warn("operation {} failed ({}): {}", context.action, status, message);
		exchange.setProperty(OperationPayloads.OPERATION_DELEGATION_FAILED, true);
		exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, status);
		exchange.getMessage().setBody(GSON.toJson(java.util.Map.of("error", message)));
	}
}
