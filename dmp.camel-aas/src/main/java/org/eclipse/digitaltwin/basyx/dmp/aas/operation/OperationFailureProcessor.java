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
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadValidationException;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

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
		if (caught instanceof OperationValidationException validation) {
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, validation.getStatus());
			exchange.getMessage().setBody(JsonCodec.toJson(java.util.Map.of(
					"code", validation.getCode(), "error", validation.getMessage(),
					"violations", validation.getViolations())));
		} else if (caught instanceof PayloadValidationException validation) {
			String code;
			int callerStatus;
			switch (validation.getStage()) {
			case COMMAND -> {
				code = "DMP_COMMAND_VALIDATION_FAILED";
				callerStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
			}
			case RESPONSE -> {
				code = "DMP_RESPONSE_VALIDATION_FAILED";
				callerStatus = HttpStatus.SC_BAD_GATEWAY;
			}
			default -> {
				code = "DMP_RESPONSE_MAPPING_FAILED";
				callerStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
			}
			}
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, callerStatus);
			exchange.getMessage().setBody(JsonCodec.toJson(java.util.Map.of(
					"code", code, "error", validation.getMessage(),
					"violations", validation.getViolations())));
		} else if (caught instanceof ConnectorInvocationException connectorFailure) {
			int callerStatus;
			String callerCode;
			switch (connectorFailure.getCategory()) {
			case REMOTE -> {
				callerStatus = HttpStatus.SC_BAD_GATEWAY;
				callerCode = "DMP_DEVICE_ERROR";
			}
			case TIMEOUT -> {
				callerStatus = HttpStatus.SC_GATEWAY_TIMEOUT;
				callerCode = "DMP_DEVICE_TIMEOUT";
			}
			case TRANSPORT -> {
				callerStatus = HttpStatus.SC_BAD_GATEWAY;
				callerCode = "DMP_DEVICE_TRANSPORT_ERROR";
			}
			default -> {
				callerStatus = HttpStatus.SC_INTERNAL_SERVER_ERROR;
				callerCode = "DMP_CONNECTOR_CONFIGURATION_ERROR";
			}
			}
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, callerStatus);
			java.util.Map<String, Object> body = new java.util.LinkedHashMap<>();
			body.put("code", callerCode);
			body.put("error", connectorFailure.getMessage());
			body.put("connectorCode", connectorFailure.getConnectorCode());
			if (!connectorFailure.getDetails().isEmpty()) body.put("details", connectorFailure.getDetails());
			exchange.getMessage().setBody(JsonCodec.toJson(body));
		} else {
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, status);
			exchange.getMessage().setBody(JsonCodec.toJson(java.util.Map.of("error", message)));
		}
	}

}
