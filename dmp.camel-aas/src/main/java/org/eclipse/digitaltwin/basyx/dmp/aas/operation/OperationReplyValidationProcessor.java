package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;

/** Validates the connector-normalized correlated reply before Lua. */
public final class OperationReplyValidationProcessor implements Processor {
	private final AffordanceValidation validation;

	public OperationReplyValidationProcessor(String action, AffordanceValidation validation) {
		this.validation = validation;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Object body = exchange.getMessage().getBody();
		validation.correlatedResponse(body);
	}
}
