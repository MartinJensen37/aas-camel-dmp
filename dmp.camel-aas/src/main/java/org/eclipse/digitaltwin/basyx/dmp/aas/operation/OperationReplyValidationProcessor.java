package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;

/** Normalizes the device's correlated reply and validates it before Lua. */
public final class OperationReplyValidationProcessor implements Processor {
	private final String action;
	private final AffordanceValidation validation;

	public OperationReplyValidationProcessor(String action, AffordanceValidation validation) {
		this.action = action;
		this.validation = validation;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Object body = exchange.getMessage().getBody();
		Object value = body;
		if (value instanceof Map<?, ?> map && map.containsKey(action)) {
			value = map.get(action);
		}
		validation.correlatedResponse(value);
		exchange.getMessage().setBody(value);
	}
}
