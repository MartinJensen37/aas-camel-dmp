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
package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseProjectionProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import java.net.HttpURLConnection;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditLog;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;

/**
 * AAS Operation accept/invoke/reply flow. The source connector owns the AAS
 * request/envelope; a target {@code SynchronousInvocationAdapter} owns the
 * device request/reply wire format and returns named values.
 *
 * <p>
 * Flow per invocation:
 * <ol>
 * <li>run the request-direction Lua blob (REST operation variables → named
 * command payload), keyed by the authored SourceId;</li>
 * <li>select authored SinkId values, assemble one action payload, and invoke the command sink
 * synchronously through the selected connector adapter;</li>
 * <li>validate the correlated reply, project authored ResponseSources, run
 * response Lua once, and return the caller's SinkId while dispatching other
 * per-SinkId values independently.</li>
 * </ol>
 *
 * @author aas-camel-dmp
 */
public class SyncOperationProcessor implements Processor, ManagedRouteResource {
	private static final String CALLER_REPLY_RESULT = "dmp.operation.callerReplyResult";
	private static final String AUDIT_INVOCATION = "dmp.operation.auditInvocation";
	private static final String AUDIT_STARTED = "dmp.operation.auditStarted";

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncOperationProcessor.class);

	private final OperationContext context;
	private final CamelContext camelContext;
	private final ProducerTemplate producerTemplate;
	private final AffordanceValidation validation;
	private final OperationReplyValidationProcessor replyValidationProcessor;
	private final java.util.List<java.util.Map<String, Object>> responseSourceBindings;
	private final java.util.List<java.util.Map<String, Object>> responseSinkTargets;
	private final ResponseSinkFanOutProcessor responseFanOut;
	private final java.util.List<SynchronousInvocationAdapter> invocationAdapters;
	private final SynchronousInvocationAdapter.InvocationSchema invocationSchema;
	private volatile Map<String, String> auditTags = Map.of();

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context) {
		this(camelContext, context, new AffordanceValidation(null, "operation " + context.action),
				new AffordanceValidation(null, "operation output " + context.action));
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation) {
		this(camelContext, context, validation,
				new AffordanceValidation(null, "operation output " + context.action));
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation) {
		this(camelContext, context, validation, responseValidation, java.util.List.of(), java.util.List.of());
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation,
			java.util.List<java.util.Map<String, Object>> responseSourceBindings,
			java.util.List<java.util.Map<String, Object>> responseSinkTargets) {
		this(camelContext, context, validation, responseValidation, responseSourceBindings,
				responseSinkTargets, context.action);
	}

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation,
			java.util.List<java.util.Map<String, Object>> responseSourceBindings,
			java.util.List<java.util.Map<String, Object>> responseSinkTargets, String resourceIdentity) {
		this(camelContext, context, validation, responseValidation, responseSourceBindings,
				responseSinkTargets, resourceIdentity, null);
	}

	SyncOperationProcessor(CamelContext camelContext, OperationContext context,
			AffordanceValidation validation, AffordanceValidation responseValidation,
			java.util.List<java.util.Map<String, Object>> responseSourceBindings,
			java.util.List<java.util.Map<String, Object>> responseSinkTargets, String resourceIdentity,
			java.util.List<SynchronousInvocationAdapter> adapters) {
		this.context = context;
		this.camelContext = camelContext;
		this.validation = validation;
		this.replyValidationProcessor = new OperationReplyValidationProcessor(context.action, responseValidation);
		this.responseSourceBindings = java.util.List.copyOf(responseSourceBindings);
		this.responseSinkTargets = java.util.List.copyOf(responseSinkTargets);
		this.invocationSchema = new SynchronousInvocationAdapter.InvocationSchema(context.parameterTypes,
				context.parameterRequired, context.responseTypes, context.responseRequired, context.targetOptions);
		this.invocationAdapters = adapters == null ? java.util.ServiceLoader
				.load(SynchronousInvocationAdapter.class,
						Thread.currentThread().getContextClassLoader())
				.stream().map(java.util.ServiceLoader.Provider::get).toList() : java.util.List.copyOf(adapters);
		this.responseFanOut = new ResponseSinkFanOutProcessor(camelContext,
				resourceIdentity, this.responseSinkTargets);
		this.producerTemplate = camelContext.createProducerTemplate();
	}

	@Override
	public void bind(CamelContext owner) {
		if (camelContext != owner) throw new IllegalArgumentException("operation processor belongs to another Camel context");
	}

	@Override
	public java.util.Set<String> endpointUris() {
		return invocationAdapters.stream().filter(adapter -> adapter.supports(context.commandSinkUri)).findFirst()
				.map(adapter -> adapter.ownedEndpointUris(context.commandSinkUri, invocationSchema))
				.orElseGet(() -> java.util.Set.of(context.commandSinkUri));
	}

	@Override
	public void close() throws Exception {
		responseFanOut.close();
		producerTemplate.stop();
	}

	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		responseFanOut.drain(timeout);
	}

	@Override
	public void process(Exchange exchange) {
		try {
			prepareRequest(exchange);
			Object transformed = exchange.getMessage().getBody();
			for (String transformerUri : context.requestTransformerUris) {
				transformed = producerTemplate.requestBody(transformerUri, transformed);
			}
			exchange.getMessage().setBody(transformed);
			invokeAndPrepareReply(exchange);
			if (context.responseLuaUri != null) {
				exchange.getMessage().setBody(producerTemplate.requestBody(
						context.responseLuaUri, exchange.getMessage().getBody()));
			}
			prepareResponseWrites(exchange);
			@SuppressWarnings("unchecked")
			java.util.List<ResponseSinkFanOutProcessor.DispatchJob> jobs =
					(java.util.List<ResponseSinkFanOutProcessor.DispatchJob>) exchange.getMessage().getBody();
			jobs.forEach(responseFanOut::enqueueJob);
			completeReply(exchange);
		} catch (Exception e) {
			if (!exchange.isRouteStop()) fail(exchange, e);
		}
	}

	/** The Operation's identity in the interaction log (see {@link ProtocolBridgeRouteConfiguration#getAuditTags()}). */
	void setAuditTags(Map<String, String> tags) {
		this.auditTags = tags == null ? Map.of() : Map.copyOf(tags);
	}

	public void prepareRequest(Exchange exchange) throws Exception {
		String invocation = java.util.UUID.randomUUID().toString();
		exchange.setProperty(AUDIT_INVOCATION, invocation);
		exchange.setProperty(AUDIT_STARTED, System.nanoTime());
		new OperationRequestProcessor(context.operationSourceId, context.callerInputTemplates).process(exchange);
		Object request = exchange.getMessage().getBody();
		boolean basyx = Boolean.TRUE.equals(exchange.getProperty(OperationPayloads.BASYX_DELEGATION_BODY, Boolean.class));
		AuditLog.emit(() -> auditPoint("aas_invoke", invocation).tag("caller", basyx ? "basyx" : "rest")
				.value(request instanceof Map<?, ?> keyed && keyed.containsKey(context.operationSourceId)
						? keyed.get(context.operationSourceId) : request));
	}

	public void invokeAndPrepareReply(Exchange exchange) throws Exception {
			// 1. request-direction Lua transform (REST -> native command payload)
			Object transformed = exchange.getMessage().getBody();
			transformed = applySinkTransformers(transformed);

			// 2. unwrap the SinkId (action) key and invoke the named command
			//    through the endpoint's registered connector adapter.
			Map<String, Object> command = OperationPayloads.assembleCommand(transformed,
					context.commandSinkIds, context.commandSinkValuePaths, context.commandSinkId);
			// parse every DECLARED parameter per its AID DataSchema type —
			// the wire representation is data-driven, never name-based
			OperationPayloads.coerceFields(command, context.parameterTypes);
			try {
				validation.outgoing(command);
			} catch (IllegalArgumentException invalid) {
				throw new OperationValidationException("DMP_COMMAND_VALIDATION_FAILED", 500,
						"Mapped command violates the declared input schema",
					java.util.List.of(invalid.getMessage()));
			}
			SynchronousInvocationAdapter connector = invocationAdapters.stream()
					.filter(candidate -> candidate.supports(context.commandSinkUri)).findFirst().orElse(null);
			LOGGER.debug("sync '{}': invoking declared command endpoint", context.action);
			Object reply = connector == null
					? producerTemplate.requestBody(context.commandSinkUri, command)
					: connector.invoke(exchange.getContext(), context.commandSinkUri, command,
							invocationSchema);
			LOGGER.debug("sync '{}': reply type={}", context.action,
					reply == null ? "null" : reply.getClass().getName());

			// Connector adapters normalize replies to a named logical object;
			// the generic response path validates/projects/transforms it once.
			Exchange replyExchange = new org.apache.camel.support.DefaultExchange(exchange.getContext());
			replyExchange.getMessage().setBody(reply);
			replyValidationProcessor.process(replyExchange);
			Object normalizedReply = replyExchange.getMessage().getBody();
			Object responseInput = responseSourceBindings.isEmpty() ? normalizedReply
					: ResponseProjectionProcessor.project(normalizedReply, responseSourceBindings);
			exchange.getMessage().setBody(responseInput);
	}

	public void prepareResponseWrites(Exchange exchange) {
			Object result = exchange.getMessage().getBody();
			if (context.explicitResponseMapping) {
				OperationPayloads.requireSinkValue(result, context.callerReplySinkId);
			}
			exchange.setProperty(CALLER_REPLY_RESULT, result);
			if (responseSinkTargets.isEmpty()) {
				exchange.getMessage().setBody(java.util.List.of());
			} else {
				exchange.getMessage().setBody(result);
				exchange.getMessage().setBody(responseFanOut.prepareJobs(exchange));
			}
	}

	public void dispatchResponseWriteJob(Exchange exchange) {
		responseFanOut.sendJob(exchange);
	}

	public void recordResponseWriteRejection(Exchange exchange) {
		responseFanOut.recordRejection(exchange);
	}

	public java.util.concurrent.ExecutorService responseWriteExecutor() {
		return responseFanOut.responseExecutor();
	}

	public void completeReply(Exchange exchange) throws Exception {
			Object result = exchange.getProperty(CALLER_REPLY_RESULT);
			exchange.getMessage().setBody(result);
			new OperationReplyProcessor(context).process(exchange);
			AuditLog.emit(() -> reply(exchange, "ok", HttpURLConnection.HTTP_OK, null).value(result));
	}

	public void handleFailure(Exchange exchange) {
		Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
		if (failure == null) failure = exchange.getException();
		if (failure == null) failure = new IllegalStateException("operation route failed without a cause");
		fail(exchange, failure);
	}

	private void fail(Exchange exchange, Throwable failure) {
		exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);
		new OperationFailureProcessor(context, HttpURLConnection.HTTP_INTERNAL_ERROR).process(exchange);
		exchange.setRouteStop(true);
		AuditLog.emit(() -> {
			Object status = exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE);
			Object body = exchange.getMessage().getBody();
			Object code = null;
			if (body instanceof String text) {
				try { body = org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec.parseJson(text); }
				catch (RuntimeException notJson) { body = null; }
			}
			if (body instanceof Map<?, ?> error) code = error.get("code");
			return reply(exchange, "failed", status instanceof Number number ? number.intValue() : null,
					code == null ? null : String.valueOf(code));
		});
	}

	private AuditPoint reply(Exchange exchange, String outcome, Integer status, String errorCode) {
		Object started = exchange.getProperty(AUDIT_STARTED);
		AuditPoint point = auditPoint("aas_reply", exchange.getProperty(AUDIT_INVOCATION, String.class))
				.tag("outcome", outcome).field("errorCode", errorCode)
				.field("httpStatus", status == null ? null : status.longValue());
		if (started instanceof Long nanos) point.field("durationMs", (System.nanoTime() - nanos) / 1_000_000d);
		return point;
	}

	private AuditPoint auditPoint(String measurement, String invocation) {
		AuditPoint point = AuditPoint.of(measurement);
		auditTags.forEach(point::tag);
		return point.field("invocation", invocation);
	}

	private Object applySinkTransformers(Object transformed) {
		if (context.sinkTransformerUris.isEmpty()) return transformed;
		Map<String, Object> values = OperationPayloads.keyedValues(transformed);
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		for (String sinkId : context.commandSinkIds) {
			Object sinkValue;
			java.util.List<String> chain = context.sinkTransformerUris.get(sinkId);
			if (chain != null) {
				sinkValue = values;
				for (String transformerUri : chain) {
					sinkValue = producerTemplate.requestBody(transformerUri, sinkValue);
				}
				sinkValue = OperationPayloads.requireCommandSinkValue(sinkValue, sinkId);
			} else {
				sinkValue = OperationPayloads.requireCommandSinkValue(values, sinkId);
			}
			result.put(sinkId, sinkValue);
		}
		return result;
	}

}
