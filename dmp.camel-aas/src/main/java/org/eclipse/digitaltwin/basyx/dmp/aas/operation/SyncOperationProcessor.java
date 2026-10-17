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

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseProjectionProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.SinkJobs;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
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
public class SyncOperationProcessor implements ManagedRouteResource {
	private static final String CALLER_REPLY_RESULT = "dmp.operation.callerReplyResult";
	private static final String AUDIT_INVOCATION = "dmp.operation.auditInvocation";
	private static final String AUDIT_STARTED = "dmp.operation.auditStarted";
	private static final String CALLER_INPUTS = "dmp.operation.callerInputs";
	/** The request transformation's result keyed by SinkId: parameter values and the action command. */
	private static final String REQUEST_RESULT = "dmp.operation.requestResult";
	private static final String COMMAND_REPLY = "dmp.operation.commandReply";
	static final String PARAMETER_JOBS = "dmp.operation.parameterJobs";
	static final String DEVICE_JOBS = "dmp.operation.deviceJobs";
	/** Header naming the write route of one acknowledged write. */
	static final String WRITE_ENDPOINT_HEADER = "dmpWriteEndpoint";

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
	private final java.util.Set<String> deviceEndpoints;
	private volatile Map<String, String> auditTags = Map.of();
	private volatile StatusPhase status;
	private volatile SinkJobs parameterJobs = new SinkJobs("parameters", java.util.List.of());

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
		this.deviceEndpoints = this.responseSinkTargets.stream().filter(target -> Boolean.TRUE.equals(target.get("device")))
				.map(target -> String.valueOf(target.get("endpoint"))).collect(java.util.stream.Collectors.toUnmodifiableSet());
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
		if (status != null) status.close();
		responseFanOut.close();
		producerTemplate.stop();
	}

	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		responseFanOut.drain(timeout);
	}

	/** The Operation's identity in the interaction log (see {@link ProtocolBridgeRouteConfiguration#getAuditTags()}). */
	void setAuditTags(Map<String, String> tags) {
		this.auditTags = tags == null ? Map.of() : Map.copyOf(tags);
	}

	/** Gives the action a status phase (StatusSources). */
	void setStatusPhase(StatusPhase status) {
		this.status = status;
	}

	/** Parameter properties written, in order, before the action. */
	void setParameterWrites(java.util.List<java.util.Map<String, Object>> targets) {
		this.parameterJobs = new SinkJobs(context.action + "-parameters", targets);
	}

	public void prepareRequest(Exchange exchange) throws Exception {
		String invocation = java.util.UUID.randomUUID().toString();
		exchange.setProperty(AUDIT_INVOCATION, invocation);
		exchange.setProperty(AUDIT_STARTED, System.nanoTime());
		new OperationRequestProcessor(context.operationSourceId, context.callerInputTemplates).process(exchange);
		Object request = exchange.getMessage().getBody();
		exchange.setProperty(CALLER_INPUTS, request instanceof Map<?, ?> keyed
				? keyed.get(context.operationSourceId) : request);
		boolean basyx = Boolean.TRUE.equals(exchange.getProperty(OperationPayloads.BASYX_DELEGATION_BODY, Boolean.class));
		AuditLog.emit(() -> auditPoint("aas_invoke", invocation).tag("caller", basyx ? "basyx" : "rest")
				.value(request instanceof Map<?, ?> keyed && keyed.containsKey(context.operationSourceId)
						? keyed.get(context.operationSourceId) : request));
	}

	/** After the request transformation: keeps its result for the parameter writes and the command. */
	public void keepRequestResult(Exchange exchange) {
		exchange.setProperty(REQUEST_RESULT, exchange.getMessage().getBody());
	}

	/** The acknowledged parameter writes, in authored order, before the action. */
	public void prepareParameterWrites(Exchange exchange) {
		exchange.setProperty(PARAMETER_JOBS, parameterJobs.prepare(
				OperationPayloads.keyedValues(exchange.getProperty(REQUEST_RESULT)), true));
	}

	/** Split step of an acknowledged write: the job's payload, addressed to its write route. */
	public static void toWriteMessage(Exchange exchange) {
		ResponseSinkFanOutProcessor.DispatchJob job = (ResponseSinkFanOutProcessor.DispatchJob) exchange.getMessage().getBody();
		Object body = job.body();
		if (job.jsonPayload() && (body instanceof String || body == null))
			body = new org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload(body);
		else if (body == null) body = org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.JSON_NULL;
		if (job.jsonPayload()) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		else exchange.getMessage().removeHeader(Exchange.CONTENT_TYPE);
		exchange.getMessage().setHeader(WRITE_ENDPOINT_HEADER, job.endpoint());
		exchange.getMessage().setBody(body);
	}

	/**
	 * Before the trigger: the action command from the request result, typed by the
	 * declared input schema and validated. The connector's invocation steps send it.
	 */
	public void prepareCommand(Exchange exchange) {
		// parse every DECLARED parameter per its AID DataSchema type —
		// the wire representation is data-driven, never name-based
		Map<String, Object> command = assembleCommand(applySinkTransformers(exchange.getProperty(REQUEST_RESULT)));
		OperationPayloads.coerceFields(command, context.parameterTypes);
		Object tag = context.targetOptions.get("commandTag");
		try {
			validation.outgoing(tag == null ? command : command.get(String.valueOf(tag)));
		} catch (IllegalArgumentException invalid) {
			throw new OperationValidationException("DMP_COMMAND_VALIDATION_FAILED", 500,
					"Mapped command violates the declared input schema",
				java.util.List.of(invalid.getMessage()));
		}
		exchange.getMessage().setBody(command);
	}

	/** After the trigger: keeps the connector's reply, validated against the declared output schema. */
	public void acceptReply(Exchange exchange) {
		exchange.setProperty(COMMAND_REPLY, normalizeReply(exchange, exchange.getMessage().getBody()));
	}

	/** The connector's invocation steps for the action endpoint, or a plain InOut send. */
	void configureInvoke(org.apache.camel.model.ProcessorDefinition<?> route) {
		SynchronousInvocationAdapter connector = invocationAdapters.stream()
				.filter(candidate -> candidate.supports(context.commandSinkUri)).findFirst().orElse(null);
		if (connector == null) route.to(org.apache.camel.ExchangePattern.InOut, context.commandSinkUri);
		else connector.configureInvoke(route, context.commandSinkUri, invocationSchema);
	}

	/** Status phase: the body and invocation of one status check. */
	public void prepareStatusCheck(Exchange exchange) {
		status.prepareCheck(exchange, exchange.getProperty(CALLER_INPUTS), decodedReply(exchange.getProperty(COMMAND_REPLY)));
	}

	/** The validated reply is JSON text; the Lua invocation receives its decoded value. */
	private static Object decodedReply(Object reply) {
		if (!(reply instanceof String text) || text.isBlank()) return reply;
		try { return org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec.parseJson(text); }
		catch (RuntimeException notJson) { return text; }
	}

	/**
	 * Response phase: the command's reply, projected by the reply bindings,
	 * and the final values of reused status sources. Read-once sources are
	 * added by Content Enrichers after this step.
	 */
	public void collectResponseSources(Exchange exchange) {
		Object reply = exchange.getProperty(COMMAND_REPLY);
		StatusPhase.Run run = status == null ? null : StatusPhase.run(exchange);
		if (responseSourceBindings.isEmpty()) {
			exchange.getMessage().setBody(reply);
		} else {
			java.util.List<Map<String, Object>> replyBindings = responseSourceBindings.stream()
					.filter(binding -> binding.get("kind") == null || "reply".equals(binding.get("kind"))).toList();
			Map<String, Object> values = new java.util.LinkedHashMap<>();
			if (!replyBindings.isEmpty()) {
				Object projected = ResponseProjectionProcessor.project(reply, replyBindings);
				if (projected instanceof Map<?, ?> map) map.forEach((key, value) -> values.put(String.valueOf(key), value));
			}
			for (Map<String, Object> binding : responseSourceBindings)
				if ("status".equals(binding.get("kind")))
					values.put(String.valueOf(binding.get("sourceId")), run == null ? null
							: run.current.get(String.valueOf(binding.get("statusSourceId"))));
			exchange.getMessage().setBody(values);
		}
		Map<String, Object> invocation = new java.util.LinkedHashMap<>();
		if (run != null) {
			invocation.put("status", run.state);
			invocation.put("reason", run.reason);
		}
		invocation.put("inputs", exchange.getProperty(CALLER_INPUTS));
		invocation.put("reply", decodedReply(reply));
		exchange.setProperty(org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.TransformerInvocation.ARGUMENT_PROPERTY,
				invocation);
	}

	/** Enricher strategy for a read-once response source. */
	org.apache.camel.AggregationStrategy responseRead(String sourceId) {
		return (original, read) -> {
			@SuppressWarnings("unchecked")
			Map<String, Object> values = (Map<String, Object>) original.getMessage().getBody();
			values.put(sourceId, read.getMessage().getBody());
			return original;
		};
	}

	private Map<String, Object> assembleCommand(Object transformed) {
		// A tag-write action (PLC4X) writes the SinkId value itself to its one tag.
		Object tag = context.targetOptions.get("commandTag");
		if (tag == null) return OperationPayloads.assembleCommand(transformed,
				context.commandSinkIds, context.commandSinkValuePaths, context.commandSinkId);
		Object value = org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadAssembler
				.assemble(OperationPayloads.keyedValues(transformed), context.commandSinkIds, context.commandSinkValuePaths);
		Object type = context.targetOptions.get("commandType");
		Map<String, Object> command = new java.util.LinkedHashMap<>();
		command.put(String.valueOf(tag), type == null ? value : OperationPayloads.coerceTyped(value, String.valueOf(type)));
		return command;
	}

	private Object normalizeReply(Exchange exchange, Object reply) {
		// Connector adapters normalize replies to a named logical object;
		// the generic response path validates/projects/transforms it once.
		Exchange replyExchange = new org.apache.camel.support.DefaultExchange(exchange.getContext());
		replyExchange.getMessage().setBody(reply);
		try {
			replyValidationProcessor.process(replyExchange);
		} catch (RuntimeException failure) {
			throw failure;
		} catch (Exception failure) {
			throw new IllegalStateException(failure);
		}
		return replyExchange.getMessage().getBody();
	}

	/**
	 * Splits the response writes: writes to the device's own AID (e.g. a reset
	 * of the start coil) go to {@link #DEVICE_JOBS}, made before the reply;
	 * AAS writes stay in the body, written best effort by a Wire Tap.
	 */
	public void prepareResponseWrites(Exchange exchange) {
			Object result = exchange.getMessage().getBody();
			if (context.explicitResponseMapping && !failed(exchange)) {
				OperationPayloads.requireSinkValue(result, context.callerReplySinkId);
			}
			exchange.setProperty(CALLER_REPLY_RESULT, result);
			java.util.List<ResponseSinkFanOutProcessor.DispatchJob> jobs = responseSinkTargets.isEmpty()
					? java.util.List.of() : responseFanOut.prepareJobs(exchange);
			exchange.setProperty(DEVICE_JOBS, jobs.stream().filter(job -> deviceEndpoints.contains(job.endpoint())).toList());
			exchange.getMessage().setBody(jobs.stream().filter(job -> !deviceEndpoints.contains(job.endpoint())).toList());
	}

	private boolean failed(Exchange exchange) {
		return status != null && StatusPhase.FAILED.equals(StatusPhase.run(exchange).state);
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
			// A failed action answers with the caller outputs the response transformation chose
			// (e.g. an error code); without any, the call fails.
			if (failed(exchange) && (!(result instanceof Map<?, ?> values) || values.get(context.callerReplySinkId) == null))
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "DMP_ACTION_FAILED",
						"the status transformation reported a failed action", Map.of());
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
