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
	/** The request transformation's result keyed by SinkId. */
	private static final String REQUEST_RESULT = "dmp.operation.requestResult";
	private static final String COMMAND_REPLY = "dmp.operation.commandReply";
	/** Per-sink transformer chains' results, keyed by SinkId. */
	private static final String SINK_RESULTS = "dmp.operation.sinkResults";
	/** A property write: the value written, which is also the call's reply. */
	private static final String WRITTEN_VALUE = "dmp.operation.writtenValue";

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncOperationProcessor.class);

	private final OperationContext context;
	private final CamelContext camelContext;
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
	}

	@Override
	public void bind(CamelContext owner) {
		if (camelContext != owner) throw new IllegalArgumentException("operation processor belongs to another Camel context");
	}

	@Override
	public java.util.Set<String> endpointUris() {
		// A property write uses the sink endpoint itself, not a connector's invocation endpoints.
		if (writesProperty()) return java.util.Set.of(context.commandSinkUri);
		return invocationAdapters.stream().filter(adapter -> adapter.supports(context.commandSinkUri)).findFirst()
				.map(adapter -> adapter.ownedEndpointUris(context.commandSinkUri, invocationSchema))
				.orElseGet(() -> java.util.Set.of(context.commandSinkUri));
	}

	@Override
	public void close() throws Exception {
		responseFanOut.close();
	}

	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		responseFanOut.drain(timeout);
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

	/** After the request transformation: keeps its result for the command. */
	public void keepRequestResult(Exchange exchange) {
		exchange.setProperty(REQUEST_RESULT, exchange.getMessage().getBody());
	}

	/**
	 * Before the trigger: the action command from the request result, typed by the
	 * declared input schema and validated. The connector's invocation steps send it.
	 */
	public void prepareCommand(Exchange exchange) {
		if (writesProperty()) {
			prepareWrite(exchange);
			return;
		}
		// parse every DECLARED parameter per its AID DataSchema type —
		// the wire representation is data-driven, never name-based
		Map<String, Object> command = OperationPayloads.assembleCommand(applySinkTransformers(exchange),
				context.commandSinkIds, context.commandSinkValuePaths, context.commandSinkId);
		OperationPayloads.coerceFields(command, context.parameterTypes);
		try {
			validation.outgoing(command);
		} catch (IllegalArgumentException invalid) {
			throw new OperationValidationException("DMP_COMMAND_VALIDATION_FAILED", 500,
					"Mapped command violates the declared input schema",
				java.util.List.of(invalid.getMessage()));
		}
		exchange.getMessage().setBody(command);
	}

	/** An Operation bound to a property affordance writes it (instead of invoking an action). */
	private boolean writesProperty() {
		return Boolean.TRUE.equals(context.targetOptions.get("write"));
	}

	/**
	 * A property write: the request transformation's value for the property's SinkId,
	 * validated against the property's schema and encoded for its connector (JSON text
	 * for HTTP and MQTT, the value itself otherwise).
	 */
	private void prepareWrite(Exchange exchange) {
		Object value = org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadAssembler.assemble(
				OperationPayloads.keyedValues(applySinkTransformers(exchange)),
				context.commandSinkIds, context.commandSinkValuePaths);
		Object written = value == org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.JSON_NULL ? null : value;
		try {
			validation.outgoing(written);
		} catch (IllegalArgumentException invalid) {
			throw new OperationValidationException("DMP_COMMAND_VALIDATION_FAILED", 500,
					"Mapped command violates the declared input schema",
				java.util.List.of(invalid.getMessage()));
		}
		exchange.setProperty(WRITTEN_VALUE, written);
		boolean json = Boolean.TRUE.equals(context.targetOptions.get("jsonPayload"));
		Object body = written == null ? org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.JSON_NULL : written;
		if (json && written instanceof String) body = new org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload(written);
		if (json) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
		exchange.getMessage().setBody(body);
	}

	/** After the trigger: keeps the connector's reply, validated against the declared output schema. */
	public void acceptReply(Exchange exchange) {
		exchange.setProperty(COMMAND_REPLY, normalizeReply(exchange, exchange.getMessage().getBody()));
	}

	/** The connector's invocation steps for the action endpoint, or a plain InOut send. */
	void configureInvoke(org.apache.camel.model.ProcessorDefinition<?> route) {
		if (writesProperty()) {
			// The connector's producer acknowledges the write (PLC4X, OPC UA, HTTP) or the broker
			// does (MQTT QoS 1/2); any failure is a device or transport error. The reply is the value.
			route.doTry()
					.to(context.commandSinkUri)
					.process(exchange -> exchange.getMessage().setBody(exchange.getProperty(WRITTEN_VALUE)))
				.doCatch(Exception.class)
					.process(SyncOperationProcessor::writeFailed)
				.end();
			return;
		}
		SynchronousInvocationAdapter connector = invocationAdapters.stream()
				.filter(candidate -> candidate.supports(context.commandSinkUri)).findFirst().orElse(null);
		if (connector == null) route.to(org.apache.camel.ExchangePattern.InOut, context.commandSinkUri);
		else connector.configureInvoke(route, context.commandSinkUri, invocationSchema);
	}

	private static void writeFailed(Exchange exchange) {
		Exception caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
		for (Throwable cause = caught; cause != null; cause = cause.getCause() == cause ? null : cause.getCause())
			if (cause instanceof org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException typed) throw typed;
		// A device that answered with an error (HTTP non-2xx) is a remote failure; anything else is transport.
		boolean answered = caught != null && "org.apache.camel.http.base.HttpOperationFailedException".equals(caught.getClass().getName());
		throw new org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException(
				answered ? org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException.Category.REMOTE
						: org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException.Category.TRANSPORT,
				"DMP_WRITE_FAILED", "the property write was not acknowledged", Map.of());
	}

	/**
	 * The response transformation's input: the reply (an action's output, or the value a
	 * property write wrote), projected by the ResponseSource bindings. Without a response
	 * mapping the call answers with no outputs.
	 */
	public void collectResponseSources(Exchange exchange) {
		if (!context.explicitResponseMapping) {
			exchange.getMessage().setBody(new java.util.LinkedHashMap<String, Object>());
			return;
		}
		exchange.getMessage().setBody(ResponseProjectionProcessor.project(exchange.getProperty(COMMAND_REPLY),
				responseSourceBindings));
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

	public void prepareResponseWrites(Exchange exchange) {
			Object result = exchange.getMessage().getBody();
			if (context.explicitResponseMapping && context.callerReplySinkId != null) {
				OperationPayloads.requireSinkValue(result, context.callerReplySinkId);
			}
			exchange.setProperty(CALLER_REPLY_RESULT, result);
			exchange.getMessage().setBody(responseSinkTargets.isEmpty() ? java.util.List.of()
					: responseFanOut.prepareJobs(exchange));
	}

	/** The detached AAS response writes: a bounded Wire Tap per job into {@code writeUri}. */
	void configureResponseDispatch(org.apache.camel.model.ProcessorDefinition<?> route, String writeUri) {
		responseFanOut.configureDispatch(route, writeUri);
	}

	/** The response write route's steps. */
	void configureResponseWrite(org.apache.camel.model.ProcessorDefinition<?> route) {
		responseFanOut.configureWrite(route);
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

	/** The authored per-sink transformer chains (a direct route for a multi-step chain), by SinkId. */
	java.util.Map<String, java.util.List<String>> sinkTransformerChains() {
		return context.sinkTransformerUris;
	}

	/** Enricher strategy: keeps a per-sink chain's result for {@code sinkId}; the exchange body is unchanged. */
	org.apache.camel.AggregationStrategy keepSinkResult(String sinkId) {
		return (original, transformed) -> {
			@SuppressWarnings("unchecked")
			Map<String, Object> results = original.getProperty(SINK_RESULTS, Map.class);
			if (results == null) {
				results = new java.util.LinkedHashMap<>();
				original.setProperty(SINK_RESULTS, results);
			}
			results.put(sinkId, OperationPayloads.requireCommandSinkValue(transformed.getMessage().getBody(), sinkId));
			return original;
		};
	}

	/** The request result per SinkId, a sink with its own transformer chain taking that chain's result. */
	private Object applySinkTransformers(Exchange exchange) {
		Object transformed = exchange.getProperty(REQUEST_RESULT);
		if (context.sinkTransformerUris.isEmpty()) return transformed;
		Map<String, Object> values = OperationPayloads.keyedValues(transformed);
		@SuppressWarnings("unchecked")
		Map<String, Object> chained = exchange.getProperty(SINK_RESULTS, Map.class);
		Map<String, Object> result = new java.util.LinkedHashMap<>();
		for (String sinkId : context.commandSinkIds) {
			result.put(sinkId, chained != null && chained.containsKey(sinkId) ? chained.get(sinkId)
					: OperationPayloads.requireCommandSinkValue(values, sinkId));
		}
		return result;
	}

}
