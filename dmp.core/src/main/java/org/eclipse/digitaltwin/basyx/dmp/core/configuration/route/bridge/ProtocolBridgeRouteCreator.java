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
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.bridge;

import java.util.Collections;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreator;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationCommandProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationContext;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationDelegationTimeoutException;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationFailureProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationReplyProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.OperationRequestProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.operation.SyncOperationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generalized interactive-bridge creator (ADR-020 ProtocolBridge, DMP v3).
 *
 * <p>
 * Applies to every operation-delegation mapping: the descriptor derives the
 * caller-facing REST surface by convention and carries {@code restPath}/
 * {@code restMethod} + {@code protocol} in the route's {@code triggerData} —
 * this creator emits the REST consumer from it directly (no REST datasource
 * file exists) and dispatches the command leg on the native command
 * affordance's protocol. The SYNC legs (DECLARED request/reply correlation on
 * mqtt, and http) share ONE declarative creator code path:
 * <pre>
 * rest → flatten → request Lua → command prep → to(command endpoint) [InOut]
 *      → response Lua → reply envelope (OperationVariable[] for BaSyx callers)
 * </pre>
 * <ul>
 * <li><b>mqtt</b> with a DECLARED {@code mqv_request_reply} correlation: the
 * command endpoint is the {@code paho-mqtt5-inout:} component — the Camel
 * RequestReply EIP. The component publishes the command with the MQTT 5
 * ResponseTopic + CorrelationData properties, subscribes the DECLARED reply
 * topic and completes the exchange on the first terminal reply (filtering the
 * PackML RUNNING progress ack); a timeout completes the exchange as a failure
 * (HTTP 504), any other error as 500. The component itself registers the
 * upstream reply-consumer route;</li>
 * <li><b>http</b> (incl. IO-Link over Profinet REST): the camel-http producer
 * POSTs synchronously (the declared method rides the {@code CamelHttpMethod}
 * header) — the HTTP response IS the reply;</li>
 * <li><b>opcua</b> / <b>plc4x</b>: synchronous write-style invocation through
 * the {@link SyncOperationProcessor} (the awaited UA Call decodes the
 * {@code CallMethodResult} positionally; the MethodReply node read-back is a
 * fallback);</li>
 * </ul>
 *
 * <p>
 * The interactive consumer joins the DMP's consolidated REST surface
 * (camel-rest with jetty, same port as {@code /health}).
 */
public class ProtocolBridgeRouteCreator implements RouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolBridgeRouteCreator.class);
	private static final String CONTENT_TYPE_JSON = "application/json";

	private final RouteBuilder routeBuilder;
	private final RoutesConfiguration routesConfiguration;

	public ProtocolBridgeRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		this.routeBuilder = routeBuilder;
		this.routesConfiguration = routesConfiguration;
	}

	@Override
	public void addRouteToRouteBuilder(RouteConfiguration routeConfig) {
		if (!(routeConfig instanceof ProtocolBridgeRouteConfiguration)) {
			LOGGER.warn("bridge creator received a non-bridge config: {}", routeConfig);
			return;
		}
		ProtocolBridgeRouteConfiguration bridgeConfig = (ProtocolBridgeRouteConfiguration) routeConfig;

		String restPath = bridgeConfig.getRestPath();
		if (restPath == null || restPath.isBlank()) {
			LOGGER.warn("bridge {}: no derived operation path (triggerData.restPath); skipping",
					bridgeConfig.getRouteId());
			return;
		}

		String requestLuaUri = firstTransformerUri(bridgeConfig.getTransformers());
		String responseLuaUri = firstTransformerUri(Collections.singletonList(bridgeConfig.getResponseTransformer()));
		String commandSinkUri = firstSinkUri(bridgeConfig.getDatasinks());
		if (requestLuaUri == null || commandSinkUri == null) {
			LOGGER.warn("bridge {}: missing transformer/sink endpoints; skipping",
					bridgeConfig.getRouteId());
			return;
		}

		// The action (skill) name is the last segment of the operation path; it
		// is also the Lua source key the AIMC blobs address (sources.<action>).
		// Disambiguated dual-binding paths carry a trailing /{protocol} — the
		// action is the segment BEFORE it.
		String action = restPath.substring(restPath.lastIndexOf('/') + 1);
		if (action.equalsIgnoreCase(bridgeConfig.getProtocol())) {
			String base = restPath.substring(0, restPath.lastIndexOf('/'));
			action = base.substring(base.lastIndexOf('/') + 1);
		}
		String routeId = bridgeConfig.getRouteId() != null
				? bridgeConfig.getRouteId() : "bridge-" + action;
		String method = bridgeConfig.getRestMethod();
		String protocol = bridgeConfig.getProtocol();
		if (ProtocolBridgeRouteConfiguration.DEFAULT_PROTOCOL.equalsIgnoreCase(protocol)
				&& (!bridgeConfig.isRequestReply() || bridgeConfig.getResponseTopic() == null
						|| bridgeConfig.getResponseTopic().isBlank())) {
			LOGGER.warn("bridge {}: MQTT operation lacks declared request/reply terms; skipping", routeId);
			return;
		}

		// 1. Interactive consumer (consolidated camel-rest surface), emitted
		// from the DERIVED operation path. Camel merges the rest consumer and
		// the direct route it targets into a single route; the direct route's
		// id wins, so it carries the canonical bridge id.
		org.apache.camel.model.rest.RestDefinition restVerb;
		if ("GET".equalsIgnoreCase(method)) {
			restVerb = routeBuilder.rest().get(restPath);
		} else {
			restVerb = routeBuilder.rest().post(restPath);
		}
		restVerb
				.consumes(CONTENT_TYPE_JSON)
				.produces(CONTENT_TYPE_JSON)
				.to("direct:" + routeId);

		if (ProtocolBridgeRouteConfiguration.DEFAULT_PROTOCOL.equalsIgnoreCase(protocol)) {
				// DECLARED MQTT 5 request/reply correlation (mqv_request_reply
				// + mqv_response_topic): the camel RequestReply EIP through
				// the paho-mqtt5-inout component (correlation semantics on
				// top of the upstream camel-paho-mqtt5 transport, whose
				// reply-consumer route the InOut producer registers itself)
				String mqtt5Uri = mqtt5InOutUri(commandSinkUri, bridgeConfig.getResponseTopic(),
						bridgeConfig.getTimeoutSeconds());
				OperationContext operationContext = new OperationContext(
						action, requestLuaUri, responseLuaUri, mqtt5Uri,
						bridgeConfig.getTimeoutSeconds(), method,
						ProtocolBridgeRouteConfiguration.DEFAULT_PROTOCOL,
						bridgeConfig.getParameterTypes(), bridgeConfig.getResponseTypes());
				addDelegationRoute(routeId, method, restPath, requestLuaUri, responseLuaUri,
						operationContext, mqtt5Uri);
				LOGGER.info("bridge route {}: {} {} -> paho-mqtt5-inout ({}, replyTopic={}, "
								+ "replyTimeout={}s, parameterTypes {}, responseTypes {})",
						routeId, method, restPath, mqtt5Uri, bridgeConfig.getResponseTopic(),
						bridgeConfig.getTimeoutSeconds(), operationContext.parameterTypes,
						operationContext.responseTypes);
			return;
		}

		if ("http".equalsIgnoreCase(protocol)) {
			// the camel-http producer IS the InOut leg (the HTTP response IS
			// the reply) — the same declarative delegation route shape
			OperationContext operationContext = new OperationContext(
					action, requestLuaUri, responseLuaUri, commandSinkUri,
					bridgeConfig.getTimeoutSeconds(), method, protocol,
					bridgeConfig.getParameterTypes(), bridgeConfig.getResponseTypes());
			addDelegationRoute(routeId, method, restPath, requestLuaUri, responseLuaUri,
					operationContext, commandSinkUri);
			LOGGER.info("bridge route {}: {} {} -> {} (http, synchronous, "
							+ "parameterTypes {}, responseTypes {})",
					routeId, method, restPath, commandSinkUri,
					operationContext.parameterTypes, operationContext.responseTypes);
			return;
		}

		// opcua / plc4x: synchronous write-style invocation with the awaited
		// CallMethodResult / write-status decode (and the MethodReply fallback)
		OperationContext operationContext = new OperationContext(
				action, requestLuaUri, responseLuaUri, commandSinkUri,
				bridgeConfig.getTimeoutSeconds(), method, protocol,
				bridgeConfig.getParameterTypes(), bridgeConfig.getResponseTypes(),
				bridgeConfig.getResponseOrder());
		routeBuilder.from("direct:" + routeId)
				.routeId(routeId)
				.process(new SyncOperationProcessor(routeBuilder.getContext(), operationContext));
		LOGGER.info("bridge route {}: {} {} -> {} ({}, synchronous, responseOrder {})",
				routeId, method, restPath, commandSinkUri, protocol,
				operationContext.responseOrder);
	}

	/**
	 * The shared declarative delegation route: REST consumer → flatten →
	 * request Lua → command prep → InOut command endpoint → response Lua →
	 * reply envelope. A correlation timeout completes the exchange as a
	 * failure (HTTP 504), any other error as 500 (matching the Python
	 * operation-delegation reference).
	 */
	private void addDelegationRoute(String routeId, String method, String restPath,
			String requestLuaUri, String responseLuaUri, OperationContext operationContext,
			String commandUri) {
		if (responseLuaUri == null) {
			LOGGER.warn("bridge {}: request/reply interaction without a response "
					+ "transformer; skipping", routeId);
			return;
		}
		routeBuilder.from("direct:" + routeId)
				.routeId(routeId)
				.doTry()
						.process(new OperationRequestProcessor())
						.to(requestLuaUri)
						.process(new OperationCommandProcessor(operationContext))
						.to(commandUri)
						.to(responseLuaUri)
						.process(new OperationReplyProcessor(operationContext))
				.doCatch(OperationDelegationTimeoutException.class)
						.process(new OperationFailureProcessor(operationContext,
								org.apache.http.HttpStatus.SC_GATEWAY_TIMEOUT))
				.doCatch(RuntimeException.class)
						.process(new OperationFailureProcessor(operationContext,
								org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR))
				.end();
	}

	/**
	 * Derives the {@code paho-mqtt5-inout:} InOut endpoint URI from the paho
	 * command sink URI ({@code paho:<topic>?brokerUrl=...&clientId=...&qos=...&retained=...})
	 * and the DECLARED reply facts. The InOut component owns only the
	 * correlation semantics; the wire transport (publish and the reply
	 * subscription) rides the upstream {@code paho-mqtt5:} component — see
	 * {@link #addReplyConsumerRoute}.
	 */
	private String mqtt5InOutUri(String pahoSinkUri, String replyTopic, long timeoutSeconds) {
		if (pahoSinkUri == null || !pahoSinkUri.startsWith("paho:")) {
			throw new IllegalArgumentException(
					"declared mqv_request_reply requires a paho command sink, got: " + pahoSinkUri);
		}
		StringBuilder uri = new StringBuilder("paho-mqtt5-inout:")
				.append(pahoSinkUri.substring("paho:".length()));
		uri.append("&replyTopic=").append(replyTopic);
		uri.append("&replyTimeout=").append(timeoutSeconds);
		// the PackML intermediate progress ack never completes an exchange
		uri.append("&skipReplyStates=RUNNING");
		LOGGER.info("derived InOut endpoint {}", uri);
		return uri.toString();
	}

	private String firstTransformerUri(java.util.List<String> transformerIds) {
		if (transformerIds == null || transformerIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataTransformerEndpoints(routesConfiguration, transformerIds,
				"bridge")[0];
	}

	private String firstSinkUri(java.util.List<String> sinkIds) {
		if (sinkIds == null || sinkIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataSinkEndpoints(routesConfiguration, sinkIds, "bridge")[0];
	}
}
