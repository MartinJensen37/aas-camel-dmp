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

import java.util.Collections;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreator;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.dmp.core.routebuilder.DmpRouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generalized interactive-bridge creator (ADR-020 ProtocolBridge, DMP v3).
 *
 * <p>
 * Applies to every operation-delegation mapping: the descriptor derives the
 * caller-facing REST surface by convention and carries {@code restPath}/
 * {@code restMethod} in generic route trigger data. This AAS connector emits
 * the REST consumer and completes the accepted Operation invocation. Target
 * request/reply behavior is delegated to registered connector adapters:
 * <pre>
 * rest → flatten → request Lua → named command → Invoke adapter [InOut]
 *      → response Lua → reply envelope (OperationVariable[] for BaSyx callers)
 * </pre>
 * <p>Target connectors own endpoint addressing, request encoding,
 * correlation, reply normalization and native error/status conversion.
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
		ProtocolBridgeRouteConfiguration bridgeConfig = routeConfig instanceof ProtocolBridgeRouteConfiguration typed
				? typed : new ProtocolBridgeRouteConfiguration(routeConfig);

		String restPath = bridgeConfig.getRestPath();
		if (restPath == null || restPath.isBlank()) {
			LOGGER.warn("bridge {}: no derived operation path (triggerData.restPath); skipping",
					bridgeConfig.getRouteId());
			return;
		}

		String[] requestTransformers = RouteCreatorHelper.getDataTransformerEndpoints(
				routesConfiguration, bridgeConfig.getTransformers(), "bridge");
		String requestLuaUri = requestTransformers.length == 0 ? null : requestTransformers[0];
		String[] responseTransformers = bridgeConfig.getResponseTransformer() == null ? new String[0]
				: RouteCreatorHelper.getDataTransformerEndpoints(routesConfiguration,
						Collections.singletonList(bridgeConfig.getResponseTransformer()), "bridge");
		String responseLuaUri = responseTransformers.length == 0 ? null : responseTransformers[0];
		java.util.List<String> commandSinkIds = bridgeConfig.getCommandSinkIds().isEmpty()
				? bridgeConfig.getDatasinks() : bridgeConfig.getCommandSinkIds();
		String commandSinkUri = firstSinkUri(commandSinkIds);
		java.util.Map<String, String[]> sinkTransformerEndpoints = RouteCreatorHelper.getDataSinkMappingById(
				routesConfiguration, bridgeConfig.getDatasinkMappingConfiguration(), bridgeConfig.getRouteId());
		AffordanceValidation commandValidation = commandValidation(commandSinkIds);
		java.util.List<java.util.Map<String, Object>> responseSinkTargets =
				resolveResponseSinkTargets(bridgeConfig);
		java.util.List<java.util.Map<String, Object>> responseSourceBindings =
				bridgeConfig.getResponseSourceBindings();
		java.util.List<String> commandSourceIds = bridgeConfig.getCommandSinkBindings().isEmpty()
				? java.util.List.of(bridgeConfig.getCommandSinkId() == null
						? String.valueOf(bridgeConfig.getAction()) : bridgeConfig.getCommandSinkId())
				: bridgeConfig.getCommandSinkBindings().stream()
						.map(binding -> String.valueOf(binding.get("sinkId"))).toList();
		java.util.Map<String, String> commandValuePaths = new java.util.LinkedHashMap<>();
		for (java.util.Map<String, Object> binding : bridgeConfig.getCommandSinkBindings()) {
			if (binding.get("valuePath") != null) {
				commandValuePaths.put(String.valueOf(binding.get("sinkId")),
						String.valueOf(binding.get("valuePath")));
			}
		}
		java.util.Map<String, java.util.List<String>> authoredSinkTransformers = new java.util.LinkedHashMap<>();
		if (sinkTransformerEndpoints != null) {
			if (bridgeConfig.getCommandSinkBindings().isEmpty()) {
				for (var entry : sinkTransformerEndpoints.entrySet()) {
					authoredSinkTransformers.put(entry.getKey(), java.util.Arrays.asList(entry.getValue()));
				}
			} else {
				for (java.util.Map<String, Object> binding : bridgeConfig.getCommandSinkBindings()) {
					String sinkId = String.valueOf(binding.get("sinkId"));
					String dataSinkId = String.valueOf(binding.get("dataSinkId"));
					String[] chain = sinkTransformerEndpoints.get(dataSinkId);
					if (chain != null) authoredSinkTransformers.put(sinkId, java.util.Arrays.asList(chain));
				}
			}
		}
		if (requestLuaUri == null || commandSinkUri == null) {
			LOGGER.warn("bridge {}: missing transformer/sink endpoints; skipping",
					bridgeConfig.getRouteId());
			return;
		}

		String action = bridgeConfig.getAction();
		if (action == null || action.isBlank()) {
			LOGGER.warn("bridge {}: descriptor has no resolved action name; skipping",
					bridgeConfig.getRouteId());
			return;
		}
		String routeId = bridgeConfig.getRouteId() != null
				? bridgeConfig.getRouteId() : "bridge-" + action;
		String method = bridgeConfig.getRestMethod();
		authoredSinkTransformers = nativeSinkTransformerChains(routeId, authoredSinkTransformers);

		// 1. Interactive consumer (consolidated camel-rest surface), emitted
		// from the DERIVED operation path. Camel merges the rest consumer and
		// the direct route it targets into a single route; the direct route's
		// id wins, so it carries the canonical bridge id.
		org.apache.camel.model.rest.RestDefinition rest = routeBuilder.rest();
		org.apache.camel.model.rest.RestDefinition restVerb;
		if ("GET".equalsIgnoreCase(method)) {
			restVerb = rest.get(restPath);
		} else {
			restVerb = rest.post(restPath);
		}
		String restRouteId = routeBuilder instanceof DmpRouteBuilder ? routeId + "-rest-consumer" : null;
		if (restRouteId != null) restVerb.getVerbs().get(restVerb.getVerbs().size() - 1).setRouteId(restRouteId);
		restVerb
				.consumes(CONTENT_TYPE_JSON)
				.produces(CONTENT_TYPE_JSON)
				.to("direct:" + routeId);

		OperationContext operationContext = new OperationContext(
				action, requestLuaUri, responseLuaUri, commandSinkUri,
				bridgeConfig.getParameterTypes(),
				bridgeConfig.getResponseTypes(), bridgeConfig.getCallerOutputOrder(),
				bridgeConfig.getOperationSourceId(), bridgeConfig.getCommandSinkId(),
				bridgeConfig.getCallerReplySinkId(), commandSourceIds, commandValuePaths,
				bridgeConfig.isExplicitResponseMapping(), bridgeConfig.getParameterRequired(),
				bridgeConfig.getResponseRequired(), bridgeConfig.getTargetOptions(),
				java.util.Arrays.asList(requestTransformers),
				authoredSinkTransformers, bridgeConfig.getCallerResponseTypes());
		SyncOperationProcessor operation = new SyncOperationProcessor(routeBuilder.getContext(), operationContext,
				commandValidation, new AffordanceValidation(bridgeConfig.getResponseSchema(),
						"bridge output " + action), responseSourceBindings, responseSinkTargets, routeId);
		registerManagedResource(operation);
		String responseWriteRouteId = routeId + "-response-write";
		String responseWriteUri = "direct:" + responseWriteRouteId;
		if (!responseSinkTargets.isEmpty()) {
			registerRouteId(responseWriteRouteId);
			routeBuilder.from(responseWriteUri)
					.routeId(responseWriteRouteId)
					.process(operation::dispatchResponseWriteJob);
		}
		registerRouteId(routeId);
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerIngressRouteId(routeId);
		var operationRoute = routeBuilder.from("direct:" + routeId).routeId(routeId);
		operationRoute.doTry()
				.process(operation::prepareRequest)
				.doCatch(Exception.class)
					.process(operation::handleFailure)
				.end();
		operationRoute.doTry()
					.to(requestTransformers)
				.doCatch(Exception.class)
					.process(operation::handleFailure)
				.end();
		operationRoute.doTry()
				.process(operation::invokeAndPrepareReply)
				.doCatch(Exception.class)
					.process(operation::handleFailure)
				.end();
		if (responseLuaUri != null) {
			operationRoute.doTry()
					.to(responseLuaUri)
				.doCatch(Exception.class)
					.process(operation::handleFailure)
				.end();
		}
		operationRoute.doTry()
				.process(operation::prepareResponseWrites)
				.doCatch(Exception.class)
					.process(operation::handleFailure)
				.end();
		if (!responseSinkTargets.isEmpty()) {
			var responseJobs = operationRoute.split(org.apache.camel.builder.Builder.body());
			var enqueue = responseJobs.doTry();
			enqueue.wireTap(responseWriteUri)
					.onPrepare(org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor::prepareDetachedExchange)
					.executorService(operation.responseWriteExecutor());
			enqueue.doCatch(Exception.class)
				.process(operation::recordResponseWriteRejection)
				.end();
			responseJobs.end();
		}
		operationRoute.doTry()
				.process(operation::completeReply)
				.doCatch(Exception.class)
					.process(operation::handleFailure)
				.end();
		operationRoute.process(ProtocolBridgeRouteCreator::encodeRestResponse);
		registerRestReservation(method, restPath, rest, restRouteId);
		LOGGER.info("bridge route {}: {} {} (callerOutputOrder {})",
				routeId, method, restPath,
				operationContext.callerOutputOrder);
	}

	private static void encodeRestResponse(org.apache.camel.Exchange exchange) {
		if (Boolean.TRUE.equals(exchange.getProperty(OperationPayloads.OPERATION_DELEGATION_FAILED))) return;
		exchange.getMessage().setBody(OperationPayloads.encodeReply(exchange.getMessage().getBody()));
	}

	private void registerRouteId(String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerRouteId(routeId);
	}

	private void registerRestReservation(String method, String path,
			org.apache.camel.model.rest.RestDefinition definition, String restRouteId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) {
			managed.registerRestReservation(method, path, definition, restRouteId);
		}
	}

	private void registerManagedResource(SyncOperationProcessor operation) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(operation);
	}

	private java.util.Map<String, java.util.List<String>> nativeSinkTransformerChains(
			String routeId, java.util.Map<String, java.util.List<String>> chains) {
		java.util.Map<String, java.util.List<String>> routes = new java.util.LinkedHashMap<>();
		int index = 0;
		for (var entry : chains.entrySet()) {
			java.util.List<String> chain = entry.getValue();
			if (chain.size() <= 1) {
				routes.put(entry.getKey(), chain);
				continue;
			}
			String chainRouteId = routeId + "-sink-chain-" + index++;
			registerRouteId(chainRouteId);
			routeBuilder.from("direct:" + chainRouteId)
					.routeId(chainRouteId)
					.to(chain.toArray(String[]::new));
			routes.put(entry.getKey(), java.util.List.of("direct:" + chainRouteId));
		}
		return routes;
	}

	private java.util.List<java.util.Map<String, Object>> resolveResponseSinkTargets(
			ProtocolBridgeRouteConfiguration config) {
		java.util.List<java.util.Map<String, Object>> targets = new java.util.ArrayList<>();
		int aasRouteIndex = 0;
		for (java.util.Map<String, Object> binding : config.getResponseSinkBindings()) {
			String dataSinkId = String.valueOf(binding.get("dataSinkId"));
			String endpoint = RouteCreatorHelper.getDataSinkEndpoint(routesConfiguration,
					dataSinkId, config.getRouteId());
			java.util.Map<String, Object> target = new java.util.LinkedHashMap<>(binding);
			if (Boolean.TRUE.equals(binding.get("aas"))) {
				String sinkId = String.valueOf(binding.get("sinkId"));
				String adapterRouteId = (config.getRouteId() + "-aas-response-" + aasRouteIndex++)
						.replaceAll("[^A-Za-z0-9_.-]", "-");
				registerRouteId(adapterRouteId);
				String adapterUri = "direct:" + adapterRouteId;
				routeBuilder.from(adapterUri)
						.routeId(adapterRouteId)
						.process(exchange -> {
							java.util.Map<String, Object> envelope = new java.util.LinkedHashMap<>();
							envelope.put(sinkId, exchange.getMessage().getBody());
							exchange.getMessage().setBody(envelope);
						})
						.to(endpoint);
				target.put("endpoint", adapterUri);
				// An AAS endpoint writes one element per destination; its own valuePath
				// extracts SinkId from the connector envelope above.
				target.put("payloadGroup", null);
				target.put("valuePath", null);
				target.put("jsonPayload", false);
			} else {
				target.put("endpoint", endpoint);
			}
			target.remove("aas");
			targets.add(java.util.Collections.unmodifiableMap(target));
		}
		return java.util.List.copyOf(targets);
	}

	private AffordanceValidation commandValidation(java.util.List<String> sinkIds) {
		DataSinkConfiguration sink = sinkIds == null || sinkIds.isEmpty() ? null
				: routesConfiguration.getDatasinks().get(sinkIds.get(0));
		return new AffordanceValidation(sink == null ? null
				: sink.getPayloadSchema() != null ? sink.getPayloadSchema() : sink.getSchema(),
				"bridge sink " + (sinkIds == null || sinkIds.isEmpty() ? "unknown" : sinkIds.get(0)));
	}

	private String firstSinkUri(java.util.List<String> sinkIds) {
		if (sinkIds == null || sinkIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataSinkEndpoints(routesConfiguration, sinkIds, "bridge")[0];
	}
}
