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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.writedelegation;

import java.util.Collections;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.rest.RestDefinition;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreator;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationCommandProcessor;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationContext;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationDelegationTimeoutException;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationFailureProcessor;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.AsyncWritebackProcessor;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.SyncWriteAckProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compiles one {@link WriteDelegationRouteConfiguration} (trigger
 * {@code write-delegation}, W2 Shape A): a camel-rest PUT consumer on the
 * derived {@code /properties/...} path; the device command leg (MQTT 5
 * InOut / opcua node write / http request) and the
 * ack-then-writeback tail into the AAS property.
 */
public class WriteDelegationRouteCreator implements RouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(WriteDelegationRouteCreator.class);
	private static final String CONTENT_TYPE_JSON = "application/json";

	private final RouteBuilder routeBuilder;
	private final RoutesConfiguration routesConfiguration;

	public WriteDelegationRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		this.routeBuilder = routeBuilder;
		this.routesConfiguration = routesConfiguration;
	}

	@Override
	public void addRouteToRouteBuilder(RouteConfiguration routeConfig) {
		if (!(routeConfig instanceof WriteDelegationRouteConfiguration)) {
			LOGGER.warn("write-delegation creator received a non-write config: {}", routeConfig);
			return;
		}
		WriteDelegationRouteConfiguration cfg = (WriteDelegationRouteConfiguration) routeConfig;

		String restPath = cfg.getRestPath();
		if (restPath == null || restPath.isBlank()) {
			LOGGER.warn("write route {}: no derived property path (triggerData.restPath); skipping",
					cfg.getRouteId());
			return;
		}

		String requestLuaUri = firstTransformerUri(cfg.getTransformers());
		String commandSinkUri = firstSinkUri(cfg.getDatasinks());
		String writebackSinkUri = firstSinkUri(Collections.singletonList(cfg.getWritebackSink()));
		if (requestLuaUri == null || commandSinkUri == null || writebackSinkUri == null) {
			LOGGER.warn("write route {}: missing transformer/sink endpoints; skipping", cfg.getRouteId());
			return;
		}

		String routeId = cfg.getRouteId() != null ? cfg.getRouteId() : "write-" + restPath;
		String method = cfg.getRestMethod();
		long timeout = cfg.getTimeoutSeconds();
		String protocol = cfg.getProtocol();
		if ("mqtt".equalsIgnoreCase(protocol)
				&& (!cfg.isRequestReply() || cfg.getResponseTopic() == null
						|| cfg.getResponseTopic().isBlank())) {
			LOGGER.warn("write route {}: MQTT request/reply terms are required; skipping", routeId);
			return;
		}
		// the Lua envelope key (the request transformer's sourceKeys — the
		// writeback sink id) drives the bridge's SinkId unwrap; the raw
		// datasink id is NOT the envelope key
		String envelopeKey = requestEnvelopeKey(cfg.getTransformers());

		RestDefinition restVerb;
		if ("GET".equalsIgnoreCase(method)) {
			restVerb = routeBuilder.rest().get(restPath);
		} else {
			restVerb = routeBuilder.rest().put(restPath);
		}
		restVerb
				.consumes(CONTENT_TYPE_JSON)
				.produces(CONTENT_TYPE_JSON)
				.to("direct:" + routeId);

		String responseLuaUri = cfg.getResponseTransformer() == null ? null
				: firstTransformerUri(Collections.singletonList(cfg.getResponseTransformer()));
		OperationContext operationContext = new OperationContext(
				envelopeKey, requestLuaUri, responseLuaUri, commandSinkUri, timeout, method, protocol,
				cfg.getParameterTypes(), cfg.getResponseTypes());

		if ("mqtt".equalsIgnoreCase(protocol)) {
			String mqtt5Uri = mqtt5InOutUri(commandSinkUri, cfg.getResponseTopic(), timeout);
			org.apache.camel.model.TryDefinition tryBody = routeBuilder.from("direct:" + routeId)
					.routeId(routeId)
					.doTry()
							.to(requestLuaUri)
							.process(new OperationCommandProcessor(operationContext))
							.to(mqtt5Uri);
			if (responseLuaUri != null) {
				tryBody = tryBody.to(responseLuaUri);
			}
			tryBody.process(new AsyncWritebackProcessor(
						routeBuilder.getContext().createProducerTemplate(), writebackSinkUri, envelopeKey))
					.doCatch(OperationDelegationTimeoutException.class)
							.process(new OperationFailureProcessor(operationContext,
									org.apache.http.HttpStatus.SC_GATEWAY_TIMEOUT))
					.doCatch(RuntimeException.class)
							.process(new OperationFailureProcessor(operationContext,
									org.apache.http.HttpStatus.SC_INTERNAL_SERVER_ERROR))
					.end();
		} else {
			// synchronous write (opcua/http): write status / response IS the ack
			routeBuilder.from("direct:" + routeId)
					.routeId(routeId)
					.process(new SyncWriteAckProcessor(
							routeBuilder.getContext().createProducerTemplate(),
							operationContext, writebackSinkUri, envelopeKey));
		}
		LOGGER.info("write-delegation route {}: {} {} -> {} (writeback {}, {})",
				routeId, method, restPath, commandSinkUri, writebackSinkUri, protocol);
	}

	private String firstTransformerUri(java.util.List<String> transformerIds) {
		if (transformerIds == null || transformerIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataTransformerEndpoints(routesConfiguration, transformerIds,
				"write")[0];
	}

	/** The Lua envelope key: the request transformer's {@code sourceKeys}. */
	private String requestEnvelopeKey(java.util.List<String> transformerIds) {
		if (transformerIds == null || transformerIds.isEmpty()) {
			return "";
		}
		DataTransformerConfiguration config =
				routesConfiguration.getTransformers().get(transformerIds.get(0));
		return config == null || config.getSourceKeys() == null ? ""
				: config.getSourceKeys().trim();
	}

	private String firstSinkUri(java.util.List<String> sinkIds) {
		if (sinkIds == null || sinkIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataSinkEndpoints(routesConfiguration, sinkIds, "write")[0];
	}

	private String mqtt5InOutUri(String pahoSinkUri, String replyTopic, long timeoutSeconds) {
		if (pahoSinkUri == null || !pahoSinkUri.startsWith("paho:")) {
			throw new IllegalArgumentException("MQTT write requires a paho command sink, got: " + pahoSinkUri);
		}
		return "paho-mqtt5-inout:" + pahoSinkUri.substring("paho:".length())
				+ "&replyTopic=" + replyTopic
				+ "&replyTimeout=" + timeoutSeconds
				+ "&skipReplyStates=RUNNING";
	}
}
