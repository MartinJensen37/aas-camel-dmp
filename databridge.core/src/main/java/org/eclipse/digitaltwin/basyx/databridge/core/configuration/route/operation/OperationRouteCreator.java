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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.operation;

import java.util.Collections;
import java.util.List;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.RestDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.IRouteCreator;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.MqttRequestReplyBridge;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationContext;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationInvocationProcessor;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationResponseProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds the REST operation-delegation routes (M7).
 *
 * <p>
 * For every operation route config the creator wires:
 * <ul>
 * <li>a camel-rest consumer for the operation's REST endpoint (the REST
 * datasource of the route), delegating each invocation to a
 * {@link MqttRequestReplyBridge} — the request is transformed by the
 * request Lua blob, published to the native MQTT command topic and the
 * correlated reply is awaited; and</li>
 * <li>a paho consumer on the operation's MQTT response topic that forwards
 * replies to the bridge (correlated by Uuid).</li>
 * </ul>
 *
 * <p>
 * The REST consumer is exposed on the DMP's consolidated REST surface
 * (camel-rest with jetty, same port as {@code /health}).
 *
 * @author aas-camel-dmp
 */
public class OperationRouteCreator implements IRouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(OperationRouteCreator.class);
	private static final String CONTENT_TYPE_JSON = "application/json";

	private final RouteBuilder routeBuilder;
	private final RoutesConfiguration routesConfiguration;
	private final MqttRequestReplyBridge bridge = new MqttRequestReplyBridge();

	public OperationRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		this.routeBuilder = routeBuilder;
		this.routesConfiguration = routesConfiguration;
		this.bridge.init(routeBuilder.getContext());
	}

	@Override
	public void addRouteToRouteBuilder(RouteConfiguration routeConfig) {
		if (!(routeConfig instanceof OperationRouteConfiguration)) {
			LOGGER.warn("operation route creator received a non-operation config: {}", routeConfig);
			return;
		}
		OperationRouteConfiguration operationConfig = (OperationRouteConfiguration) routeConfig;

		RestDataSourceConfiguration restSource = getRestDataSource(operationConfig);
		if (restSource == null || restSource.getRestPath() == null || restSource.getRestPath().isBlank()) {
			LOGGER.warn("operation {}: no REST datasource/path; skipping", operationConfig.getRouteId());
			return;
		}

		String requestLuaUri = firstTransformerUri(operationConfig.getTransformers());
		String responseLuaUri = firstTransformerUri(Collections.singletonList(operationConfig.getResponseTransformer()));
		String commandSinkUri = firstSinkUri(operationConfig.getDatasinks());
		String responseSourceUri = RouteCreatorHelper.getDataSourceEndpoint(
				routesConfiguration, operationConfig.getResponseDatasource());
		if (requestLuaUri == null || responseLuaUri == null || commandSinkUri == null) {
			LOGGER.warn("operation {}: missing transformer/sink endpoints; skipping", operationConfig.getRouteId());
			return;
		}

		// The action (skill) name is the last segment of the REST path; it is
		// also the Lua source key the AIMC blobs address (sources.<action>).
		String restPath = restSource.getRestPath();
		String action = restPath.substring(restPath.lastIndexOf('/') + 1);

		OperationContext operationContext = new OperationContext(
				action, requestLuaUri, responseLuaUri, commandSinkUri, operationConfig.getTimeoutSeconds());

		String routeId = operationConfig.getRouteId() != null
				? operationConfig.getRouteId() : "operation-" + action;

		// 1. REST consumer (consolidated camel-rest surface). Camel merges the
		// rest consumer and the direct route it targets into a single route;
		// the direct route's id wins, so it carries the canonical operation id.
		String method = restSource.getRestMethod() == null ? "POST" : restSource.getRestMethod();
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

		// Bridge route: delegate the invocation through the request/reply bridge.
		routeBuilder.from("direct:" + routeId)
				.routeId(routeId)
				.process(new OperationInvocationProcessor(bridge, operationContext));

		// 2. Response-topic consumer -> bridge (Uuid correlation).
		routeBuilder.from(responseSourceUri)
				.routeId(routeId + "-response")
				.process(new OperationResponseProcessor(bridge));

		LOGGER.info("operation route {}: {} {} -> MQTT {} <-> {} (timeout {}s)",
				routeId, method, restPath, operationContext.commandSinkUri,
				responseSourceUri, operationContext.timeoutSeconds);
	}

	private RestDataSourceConfiguration getRestDataSource(OperationRouteConfiguration operationConfig) {
		// Operation routes are single-source: use the first (only) datasource.
		List<String> datasources = operationConfig.getDatasources();
		if (datasources == null || datasources.isEmpty()) {
			return null;
		}
		Object source = routesConfiguration.getDatasources().get(datasources.get(0));
		if (source instanceof RestDataSourceConfiguration) {
			return (RestDataSourceConfiguration) source;
		}
		return null;
	}

	private String firstTransformerUri(java.util.List<String> transformerIds) {
		if (transformerIds == null || transformerIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataTransformerEndpoints(routesConfiguration, transformerIds)[0];
	}

	private String firstSinkUri(java.util.List<String> sinkIds) {
		if (sinkIds == null || sinkIds.isEmpty()) {
			return null;
		}
		return RouteCreatorHelper.getDataSinkEndpoints(routesConfiguration, sinkIds)[0];
	}
}
