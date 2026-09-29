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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.policy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreator;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.pipeline.FanIn;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.pipeline.FanOut;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ONE mapping-route creator: every MappingConfiguration compiles to
 * {@code sources[] + sinks[] + transformer(s)} and this creator wires exactly
 * that, with the FIXED execution semantic (decision 2026-09-15, replacing the
 * authorable ADR-020 policy):
 *
 * <ul>
 * <li>every source contributes its LATEST value to a per-route cache —
 * async sources on message arrival, sync sources polled by their own
 * declared {@code pollingIntervalMs} (the AIMC's IDTA polling interval, MC
 * level or per-source override);</li>
 * <li>an update of ANY source (async arrival or sync poll tick) evaluates
 * the mapping <b>only once ALL sources of the mapping have delivered</b> —
 * a partial sources map is never emitted (the blob contract).</li>
 * </ul>
 *
 * <p>
 * There are no authorable knobs: no trigger kinds, no barriers, no
 * completion timeouts, no staleness bounds — sync cadence comes from the
 * declared IDTA polling intervals (plan Q8), and a partial map is never
 * emitted (plan Q9's conservative disposition is now the only behavior).
 *
 * <p>
 * Fan-out reuses the shared transform-once multicast {@link FanOut}.
 */
public class ExecutionPolicyRouteCreator implements RouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPolicyRouteCreator.class);

	/** Header carrying the AIMC SourceId on async feeder routes. */
	private static final String SOURCE_ID_HEADER = "AIMC_SourceId";

	private final RouteBuilder routeBuilder;
	private final RoutesConfiguration routesConfiguration;

	public ExecutionPolicyRouteCreator(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration) {
		this.routeBuilder = routeBuilder;
		this.routesConfiguration = routesConfiguration;
	}

	@Override
	public void addRouteToRouteBuilder(RouteConfiguration routeConfig) {
		String routeId = routeConfig.getRouteId() != null ? routeConfig.getRouteId() : "<unnamed-policy>";

		String[] dataSourceEndpoints = RouteCreatorHelper.getDataSourceEndpoints(
				routesConfiguration, routeConfig.getDatasources(), routeId);
		String[] dataSinkEndpoints = RouteCreatorHelper.getDataSinkEndpoints(
				routesConfiguration, routeConfig.getDatasinks(), routeId);
		String[] dataTransformerEndpoints = RouteCreatorHelper.getDataTransformerEndpoints(
				routesConfiguration, routeConfig.getTransformers(), routeId);
		Map<String, String[]> datasinkMapping = RouteCreatorHelper.getDataSinkMapping(
				routesConfiguration, routeConfig.getDatasinkMappingConfiguration(), routeId);

		FanIn fanIn = new FanIn(routeConfig, routesConfiguration);
		String collectUri = "direct:policy-" + routeId;

		for (int i = 0; i < routeConfig.getDatasources().size(); i++) {
			String endpointId = routeConfig.getDatasources().get(i);
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			String sourceId = FanIn.resolveSourceId(routesConfiguration, endpointId);
			if (config != null && "sync".equalsIgnoreCase(config.getDelivery())) {
				// sync source: a dedicated scheduler polls the consumer at
				// its declared cadence and feeds the same cache
				long periodMs = config.getPollingIntervalMs() > 0
						? config.getPollingIntervalMs()
						: DEFAULT_SYNC_POLL_MS;
				LOGGER.info("route {}: sync source {} polled every {} ms", routeId, sourceId, periodMs);
				routeBuilder
						.from("timer:" + timerName(routeId, endpointId) + "?period=" + periodMs)
						.routeId(routeId + "-poll-" + endpointId)
						// pollEnrich retains the original exchange when a read returns
						// null; the timer body must never enter the source cache.
						.setBody(org.apache.camel.builder.Builder.constant(null))
						.pollEnrich(dataSourceEndpoints[i], SYNC_POLL_TIMEOUT_MS)
						.filter(exchange -> exchange.getMessage().getBody() != null)
						.setHeader(SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceId))
						.process(fanIn.disassemble(endpointId))
						.filter(fanIn::contributed)
						.to(collectUri);
			} else {
				// async source: the consumer feeds the cache and drives an
				// evaluation on every arrival
				ProcessorDefinition<?> sourceRoute = routeBuilder
						.from(routesConfiguration.getDatasources().get(endpointId).getConnectionURI())
						.routeId(routeId + "-source-" + endpointId)
						;
				if (config != null && "aas-element-event".equals(config.getSourceKind())) {
					sourceRoute = sourceRoute.process(new AasPropertyEventProcessor(config))
							.filter(exchange -> Boolean.TRUE.equals(
									exchange.getProperty(AasPropertyEventProcessor.MATCH_PROPERTY)));
				}
				sourceRoute
						.setHeader(SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceId))
						.process(fanIn.disassemble(endpointId))
						.filter(fanIn::contributed)
						.to(collectUri);
			}
		}

		ProcessorDefinition<?> cycle = routeBuilder.from(collectUri)
				.routeId(routeId)
				.filter(fanIn.gatePredicate())
				.process(fanIn.merge());

		FanOut.apply(cycle, routesConfiguration, routeConfig,
				dataTransformerEndpoints, dataSinkEndpoints, datasinkMapping, routeId);
	}

	/** Bounded poll cadence for sync sources that declare none. */
	static final long DEFAULT_SYNC_POLL_MS = Long.getLong("dmp.policy.defaultSyncPollMs", 5000L);

	/** Per-source poll ceiling for one sync read. */
	static final long SYNC_POLL_TIMEOUT_MS = Long.getLong("dmp.policy.syncPollTimeoutMs", 5000L);

	private static String timerName(String routeId, String endpointId) {
		return ("policy-" + routeId + "-" + endpointId).replaceAll("[^A-Za-z0-9_.-]", "-");
	}
}
