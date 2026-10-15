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
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy;

import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreator;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.FanIn;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.FanOut;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SourceEventAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousSourceReadAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.routebuilder.DmpRouteBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ONE mapping-route creator: every MappingConfiguration compiles to
 * {@code sources[] + sinks[] + transformer(s)} and this creator wires exactly
 * that, with acquisition, trigger selection and readiness kept independent:
 *
 * <ul>
 * <li>every source contributes its LATEST value to a per-route cache —
 * async sources on message arrival, sync sources polled by their own
 * declared {@code pollingIntervalMs} (the AIMC's IDTA polling interval, MC
 * level or per-source override);</li>
 * <li>every valid source update refreshes the cached contribution; an update
 * evaluates the mapping only when its SourceId is selected by optional
 * {@code ExecutionSemantics.TriggerSourceIds} (absent = legacy any-source)
 * and once all required sources have delivered.</li>
 * </ul>
 *
 * <p>
 * Trigger selection never changes how an endpoint is read or observed.
 * Selected source contributions still wait for a complete sources snapshot;
 * unselected contributions update enrichment state without executing Lua.
 *
 * <p>
 * Fan-out reuses the shared transform-once multicast {@link FanOut}.
 */
public class ExecutionPolicyRouteCreator implements RouteCreator {

	private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionPolicyRouteCreator.class);

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
		Map<String, String[]> datasinkMapping = RouteCreatorHelper.getDataSinkMappingById(
				routesConfiguration, routeConfig.getDatasinkMappingConfiguration(), routeId);

		FanIn fanIn = new FanIn(routeConfig, routesConfiguration);
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerManagedResource(fanIn);
		List<String> triggerSourceIds = routeConfig.getTriggerSourceIds();
		if (triggerSourceIds != null && !fanIn.expectedSourceIds().containsAll(triggerSourceIds)) {
			throw new IllegalArgumentException("route " + routeId
					+ " triggerSourceIds must identify sources on this route");
		}
		String collectUri = "direct:policy-" + routeId;

		// Install the collector before source/timer ingress routes so it is ready
		// before any source can contribute during dynamic route startup.
		registerRouteId(routeId);
		if (routeConfig.getDatasources().isEmpty()) registerIngressRouteId(routeId);
		ProcessorDefinition<?> cycle = routeBuilder.from(collectUri)
				.routeId(routeId)
				.filter(fanIn.gatePredicate())
				.process(fanIn.merge());
		FanOut.apply(cycle, routesConfiguration, routeConfig,
				dataTransformerEndpoints, dataSinkEndpoints, datasinkMapping, routeId);

		for (int i = 0; i < routeConfig.getDatasources().size(); i++) {
			String endpointId = routeConfig.getDatasources().get(i);
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			String sourceId = FanIn.resolveSourceId(routesConfiguration, endpointId);
			if (config != null && "sync".equalsIgnoreCase(config.getDelivery())) {
				// Sync sources issue a fresh Read when the connector has a read
				// adapter; consumer-only sources retain their polling consumer path.
				long periodMs = config.getPollingIntervalMs();
				if (periodMs <= 0) {
					periodMs = DEFAULT_DEVICE_POLL_MS;
				}
				LOGGER.info("route {}: sync source {} polled every {} ms", routeId, sourceId, periodMs);
				String pollRouteId = routeId + "-poll-" + endpointId;
				registerRouteId(pollRouteId);
				registerIngressRouteId(pollRouteId);
				ProcessorDefinition<?> poll = routeBuilder
						.from("timer:" + timerName(routeId, endpointId) + "?period=" + periodMs)
						.routeId(pollRouteId);
				SynchronousSourceReadAdapter readAdapter = synchronousReadAdapter(dataSourceEndpoints[i]);
				if (readAdapter == null) {
					poll.setBody(org.apache.camel.builder.Builder.constant(null))
							.pollEnrich(dataSourceEndpoints[i], SYNC_POLL_TIMEOUT_MS);
				} else {
					readAdapter.configureRead(poll, dataSourceEndpoints[i]);
				}
				poll.filter(exchange -> exchange.getMessage().getBody() != null)
						.setHeader(FanIn.SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceId))
						.process(fanIn.disassemble(endpointId))
						.filter(fanIn::contributed)
						.filter(exchange -> fanIn.selectedTrigger(exchange, triggerSourceIds))
						.to(collectUri);
			} else {
				// async source: the consumer feeds the cache and drives an
				// evaluation on every arrival
				String sourceRouteId = routeId + "-source-" + endpointId;
				registerRouteId(sourceRouteId);
				registerIngressRouteId(sourceRouteId);
				ProcessorDefinition<?> sourceRoute = routeBuilder
						.from(routesConfiguration.getDatasources().get(endpointId).getConnectionURI())
						.routeId(sourceRouteId)
						;
				if (config != null && config.getSourceKind() != null
						&& !config.getSourceKind().isBlank()) {
					SourceEventAdapter adapter = sourceEventAdapter(config.getSourceKind());
					if (adapter == null) {
						throw new IllegalArgumentException("route " + routeId
								+ " has no connector for event sourceKind " + config.getSourceKind());
					}
					sourceRoute = sourceRoute.process(adapter.normalizer(config))
							.filter(exchange -> Boolean.TRUE.equals(
								exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY)));
				}
				sourceRoute
						.setHeader(FanIn.SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceId))
						.process(fanIn.disassemble(endpointId))
						.filter(fanIn::contributed)
						.filter(exchange -> fanIn.selectedTrigger(exchange, triggerSourceIds))
						.to(collectUri);
			}
		}

	}

	private void registerRouteId(String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder dmpRouteBuilder) {
			dmpRouteBuilder.registerRouteId(routeId);
		}
	}

	private void registerIngressRouteId(String routeId) {
		if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerIngressRouteId(routeId);
	}

	/** Backward-compatible cadence for device Read sources lacking authored intervals. */
	static final long DEFAULT_DEVICE_POLL_MS = Long.getLong("dmp.policy.defaultSyncPollMs", 5000L);

	private static SourceEventAdapter sourceEventAdapter(String kind) {
		return java.util.ServiceLoader.load(SourceEventAdapter.class).stream()
				.map(java.util.ServiceLoader.Provider::get)
				.filter(adapter -> kind.equals(adapter.sourceKind()))
				.findFirst().orElse(null);
	}

	private static SynchronousSourceReadAdapter synchronousReadAdapter(String endpointUri) {
		return java.util.ServiceLoader.load(SynchronousSourceReadAdapter.class).stream()
				.map(java.util.ServiceLoader.Provider::get)
				.filter(adapter -> adapter.supports(endpointUri))
				.findFirst().orElse(null);
	}

	/** Per-source poll ceiling for one sync read. */
	static final long SYNC_POLL_TIMEOUT_MS = Long.getLong("dmp.policy.syncPollTimeoutMs", 5000L);

	private static String timerName(String routeId, String endpointId) {
		return ("policy-" + routeId + "-" + endpointId).replaceAll("[^A-Za-z0-9_.-]", "-");
	}
}
