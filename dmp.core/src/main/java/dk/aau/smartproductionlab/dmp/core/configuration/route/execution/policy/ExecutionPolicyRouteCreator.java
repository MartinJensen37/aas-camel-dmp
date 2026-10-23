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
package dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ProcessorDefinition;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreator;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanIn;
import dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline.FanOut;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;
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

		for (String endpointId : routeConfig.getDatasources()) {
			RouteCreatorHelper.getDataSourceEndpoint(routesConfiguration, endpointId, routeId);
		}
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

		Map<Acquisition, List<String>> acquisitions = new LinkedHashMap<>();
		for (String endpointId : routeConfig.getDatasources()) {
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			acquisitions.computeIfAbsent(Acquisition.of(config), ignored -> new java.util.ArrayList<>()).add(endpointId);
		}
		for (List<String> endpointIds : acquisitions.values()) {
			String endpointId = endpointIds.get(0);
			DataSourceConfiguration config = routesConfiguration.getDatasources().get(endpointId);
			List<String> sourceIds = endpointIds.stream()
					.map(id -> FanIn.resolveSourceId(routesConfiguration, id)).toList();
			if (config != null && "sync".equalsIgnoreCase(config.getDelivery())) {
				// Sync sources issue a fresh Read when the connector has a read
				// adapter; consumer-only sources retain their polling consumer path.
				long periodMs = effectivePollingIntervalMs(config);
				LOGGER.info("route {}: {} sync source(s) polled every {} ms", routeId, endpointIds.size(), periodMs);
				String pollRouteId = routeId + "-poll-" + endpointId;
				registerRouteId(pollRouteId);
				registerIngressRouteId(pollRouteId);
				ProcessorDefinition<?> poll = routeBuilder
						.from("timer:" + timerName(routeId, endpointId) + "?period=" + periodMs)
						.routeId(pollRouteId);
				RouteCreatorHelper.configureSyncRead(poll, config.getConnectionURI(), SYNC_POLL_TIMEOUT_MS);
				poll.filter(exchange -> exchange.getMessage().getBody() != null)
						.setHeader(FanIn.SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceIds.get(0)))
						.process(fanIn.disassemble(endpointIds))
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
				sourceRoute = RouteCreatorHelper.normalizeSourceEvents(sourceRoute, config, routeId);
				sourceRoute
						.setHeader(FanIn.SOURCE_ID_HEADER, org.apache.camel.builder.Builder.constant(sourceIds.get(0)))
						.process(fanIn.disassemble(endpointIds))
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

	/** Per-source poll ceiling for one sync read. */
	public static final long SYNC_POLL_TIMEOUT_MS = Long.getLong("dmp.policy.syncPollTimeoutMs", 5000L);

	private static String timerName(String routeId, String endpointId) {
		return ("policy-" + routeId + "-" + endpointId).replaceAll("[^A-Za-z0-9_.-]", "-");
	}

	/** A read source is polled only at an authored interval; none is assumed. */
	private static long effectivePollingIntervalMs(DataSourceConfiguration config) {
		long authored = config.getPollingIntervalMs();
		if (authored <= 0) throw new IllegalArgumentException("read source " + config.getUniqueId()
				+ " has no polling interval; it needs a positive PollingInterval or DefaultPollingInterval");
		return authored;
	}

	/**
	 * What makes two sources share one acquisition: the same consumer or poll, delivered
	 * and normalized the same way. The normalizer is built from the group's first source,
	 * so its sources must also agree on whether their schema decodes the payload as JSON.
	 */
	private record Acquisition(Class<?> type, String uri, String delivery, long pollingIntervalMs,
			String sourceKind, String mode, Map<String, Object> eventFilter, boolean jsonSchema) {
		static Acquisition of(DataSourceConfiguration config) {
			if (config == null) return new Acquisition(null, null, null, 0, null, null, null, false);
			String delivery = config.getDelivery() == null ? null : config.getDelivery().toLowerCase(java.util.Locale.ROOT);
			long pollingIntervalMs = "sync".equals(delivery) ? effectivePollingIntervalMs(config) : 0;
			return new Acquisition(config.getClass(), config.getConnectionURI(), delivery, pollingIntervalMs,
					config.getSourceKind(), config.getMode(), config.getEventFilter(),
					FanIn.jsonSchema(config.getSchema()));
		}
	}
}
