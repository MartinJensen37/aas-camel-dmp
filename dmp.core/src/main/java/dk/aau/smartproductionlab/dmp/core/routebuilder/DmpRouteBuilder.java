/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.core.routebuilder;

import java.util.Collections;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.rest.RestDefinition;
import org.apache.camel.tooling.model.Strings;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreator;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorFactory;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;

/**
 * This factory is used to create the apache camel routes for the data bridge
 * component
 *
 * @author fischer, mateusmolina
 *
 */
public class DmpRouteBuilder extends RouteBuilder {
	private static final String ROUTE_ID_PREFIX = "route";
	private RoutesConfiguration routesConfiguration;
	private Map<String, RouteCreatorFactory> routeCreatorFactoryMap;
	private final Set<String> routeInventory = new LinkedHashSet<>();
	private final Set<String> ingressInventory = new LinkedHashSet<>();
	private final Set<String> endpointInventory = new LinkedHashSet<>();
	private volatile boolean ingressEnabled;
	private final List<RestReservation> restInventory = new ArrayList<>();
	private final Set<ManagedRouteResource> managedResources = Collections.newSetFromMap(new IdentityHashMap<>());
	private final boolean managedBundle;
	private List<RouteDefinition> preparedRouteDefinitions;
	private boolean preparationStarted;
	private boolean configurationStarted;
	private boolean configurationComplete;

	public DmpRouteBuilder(RoutesConfiguration configuration, Map<String, RouteCreatorFactory> routeCreatorFactoryMap) {
		this.managedBundle = false;
		this.routesConfiguration = configureRouteIds(configuration);
		this.routeCreatorFactoryMap = routeCreatorFactoryMap;
	}

	private DmpRouteBuilder(RoutesConfiguration configuration,
			Map<String, RouteCreatorFactory> routeCreatorFactoryMap, boolean managedBundle) {
		this.managedBundle = managedBundle;
		if (managedBundle) {
			validateManagedRouteIds(configuration);
			this.routesConfiguration = configuration;
		} else {
			this.routesConfiguration = configureRouteIds(configuration);
		}
		this.routeCreatorFactoryMap = routeCreatorFactoryMap;
	}

	/** Creates a builder for one managed bundle, requiring explicit unique mapping IDs. */
	public static DmpRouteBuilder forManagedBundle(RoutesConfiguration configuration,
			Map<String, RouteCreatorFactory> routeCreatorFactoryMap) {
		return new DmpRouteBuilder(configuration, routeCreatorFactoryMap, true);
	}

	/** Returns the exact route IDs registered while constructing this bundle. */
	public Set<String> getRouteInventory() {
		return Collections.unmodifiableSet(new LinkedHashSet<>(routeInventory));
	}

	/** Configures this managed bundle once and snapshots its exact route definitions. */
	public void prepareBundle(CamelContext camelContext) throws Exception {
		if (preparationStarted) {
			throw new IllegalStateException("managed bundle preparation has already been attempted");
		}
		preparationStarted = true;
		setCamelContext(camelContext);
		configure();
		List<RouteDefinition> definitions = new ArrayList<>(getRouteCollection().getRoutes());
		for (RestDefinition rest : getRestCollection().getRests()) {
			definitions.addAll(rest.asRouteDefinition(camelContext));
		}
		Set<String> definitionIds = new LinkedHashSet<>();
		int restIngressDefinitions = 0;
		for (RouteDefinition definition : definitions) {
			definition.autoStartup(false);
			registerEndpointUri(definition.getInput().getEndpointUri());
			collectEndpoints(definition);
			if (ingressInventory.contains(definition.getRouteId())) {
				definition.getOutputs().add(0, new org.apache.camel.model.ProcessDefinition(exchange -> {
					if (!ingressEnabled) {
						exchange.getMessage().setHeader(org.apache.camel.Exchange.HTTP_RESPONSE_CODE, 503);
						exchange.getMessage().setBody("DMP mapping temporarily unavailable");
						exchange.setRouteStop(true);
					}
				}));
			}
			if (definition.getRouteId() == null) {
				restIngressDefinitions++;
			} else if (!definitionIds.add(definition.getRouteId())) {
				throw new IllegalArgumentException("managed bundle has duplicate constructed route IDs: "
						+ definitions.stream().map(RouteDefinition::getRouteId).toList());
			}
		}
		long restReservations = restInventory.size();
		if (restIngressDefinitions > restReservations) {
			throw new IllegalArgumentException("managed REST reservations do not match anonymous REST ingress definitions: "
					+ restIngressDefinitions + " > " + restReservations);
		}
		Set<String> expectedRouteIds = new LinkedHashSet<>(definitionIds);
		for (RestReservation rest : restInventory) {
			if (rest.restRouteId() != null) expectedRouteIds.add(rest.restRouteId());
		}
		if (!expectedRouteIds.equals(routeInventory)) {
			throw new IllegalStateException("managed route inventory does not match constructed route definitions: inventory="
					+ routeInventory + " definitions=" + definitions.stream().map(RouteDefinition::getRouteId).toList());
		}
		if (!routeInventory.containsAll(ingressInventory)) {
			throw new IllegalStateException("ingress inventory contains an unowned route");
		}
		preparedRouteDefinitions = List.copyOf(definitions);
	}

	/** Returns prepared route definitions for exact preflight/installation handling. */
	public List<RouteDefinition> getPreparedRouteDefinitions() {
		return preparedRouteDefinitions != null ? preparedRouteDefinitions
				: List.copyOf(getRouteCollection().getRoutes());
	}

	/** Returns the distinct resources bound by this managed mapping. */
	public List<ManagedRouteResource> getManagedResources() {
		return List.copyOf(managedResources);
	}

	public List<RestReservation> getRestInventory() { return List.copyOf(restInventory); }
	public Set<String> getIngressInventory() { return Set.copyOf(ingressInventory); }
	public Set<String> getEndpointInventory() { return Set.copyOf(endpointInventory); }
	public void setIngressEnabled(boolean enabled) { ingressEnabled = enabled; }
	public void registerIngressRouteId(String routeId) { ingressInventory.add(routeId); }
	public void registerEndpointUri(String uri) {
		if (uri != null && !uri.isBlank()) endpointInventory.add(uri);
	}

	private void collectEndpoints(org.apache.camel.model.ProcessorDefinition<?> node) {
		if (node instanceof org.apache.camel.model.ToDefinition to) registerEndpointUri(to.getEndpointUri());
		if (node instanceof org.apache.camel.model.WireTapDefinition tap) registerEndpointUri(tap.getUri());
		for (var output : node.getOutputs()) collectEndpoints(output);
	}

	public void registerRestReservation(String method, String path) {
		registerRestReservation(method, path, null, null);
	}

	public void registerRestReservation(String method, String path, RestDefinition definition) {
		registerRestReservation(method, path, definition, null);
	}

	public void registerRestReservation(String method, String path, RestDefinition definition, String restRouteId) {
		if (configurationComplete) throw new IllegalStateException("cannot register REST reservations after preparation");
		if (restRouteId != null) {
			registerRouteId(restRouteId);
			registerIngressRouteId(restRouteId);
		}
		RestReservation reservation = new RestReservation(method.toUpperCase(java.util.Locale.ROOT), path,
				restRouteId, definition);
		if (restInventory.contains(reservation)) throw new IllegalArgumentException("duplicate REST reservation " + reservation);
		restInventory.add(reservation);
	}

	public void registerManagedResource(ManagedRouteResource resource) {
		if (configurationComplete) throw new IllegalStateException("cannot register resources after preparation");
		if (managedResources.contains(resource)) return;
		try {
			resource.bind(getCamelContext());
			managedResources.add(resource);
		} catch (Exception e) {
			throw new IllegalStateException("failed to bind managed route resource", e);
		}
	}

	/** Records a route created by this builder; duplicate/invalid IDs are rejected. */
	public void registerRouteId(String routeId) {
		if (configurationComplete) {
			throw new IllegalStateException("cannot register routes after managed bundle preparation has started");
		}
		if (Strings.isNullOrEmpty(routeId) || routeId.isBlank()) {
			throw new IllegalArgumentException("managed bundle route ID must not be blank");
		}
		if (!routeInventory.add(routeId)) {
			throw new IllegalArgumentException("duplicate route ID in managed bundle: " + routeId);
		}
	}

	@Override
	public void configure() throws Exception {
		if (configurationStarted) {
			if (configurationComplete) return;
			throw new IllegalStateException("route builder configuration failed after it had started");
		}
		configurationStarted = true;
		for (RouteConfiguration routeConfig : routesConfiguration.getRoutes()) {
			RouteCreator routeCreator = resolveRouteCreator(routeConfig);

			setCamelContextInRouteEntities();

			routeCreator.addRouteToRouteBuilder(routeConfig);
		}
		configurationComplete = true;
		if (managedBundle) {
			routesConfiguration.getDatasources().values().forEach(entity -> registerEndpointUri(entity.getConnectionURI()));
			routesConfiguration.getDatasinks().values().forEach(entity -> registerEndpointUri(entity.getConnectionURI()));
			routesConfiguration.getTransformers().values().forEach(entity -> registerEndpointUri(entity.getConnectionURI()));
			managedResources.forEach(resource -> resource.endpointUris().forEach(this::registerEndpointUri));
		}
	}

	private RouteCreator resolveRouteCreator(RouteConfiguration routeConfig) {
		String trigger = routeConfig.getRouteTrigger();
		if (trigger == null || !routeCreatorFactoryMap.containsKey(trigger)) {
			throw new IllegalArgumentException("route " + routeConfig.getRouteId()
					+ ": no route creator factory registered for trigger '" + trigger + "'");
		}
		return routeCreatorFactoryMap.get(trigger).create(this, routesConfiguration);
	}

	private RoutesConfiguration configureRouteIds(RoutesConfiguration routesConfiguration) {
		long incrementalId = 1;
		for (RouteConfiguration route : routesConfiguration.getRoutes()) {
			if (Strings.isNullOrEmpty(route.getRouteId())) {
				route.setRouteId(ROUTE_ID_PREFIX + incrementalId);
				incrementalId++;
			}
		}

		return routesConfiguration;
	}

	private static void validateManagedRouteIds(RoutesConfiguration configuration) {
		if (configuration == null || configuration.getRoutes() == null || configuration.getRoutes().isEmpty()) {
			throw new IllegalArgumentException("managed bundle must contain at least one route with an explicit route ID");
		}
		Set<String> authoredIds = new LinkedHashSet<>();
		for (RouteConfiguration route : configuration.getRoutes()) {
			String routeId = route == null ? null : route.getRouteId();
			if (Strings.isNullOrEmpty(routeId) || routeId.isBlank()) {
				throw new IllegalArgumentException("managed bundle routes require explicit nonblank route IDs");
			}
			if (!authoredIds.add(routeId)) {
				throw new IllegalArgumentException("duplicate route ID in managed bundle: " + routeId);
			}
			String trigger = route.getRouteTrigger();
			if (!dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER
					.equals(trigger) && !"bridge".equals(trigger)) {
				throw new IllegalArgumentException("managed route creator is not supported by bundle inventory: " + trigger);
			}
		}
	}

	private void setCamelContextInRouteEntities() {
		routesConfiguration.getDatasinks().entrySet().forEach(c -> {
			CamelContextAware.trySetCamelContext(c.getValue(), getCamelContext());
			bindEndpointResource(c.getValue());
		});
		routesConfiguration.getDatasources().entrySet().forEach(c -> {
			CamelContextAware.trySetCamelContext(c.getValue(), getCamelContext());
			bindEndpointResource(c.getValue());
		});
		routesConfiguration.getTransformers().entrySet().forEach(c -> {
			CamelContextAware.trySetCamelContext(c.getValue(), getCamelContext());
			if (managedBundle && c.getValue() instanceof ManagedRouteResource resource && !managedResources.contains(resource)) {
				bindResource(resource);
				managedResources.add(resource);
			}
		});
	}

	/** Sources and sinks may own registry bindings their URIs reference, such as credentials. */
	private void bindEndpointResource(Object entity) {
		if (!(entity instanceof ManagedRouteResource resource) || managedResources.contains(resource)) return;
		bindResource(resource);
		if (managedBundle) managedResources.add(resource);
	}

	private void bindResource(ManagedRouteResource resource) {
		try {
			resource.bind(getCamelContext());
		} catch (Exception e) {
			throw new IllegalStateException("failed to bind managed route resource", e);
		}
	}

	public record RestReservation(String method, String path, String restRouteId, RestDefinition definition) {
		public RestReservation(String method, String path) { this(method, path, null, null); }
		@Override public boolean equals(Object other) {
			return other instanceof RestReservation that && method.equals(that.method) && path.equals(that.path);
		}
		@Override public int hashCode() { return java.util.Objects.hash(method, path); }
	}
}
