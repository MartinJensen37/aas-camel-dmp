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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.routebuilder.DmpRouteBuilder;
import org.junit.jupiter.api.Test;

/**
 * Coverage of the ONE aggregation core under the FIXED execution semantic
 * (decision 2026-09-15): every source feeds a latest-value cache — async on
 * arrival, sync polled at its declared interval — and an update of any
 * source evaluates the mapping only once ALL sources have delivered. There
 * is no authorable policy; a config carrying an {@code executionPolicy}
 * block is a load-time error.
 */
class ExecutionPolicyRouteCreatorTest extends CamelTestSupport {

	static final class FixedSource extends DataSourceConfiguration {
		private final String uri;

		FixedSource(String uniqueId, String sourceId, String delivery, String uri) {
			super(uniqueId, null, 0);
			setSourceId(sourceId);
			setDelivery(delivery);
			if ("sync".equals(delivery)) {
				setPollingIntervalMs(100);
			}
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	static final class FixedSink extends DataSinkConfiguration {
		private final String uri;

		FixedSink(String uniqueId, String uri) {
			super(uniqueId);
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	private static RouteConfiguration route(String routeId,
			List<String> datasourceIds, List<String> sinkIds) {
		RouteConfiguration route = new RouteConfiguration(
				ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER, datasourceIds, List.of(), sinkIds);
		route.setRouteId(routeId);
		return route;
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				FixedSource nullable = new FixedSource("nullSource", "Nullable", "async", "direct:inNull");
				nullable.setValuePath("$.value");
				FixedSource sharedLeft = new FixedSource("sharedLeft", "Left", "sync", "seda:inShared");
				sharedLeft.setValuePath("$.left");
				FixedSource sharedRight = new FixedSource("sharedRight", "Right", "sync", "seda:inShared");
				sharedRight.setValuePath("$.right");
				FixedSource fastCadence = new FixedSource("fastCadence", "Fast", "sync", "seda:inDifferentCadence");
				FixedSource slowCadence = new FixedSource("slowCadence", "Slow", "sync", "seda:inDifferentCadence");
				slowCadence.setPollingIntervalMs(200);
				FixedSource eventLeft = new FixedSource("eventLeft", "EventLeft", "async", "seda:inSharedEvents");
				eventLeft.setValuePath("$.left");
				FixedSource eventRight = new FixedSource("eventRight", "EventRight", "async", "seda:inSharedEvents");
				eventRight.setValuePath("$.right");
				eventRight.setPollingIntervalMs(300);
				RoutesConfiguration routes = new RoutesConfiguration(
						Arrays.asList(
								// single-async route
								new FixedSource("stateA", "StationState", "async", "direct:inA"),
								nullable,
								// two-async route (gate on the full set)
								new FixedSource("stateB", "State", "async", "direct:inB"),
								new FixedSource("modeB", "Mode", "async", "direct:inModeB"),
								// periodic-style route: async cache + sync poller
								new FixedSource("pAsync", "PAsync", "async", "seda:inPAsync"),
								new FixedSource("pSync", "PSync", "sync", "seda:inPSync"),
								sharedLeft,
								sharedRight,
								fastCadence,
								slowCadence,
								eventLeft,
								eventRight,
								// sync-only route: the poller drives everything
								new FixedSource("sOnly", "S", "sync", "seda:inSOnly")),
						List.of(),
						List.of(new FixedSink("outAny", "mock:anyOut"),
								new FixedSink("outNull", "mock:nullOut"),
								new FixedSink("outAll", "mock:allOut"),
								new FixedSink("outPeriodic", "mock:periodicOut"),
								new FixedSink("outShared", "mock:sharedOut"),
								new FixedSink("outSharedEvents", "mock:sharedEventsOut"),
								new FixedSink("outSyncOnly", "mock:syncOnlyOut")),
						List.of());

				add(route("onAny", List.of("stateA"), List.of("outAny")), routes);
				add(route("onNull", List.of("nullSource"), List.of("outNull")), routes);
				add(route("onAll", List.of("stateB", "modeB"), List.of("outAll")), routes);
				add(route("periodic", List.of("pAsync", "pSync"), List.of("outPeriodic")), routes);
				add(route("shared", List.of("sharedLeft", "sharedRight"), List.of("outShared")), routes);
				add(route("differentCadence", List.of("fastCadence", "slowCadence"), List.of()), routes);
				add(route("sharedEvents", List.of("eventLeft", "eventRight"), List.of("outSharedEvents")), routes);
				add(route("syncOnly", List.of("sOnly"), List.of("outSyncOnly")), routes);
			}

			private void add(RouteConfiguration route, RoutesConfiguration routes) {
				new ExecutionPolicyRouteCreator(this, routes).addRouteToRouteBuilder(route);
			}
		};
	}


	private static Map<?, ?> sourcesOf(Object body) { return (Map<?, ?>) body; }

	@Test
	void asyncOnlyRouteFiresOnEveryArrivalOnceComplete() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:anyOut");
		out.expectedMessageCount(2);
		template.sendBody("direct:inA", "ONE");
		template.sendBody("direct:inA", "TWO");
		out.assertIsSatisfied(2000);

		Object body = out.getExchanges().get(1).getMessage().getBody();
		assertInstanceOf(Map.class, body);
		assertEquals("TWO", sourcesOf(body).get("StationState"));
	}

	@Test
	void deliveredJsonNullRemainsInSourcesMap() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:nullOut");
		out.expectedMessageCount(1);
		template.sendBodyAndHeader("direct:inNull", "{\"value\":null}",
				org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
		out.assertIsSatisfied(2000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertTrue(sources.containsKey("Nullable"), "a delivered null must not disappear from the blob input");
		assertEquals(null, sources.get("Nullable"));
	}

	@Test
	void gateHoldsUntilAllSourcesDeliveredThenStreamMerges() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:allOut");
		out.expectedMessageCount(2);

		template.sendBody("direct:inB", "s1"); // only one of two: held
		template.sendBody("direct:inModeB", "m1"); // completes the set: emit 1
		template.sendBody("direct:inB", "s2"); // fresh arrival: emit 2 (latest)

		out.assertIsSatisfied(4000);

		Map<?, ?> first = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("s1", first.get("State"));
		assertEquals("m1", first.get("Mode"));
		Map<?, ?> second = sourcesOf(out.getExchanges().get(1).getMessage().getBody());
		assertEquals("s2", second.get("State"), "latest value wins");
	}

	@Test
	void neverEmitsAPartialSourcesMap() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:allOut");
		// only State delivers; Mode never does → nothing is emitted
		template.sendBody("direct:inB", "lonely");
		Thread.sleep(500);
		assertEquals(0, out.getReceivedCounter(), "a partial map is never emitted");
	}

	@Test
	void syncPollerFeedsTheSameCacheAsAsyncSources() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:periodicOut");
		out.expectedMinimumMessageCount(1);

		template.sendBody("seda:inPAsync", "CACHED");
		template.sendBody("seda:inPSync", "FRESH");
		out.assertIsSatisfied(4000);

		boolean found = out.getReceivedExchanges().stream().anyMatch(e -> {
			Object body = e.getMessage().getBody();
			if (!(body instanceof Map<?, ?>)) {
				return false;
			}
			Map<?, ?> m = sourcesOf(body);
			return "CACHED".equals(m.get("PAsync")) && "FRESH".equals(m.get("PSync"));
		});
		assertTrue(found, "the sync poll tick evaluates the cached async snapshot");
	}

	@Test
	void syncOnlyRouteEmitsPerPollTick() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:syncOnlyOut");
		out.expectedMinimumMessageCount(1);

		template.sendBody("seda:inSOnly", "POLLED");
		out.assertIsSatisfied(4000);

		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("POLLED", sources.get("S"));
	}

	@Test
	void identicalAcquisitionsAreReadOnceAndProjectedToEverySourceId() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:sharedOut");
		out.expectedMessageCount(1);
		template.sendBody("seda:inShared", Map.of("left", "L", "right", "R"));
		out.assertIsSatisfied(3000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("L", sources.get("Left"));
		assertEquals("R", sources.get("Right"));
	}

	@Test
	void sameUriWithDifferentSyncCadencesKeepsSeparatePollRoutes() {
		var fast = context.getRouteDefinition("differentCadence-poll-fastCadence");
		var slow = context.getRouteDefinition("differentCadence-poll-slowCadence");
		assertNotNull(fast);
		assertNotNull(slow);
		assertTrue(fast.getInput().getEndpointUri().endsWith("?period=100"), fast.getInput().getEndpointUri());
		assertTrue(slow.getInput().getEndpointUri().endsWith("?period=200"), slow.getInput().getEndpointUri());
	}

	@Test
	void identicalSubscriptionsFanOneEventOutToEverySourceId() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:sharedEventsOut");
		out.expectedMessageCount(1);
		template.sendBody("seda:inSharedEvents", Map.of("left", "L", "right", "R"));
		out.assertIsSatisfied(3000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(0).getMessage().getBody());
		assertEquals("L", sources.get("EventLeft"));
		assertEquals("R", sources.get("EventRight"));
	}

	@Test
	void aSourceThatCannotReadAnEventDoesNotStopTheOthersOfItsAcquisition() throws Exception {
		MockEndpoint out = getMockEndpoint("mock:sharedEventsOut");
		out.expectedMessageCount(2);
		template.sendBody("seda:inSharedEvents", Map.of("left", "L", "right", "R"));
		template.sendBody("seda:inSharedEvents", Map.of("left", "L2"));
		out.assertIsSatisfied(3000);
		Map<?, ?> sources = sourcesOf(out.getExchanges().get(1).getMessage().getBody());
		assertEquals("L2", sources.get("EventLeft"));
		assertEquals("R", sources.get("EventRight"), "the last value of the source that could not read the event");
	}

	@Test
	void managedBuilderInventoriesAreExactAndIndependentForAsyncAndSyncSources() throws Exception {
		DmpRouteBuilder first = managedBuilder("mapping-A", "async-A", "sync-A");
		DmpRouteBuilder second = managedBuilder("mapping-B", "async-B", "sync-B");

		first.prepareBundle(context);
		second.prepareBundle(context);

		Set<String> firstIds = Set.of("mapping-A-source-async-A", "mapping-A-poll-sync-A", "mapping-A");
		Set<String> secondIds = Set.of("mapping-B-source-async-B", "mapping-B-poll-sync-B", "mapping-B");
		assertEquals(firstIds, first.getRouteInventory());
		assertEquals(secondIds, second.getRouteInventory());
		assertTrue(java.util.Collections.disjoint(first.getRouteInventory(), second.getRouteInventory()));
		assertThrows(UnsupportedOperationException.class,
				() -> first.getRouteInventory().add("not-owned"));
		assertEquals(List.of("mapping-A", "mapping-A-source-async-A", "mapping-A-poll-sync-A"),
				first.getPreparedRouteDefinitions().stream().map(org.apache.camel.model.RouteDefinition::getRouteId).toList());
		assertThrows(UnsupportedOperationException.class,
				() -> first.getPreparedRouteDefinitions().clear());
		assertThrows(IllegalStateException.class, () -> first.prepareBundle(context));
	}

	@Test
	void managedBuilderRejectsMissingAndDuplicateAuthoredRouteIdsWithoutChangingLegacyIds() {
		RouteConfiguration missingId = route(null, List.of("source"), List.of());
		RoutesConfiguration missingBundle = ordinaryBundle(missingId, "source", "async-source", "unused-sync");
		assertThrows(IllegalArgumentException.class,
				() -> DmpRouteBuilder.forManagedBundle(missingBundle, policyFactories()));
		assertNull(missingId.getRouteId());

		RouteConfiguration firstRoute = route("duplicate", List.of("source"), List.of());
		RouteConfiguration secondRoute = route("duplicate", List.of("source"), List.of());
		RoutesConfiguration duplicateBundle = ordinaryBundle(List.of(firstRoute, secondRoute), "source",
				"async-source", "unused-sync");
		assertThrows(IllegalArgumentException.class,
				() -> DmpRouteBuilder.forManagedBundle(duplicateBundle, policyFactories()));

		RouteConfiguration operationRoute = route("operation", List.of(), List.of());
		operationRoute.setRouteTrigger("unsupported-managed-trigger");
		assertThrows(IllegalArgumentException.class, () -> DmpRouteBuilder.forManagedBundle(
				ordinaryBundle(operationRoute, "source", "async-source", "unused-sync"), policyFactories()));

		RouteConfiguration legacy = route(null, List.of(), List.of());
		new DmpRouteBuilder(ordinaryBundle(legacy, "source", "async-source", "unused-sync"), policyFactories());
		assertEquals("route1", legacy.getRouteId());
	}

	private static DmpRouteBuilder managedBuilder(String routeId, String asyncId, String syncId) {
		RoutesConfiguration configuration = ordinaryBundle(route(routeId, List.of(asyncId, syncId), List.of()),
				asyncId, asyncId, syncId);
		return DmpRouteBuilder.forManagedBundle(configuration, policyFactories());
	}

	private static RoutesConfiguration ordinaryBundle(RouteConfiguration route, String asyncId,
			String asyncEndpointId, String syncEndpointId) {
		return ordinaryBundle(List.of(route), asyncId, asyncEndpointId, syncEndpointId);
	}

	private static RoutesConfiguration ordinaryBundle(List<RouteConfiguration> routes, String asyncId,
			String asyncEndpointId, String syncEndpointId) {
		return new RoutesConfiguration(
				List.of(new FixedSource(asyncEndpointId, asyncId, "async", "direct:" + asyncEndpointId),
						new FixedSource(syncEndpointId, "Sync", "sync", "direct:" + syncEndpointId)),
				List.of(), List.of(), routes);
	}

	private static Map<String, org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorFactory>
			policyFactories() {
		ExecutionPolicyRouteCreatorFactory factory = new ExecutionPolicyRouteCreatorFactory();
		return Map.of(factory.routeTrigger(), factory);
	}
}
