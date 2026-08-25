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
package org.eclipse.digitaltwin.basyx.databridge.core.regression.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.event.EventRouteCreator;
import org.junit.jupiter.api.Test;

/**
 * Tests the symmetric fan-in → transform → fan-out route model (SPEC §6.4).
 *
 * <p>
 * Verifies that with multiple sources, the transform fires only once every
 * source has delivered (latest value each), the {@code sources} map is keyed by
 * the AIMC SourceId, and the result fans out to the sinks.
 */
class EventRouteFanInFanOutTest extends CamelTestSupport {

	/** A minimal source config exposing a {@code sourceId}. */
	private static final class TestSourceConfiguration extends DataSourceConfiguration {
		TestSourceConfiguration(String uniqueId, String sourceId) {
			super(uniqueId, null, 0);
			setSourceId(sourceId);
		}

		TestSourceConfiguration(String uniqueId, String sourceId, String valuePath) {
			super(uniqueId, null, 0);
			setSourceId(sourceId);
			setValuePath(valuePath);
		}

		@Override
		public String getConnectionURI() {
			return "direct:" + getUniqueId();
		}
	}

	/** A minimal sink config. */
	private static final class TestSinkConfiguration extends DataSinkConfiguration {
		TestSinkConfiguration(String uniqueId) {
			super(uniqueId);
		}

		TestSinkConfiguration(String uniqueId, String sinkId, String valuePath) {
			super(uniqueId);
			setSinkId(sinkId);
			setValuePath(valuePath);
		}

		@Override
		public String getConnectionURI() {
			return "mock:" + getUniqueId();
		}
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				// Two sources with distinct AIMC SourceIds.
				DataSourceConfiguration srcA = new TestSourceConfiguration("srcA", "station_state");
				DataSourceConfiguration srcB = new TestSourceConfiguration("srcB", "station_mode");
				DataSinkConfiguration sink = new TestSinkConfiguration("result");

				RoutesConfiguration routes = new RoutesConfiguration(
						Arrays.asList(srcA, srcB),
						Arrays.asList(),
						Arrays.asList(sink),
						Arrays.asList());

				RouteConfiguration route = new RouteConfiguration(
						"event",
						Arrays.asList("srcA", "srcB"),
						Arrays.asList(),
						Arrays.asList("result"));
				route.setRouteId("fanin");

				// The transform: combine the two sources into one output map.
				// We use a processor as the "transform" that merges sources.
				EventRouteCreator creator = new EventRouteCreator(this, routes);
				creator.addRouteToRouteBuilder(route);
			}
		};
	}

	@Test
	void transformFiresOnlyAfterAllSourcesDeliver() throws Exception {
		MockEndpoint result = getMockEndpoint("mock:result");
		result.expectedMessageCount(1);

		// Deliver only the first source — the transform must NOT fire yet.
		template.sendBody("direct:srcA", "EXECUTE");
		result.assertIsNotSatisfied(500);

		// Deliver the second source — now the transform fires.
		template.sendBody("direct:srcB", "AUTO");
		result.assertIsSatisfied(1000);

		// The aggregated body is keyed by the AIMC SourceIds.
		Object body = result.getExchanges().get(0).getMessage().getBody();
		assertTrue(body instanceof Map, "expected aggregated map, got " + body);
		Map<?, ?> sources = (Map<?, ?>) body;
		assertEquals("EXECUTE", sources.get("station_state"));
		assertEquals("AUTO", sources.get("station_mode"));
	}

	@Test
	void latestValueWinsPerSource() throws Exception {
		MockEndpoint result = getMockEndpoint("mock:result");
		result.expectedMessageCount(1);

		// First source delivers twice; only the latest should be retained.
		template.sendBody("direct:srcA", "OLD");
		template.sendBody("direct:srcA", "NEW");
		template.sendBody("direct:srcB", "AUTO");
		result.assertIsSatisfied(1000);

		Object body = result.getExchanges().get(0).getMessage().getBody();
		Map<?, ?> sources = (Map<?, ?>) body;
		assertEquals("NEW", sources.get("station_state"));
		assertEquals("AUTO", sources.get("station_mode"));
	}

	@Test
	void sinkAssemblyConstructsOutgoingPayloadAroundSinkValue() throws Exception {
		// SPEC §8.4 #10 (fine sink): the datasink valuePath places the SinkId
		// value at the target field of the outgoing payload (assembly).
		DataSinkConfiguration sink = new TestSinkConfiguration("stageOut", "SetPoint", "$.SetPoint");

		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				RoutesConfiguration routes = new RoutesConfiguration(
						Arrays.asList(new TestSourceConfiguration("stageSrc", "station")),
						Arrays.asList(),
						Arrays.asList(sink),
						Arrays.asList());
				RouteConfiguration route = new RouteConfiguration(
						"event",
						Arrays.asList("stageSrc"),
						Arrays.asList(),
						Arrays.asList("stageOut"));
				route.setRouteId("sink-assemble");
				new EventRouteCreator(this, routes).addRouteToRouteBuilder(route);
			}
		});

		MockEndpoint result = getMockEndpoint("mock:stageOut");
		result.expectedMessageCount(1);

		// A "transform result" keyed by SinkId.
		template.sendBody("direct:stageSrc", "{\"State\": \"EXECUTE\", \"SetPoint\": \"25.5\"}");
		result.assertIsSatisfied(1000);

		String body = result.getExchanges().get(0).getMessage().getBody(String.class);
		assertEquals("{\"SetPoint\":\"25.5\"}", body, "fine sink must construct the outgoing payload");
	}

	@Test
	void coarseSinkWritesWholeSinkIdValue() throws Exception {
		// SPEC §8.4 #10 (coarse sink, write whole SinkId value as-is).
		DataSinkConfiguration sink = new TestSinkConfiguration("coarseOut", "State", null);

		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				RoutesConfiguration routes = new RoutesConfiguration(
						Arrays.asList(new TestSourceConfiguration("coarseSrc", "station")),
						Arrays.asList(),
						Arrays.asList(sink),
						Arrays.asList());
				RouteConfiguration route = new RouteConfiguration(
						"event",
						Arrays.asList("coarseSrc"),
						Arrays.asList(),
						Arrays.asList("coarseOut"));
				route.setRouteId("sink-coarse");
				new EventRouteCreator(this, routes).addRouteToRouteBuilder(route);
			}
		});

		MockEndpoint result = getMockEndpoint("mock:coarseOut");
		result.expectedMessageCount(1);

		template.sendBody("direct:coarseSrc", "{\"State\": \"EXECUTE\", \"SetPoint\": \"25.5\"}");
		result.assertIsSatisfied(1000);

		String body = result.getExchanges().get(0).getMessage().getBody(String.class);
		assertEquals("EXECUTE", body, "coarse sink must write the whole SinkId value");
	}

	@Test
	void fineSourceIsDisassembledBeforeAggregation() throws Exception {
		// SPEC §8.4 #9: one coarse + one fine source fanned-in → the aggregated
		// sources map mixes whole-payload (coarse) and leaf (fine) values.
		DataSourceConfiguration fine = new TestSourceConfiguration("fineA", "station_state", "$.State");
		DataSourceConfiguration coarse = new TestSourceConfiguration("fineB", "station_mode");

		// Distinct endpoint names — the shared createRouteBuilder fixture already
		// consumes direct:srcA/srcB -> mock:result.
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				DataSinkConfiguration sink = new TestSinkConfiguration("fineResult");
				RoutesConfiguration routes = new RoutesConfiguration(
						Arrays.asList(fine, coarse),
						Arrays.asList(),
						Arrays.asList(sink),
						Arrays.asList());
				RouteConfiguration route = new RouteConfiguration(
						"event",
						Arrays.asList("fineA", "fineB"),
						Arrays.asList(),
						Arrays.asList("fineResult"));
				route.setRouteId("fanin-fine");
				new EventRouteCreator(this, routes).addRouteToRouteBuilder(route);
			}
		});

		MockEndpoint result = getMockEndpoint("mock:fineResult");
		result.expectedMessageCount(1);

		template.sendBody("direct:fineA", "{\"State\": \"EXECUTE\", \"ProcessQueue\": {}}");
		template.sendBody("direct:fineB", "AUTO");
		result.assertIsSatisfied(1000);

		Object body = result.getExchanges().get(0).getMessage().getBody();
		Map<?, ?> sources = (Map<?, ?>) body;
		assertEquals("EXECUTE", sources.get("station_state"), "fine source must deliver the extracted leaf");
		assertEquals("AUTO", sources.get("station_mode"), "coarse source must deliver the whole payload");
	}
}