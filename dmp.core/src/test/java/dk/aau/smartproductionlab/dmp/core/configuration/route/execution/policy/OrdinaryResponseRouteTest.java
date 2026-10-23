/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.execution.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import org.junit.jupiter.api.Test;

/** An ordinary mapping's response half: after its sinks, also when they failed, before the next evaluation. */
class OrdinaryResponseRouteTest extends CamelTestSupport {

	private final AtomicReference<Map<?, ?>> responseMeta = new AtomicReference<>();
	private final AtomicReference<Object> requestSources = new AtomicReference<>();

	private static final class Source extends DataSourceConfiguration {
		private final String uri;
		Source(String uniqueId, String sourceId, String uri) {
			super(uniqueId, null, 0);
			setSourceId(sourceId);
			setDelivery("async");
			this.uri = uri;
		}
		@Override public String getConnectionURI() { return uri; }
	}

	private static final class Sink extends DataSinkConfiguration {
		private final String uri;
		Sink(String uniqueId, String sinkId, String uri) {
			super(uniqueId);
			setSinkId(sinkId);
			this.uri = uri;
		}
		@Override public String getConnectionURI() { return uri; }
	}

	private static final class Transformer extends DataTransformerConfiguration {
		private final String uri;
		Transformer(String uniqueId, String uri) {
			super(uniqueId);
			this.uri = uri;
		}
		@Override public String getConnectionURI() { return uri; }
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				// the request Lua passes the value to the device; the device rejects "reject"
				from("direct:requestLua").process(exchange -> exchange.getMessage().setBody(
						Map.of("Setpoint", ((Map<?, ?>) exchange.getMessage().getBody()).get("Value"))));
				from("direct:device").process(exchange -> {
					if ("reject".equals(exchange.getMessage().getBody()))
						throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
								"DEVICE_REJECTED", "the device rejected the write", Map.of());
				}).to("mock:device");
				// the response Lua logs what was written and the outcome
				from("direct:responseLua").process(exchange -> {
					responseMeta.set(exchange.getProperty(DataTransformerConfiguration.META, Map.class));
					requestSources.set(exchange.getProperty(DataTransformerConfiguration.REQUEST_SOURCES));
					Map<?, ?> invocation = (Map<?, ?>) responseMeta.get().get("invocation");
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("outcome", invocation.get("outcome"));
					row.put("written", ((Map<?, ?>) exchange.getMessage().getBody()).get("Written"));
					exchange.getMessage().setBody(Map.of("Log", row));
				});

				RoutesConfiguration routes = new RoutesConfiguration(
						List.of(new Source("value", "Value", "direct:in")),
						List.of(new Transformer("requestLua", "direct:requestLua"),
								new Transformer("responseLua", "direct:responseLua")),
						List.of(new Sink("setpoint", "Setpoint", "direct:device"),
								new Sink("log", "Log", "mock:log")),
						List.of());
				RouteConfiguration route = new RouteConfiguration(ExecutionPolicyRouteConfiguration.ROUTE_TRIGGER,
						List.of("value"), List.of("requestLua"), List.of("setpoint"));
				route.setRouteId("logged");
				Map<String, Object> trigger = new LinkedHashMap<>();
				trigger.put(RouteConfiguration.RESPONSE_TRANSFORMER, "responseLua");
				trigger.put(RouteConfiguration.RESPONSE_SOURCE_BINDINGS,
						List.of(Map.of("sourceId", "Written", "sinkId", "Setpoint")));
				trigger.put(RouteConfiguration.RESPONSE_SINK_BINDINGS,
						List.of(Map.of("sinkId", "Log", "dataSinkId", "log")));
				route.setTriggerData(trigger);
				new ExecutionPolicyRouteCreator(this, routes).addRouteToRouteBuilder(route);
			}
		};
	}

	@Test
	void theResponseProjectsTheWrittenValueAndIsWrittenBeforeTheEvaluationEnds() throws Exception {
		MockEndpoint device = getMockEndpoint("mock:device");
		MockEndpoint log = getMockEndpoint("mock:log");
		template.sendBody("direct:in", 21);
		// written inside the evaluation: nothing to wait for once the source's send returned
		assertEquals(1, device.getReceivedCounter());
		assertEquals(1, log.getReceivedCounter());
		assertEquals(Map.of("outcome", "Succeeded", "written", 21), log.getExchanges().get(0).getMessage().getBody());
		Map<?, ?> invocation = (Map<?, ?>) responseMeta.get().get("invocation");
		assertTrue(invocation.containsKey("id") && invocation.containsKey("started") && invocation.containsKey("durationMs"),
				String.valueOf(invocation));
		assertEquals(Map.of("Log", Map.of("kind", "mock")), responseMeta.get().get("sinks"));
		assertEquals(Map.of("Value", 21), requestSources.get(), "the response sees what the request transformation saw");
	}

	@Test
	void aFailedEvaluationStillRunsItsResponseAndStillFails() throws Exception {
		MockEndpoint log = getMockEndpoint("mock:log");
		var failure = assertThrows(CamelExecutionException.class, () -> template.sendBody("direct:in", "reject"));
		assertTrue(String.valueOf(failure.getCause()).contains("could not deliver"), String.valueOf(failure.getCause()));
		assertEquals(1, log.getReceivedCounter());
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("outcome", "Failed");
		row.put("written", null);
		assertEquals(row, log.getExchanges().get(0).getMessage().getBody(), "a failed evaluation has no ResponseSources");
		Map<?, ?> error = (Map<?, ?>) ((Map<?, ?>) responseMeta.get().get("invocation")).get("error");
		assertEquals("DEVICE_REJECTED", error.get("code"), "the sink's own error: " + error);
		assertEquals("REMOTE", error.get("category"));
	}
}
