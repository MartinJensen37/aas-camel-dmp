package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.awaitility.Awaitility.await;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.aas.AASComponent;
import org.eclipse.digitaltwin.basyx.dmp.aas.AASEndpoint;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASPollingConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteCreator;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpPollingConsumerConfiguration;
import org.junit.Test;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;

/** Exercises a simulated async feed with actual HTTP and AAS polling consumers. */
public class MixedSourceFanInTest {
	@Test
	public void failedReadDoesNotCompleteGateAndLaterPollUsesAllThreeSources() throws Exception {
		AtomicBoolean aasReady = new AtomicBoolean();
		AtomicInteger httpValue = new AtomicInteger(7);
		AtomicInteger httpReads = new AtomicInteger();
		AtomicInteger aasReads = new AtomicInteger();
		AtomicInteger sourceErrors = new AtomicInteger();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/pressure", request -> {
			httpReads.incrementAndGet();
			byte[] response = ("{\"value\":" + httpValue.get() + "}").getBytes(StandardCharsets.UTF_8);
			request.sendResponseHeaders(200, response.length);
			try (var body = request.getResponseBody()) {
				body.write(response);
			}
		});
		server.createContext("/submodels/demo/submodel-elements/Temperature", request -> {
			aasReads.incrementAndGet();
			boolean ready = aasReady.get();
			byte[] response = (ready ? "{\"value\":19}" : "unavailable")
					.getBytes(StandardCharsets.UTF_8);
			request.sendResponseHeaders(ready ? 200 : 503, response.length);
			try (var body = request.getResponseBody()) {
				body.write(response);
			}
		});
		server.start();
		try {
			String base = "http://127.0.0.1:" + server.getAddress().getPort();
			DataSourceConfiguration async = new DataSourceConfiguration("asyncEvent", null, 0) {
				@Override
				public String getConnectionURI() {
					return "seda:stationUpdates";
				}
			};
			async.setSourceId("StationState");
			async.setDelivery("async");

			HttpPollingConsumerConfiguration http = new HttpPollingConsumerConfiguration(
					"httpPoll", base + "/pressure", 0, null, null);
			http.setSourceId("Pressure");
			http.setDelivery("sync");
			http.setValuePath("$.value");
			http.setPollingIntervalMs(150);

			AASPollingConsumerConfiguration aas = new AASPollingConsumerConfiguration(
					"aasPoll", null, 0, "Temperature", base + "/submodels/demo", "DotAAS-V3");
			aas.setSourceId("Temperature");
			aas.setDelivery("sync");
			aas.setValuePath("$.value");
			aas.setPollingIntervalMs(200);
			DataSinkConfiguration sink = new DataSinkConfiguration("capture") {
				@Override
				public String getConnectionURI() {
					return "direct:capture";
				}
			};
			RouteConfiguration route = new RouteConfiguration("execution-policy",
					List.of("asyncEvent", "httpPoll", "aasPoll"), List.of(), List.of("capture"));
			route.setRouteId("mixedSources");
			RoutesConfiguration routes = new RoutesConfiguration(List.of(async, http, aas),
					List.of(), List.of(sink), List.of(route));
			List<Map<?, ?>> captured = new CopyOnWriteArrayList<>();

			try (DefaultCamelContext context = new DefaultCamelContext()) {
				context.addComponent("aas", new AASComponent());
				AASEndpoint aasEndpoint = context.getEndpoint(aas.getConnectionURI(), AASEndpoint.class);
				assertEquals(base + "/submodels/demo", aasEndpoint.getSubmodelEndpoint());
				assertEquals("Temperature", aasEndpoint.getPropertyPath());
				assertEquals(base + "/submodels/demo/submodel-elements/Temperature",
						aasEndpoint.getFullProxyUrl());
				context.addRoutes(new RouteBuilder() {
					@Override
					public void configure() {
						onException(IllegalArgumentException.class)
								.process(exchange -> sourceErrors.incrementAndGet()).handled(true);
						new ExecutionPolicyRouteCreator(this, routes).addRouteToRouteBuilder(route);
						from("direct:capture").process(exchange -> captured.add(new Gson().fromJson(
								exchange.getMessage().getBody(String.class), Map.class)));
					}
				});
				context.start();
				var producer = context.createProducerTemplate();
				try {
					producer.sendBody("seda:stationUpdates", "RUNNING");
				} finally {
					producer.stop();
				}
				await().atMost(Duration.ofSeconds(4)).until(
						() -> httpReads.get() > 0 && aasReads.get() > 1);
				assertTrue("a failed sync read must not emit a partial sources map", captured.isEmpty());
				assertEquals("failed reads must not disassemble the timer exchange", 0, sourceErrors.get());

				aasReady.set(true);
				await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
					assertTrue("waiting for all three sources", !captured.isEmpty());
					Map<?, ?> sources = captured.get(0);
					assertEquals(3, sources.size());
					assertEquals("RUNNING", sources.get("StationState"));
					assertEquals(7.0, sources.get("Pressure"));
					assertEquals(19.0, sources.get("Temperature"));
				});

				httpValue.set(9);
				await().atMost(Duration.ofSeconds(8)).until(() -> captured.stream().anyMatch(
						sources -> Double.valueOf(9).equals(sources.get("Pressure"))));
			}
		} finally {
			server.stop(0);
		}
	}
}
