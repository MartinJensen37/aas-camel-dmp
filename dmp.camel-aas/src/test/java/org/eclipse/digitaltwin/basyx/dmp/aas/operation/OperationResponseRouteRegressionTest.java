package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.ResponseSinkFanOutProcessor;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

/** Exercises the production bridge, including its Split/Wire Tap scheduling boundary. */
class OperationResponseRouteRegressionTest extends CamelTestSupport {
	private final AtomicInteger invocations = new AtomicInteger();
	private final AtomicInteger responseTransforms = new AtomicInteger();
	private final AtomicReference<Object> normalizedReply = new AtomicReference<>();
	private final AtomicReference<Map<String, Object>> responseValues = new AtomicReference<>();
	private final ConcurrentLinkedQueue<Object> tapBodies = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<Object> tapHeaders = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<Object> tapProperties = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<Object> sinkBodies = new ConcurrentLinkedQueue<>();
	private final CountDownLatch writes = new CountDownLatch(2);
	private final CountDownLatch releaseWrites = new CountDownLatch(1);

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			@SuppressWarnings("unchecked")
			public void configure() throws Exception {
				int port;
				try (var probe = new java.net.ServerSocket(0)) {
					port = probe.getLocalPort();
				}
				restConfiguration().component("jetty").host("localhost").port(port).inlineRoutes(false);
				interceptFrom("direct:r0-whole-response-write").process(exchange -> {
					var job = exchange.getMessage().getBody(ResponseSinkFanOutProcessor.DispatchJob.class);
					tapBodies.add(JsonCodec.copyValue(job.body()));
					tapHeaders.add(JsonCodec.copyValue(exchange.getMessage().getHeader("payload")));
					tapProperties.add(JsonCodec.copyValue(exchange.getProperty("payload")));
					((List<Object>) ((Map<?, ?>) job.body()).get("values")).set(0, job.sinkLabel());
					((List<Object>) exchange.getMessage().getHeader("payload", Map.class).get("values")).clear();
					((List<Object>) exchange.getProperty("payload", Map.class).get("values")).clear();
					exchange.getProperty("dmp.operation.callerReplyResult", Map.class).clear();
				});
				var routes = new RoutesConfiguration(List.of(), List.of(
						new ProtocolBridgeRouteCreatorTest.FixedTransformer("request", "direct:r0-request"),
						new ProtocolBridgeRouteCreatorTest.FixedTransformer("projected", "direct:r0-projected"),
						new ProtocolBridgeRouteCreatorTest.FixedTransformer("whole", "direct:r0-whole-transform"),
						new ProtocolBridgeRouteCreatorTest.FixedTransformer("legacy", "direct:r0-legacy-transform")),
						List.of(new ProtocolBridgeRouteCreatorTest.FixedSink("command", "direct:r0-device"),
								new ProtocolBridgeRouteCreatorTest.FixedSink("extra", "direct:r0-extra")), List.of());
				for (String id : List.of("null", "missing", "duplicate", "whole", "legacy", "native")) {
					var bridge = new ProtocolBridgeRouteConfiguration(new RouteConfiguration(
							"bridge", List.of(), List.of("request"), List.of("command")));
					bridge.setRouteId("r0-" + id);
					var data = bridge.getTriggerData();
					data.put("restPath", "/r0/" + id);
					data.put("restMethod", "POST");
					data.put("action", "Invoke");
					data.put("operationSourceId", "Request");
					data.put("commandSinkId", "Command");
					data.put("callerReplySinkId", List.of("legacy", "native").contains(id) ? "Invoke" : "Reply");
					data.put("explicitResponseMapping", !List.of("legacy", "native").contains(id));
					if (!"native".equals(id)) data.put("responseTransformer",
							"whole".equals(id) || "legacy".equals(id) ? id : "projected");
					if (List.of("null", "missing", "duplicate").contains(id)) {
						var bindings = new ArrayList<Map<String, Object>>();
						bindings.add(Map.of("sourceId", "Optional", "valuePath",
								"missing".equals(id) ? "/missing" : "/optional"));
						if ("duplicate".equals(id)) bindings.add(Map.of("sourceId", "Optional", "valuePath", "/optional"));
						data.put("responseSourceBindings", bindings);
					}
					if ("whole".equals(id)) data.put("responseSinkBindings", List.of(
							Map.of("sinkId", "First", "dataSinkId", "extra"),
							Map.of("sinkId", "Second", "dataSinkId", "extra")));
					new ProtocolBridgeRouteCreator(this, routes).addRouteToRouteBuilder(bridge);
				}
				from("direct:r0-request").setBody().constant(Map.of("Command", Map.of("argument", 1L)));
				from("direct:r0-device").process(exchange -> {
					invocations.incrementAndGet();
					Map<String, Object> reply = new LinkedHashMap<>();
					reply.put("optional", null);
					reply.put("Invoke", Map.of("x", 1L));
					reply.put("sibling", 2L);
					exchange.getMessage().setBody(reply);
				});
				from("direct:r0-projected").process(exchange -> {
					responseTransforms.incrementAndGet();
					Map<String, Object> reply = new LinkedHashMap<>();
					reply.put("Reply", exchange.getMessage().getBody(Map.class).get("Optional"));
					exchange.getMessage().setBody(reply);
				});
				from("direct:r0-whole-transform").process(exchange -> {
					responseTransforms.incrementAndGet();
					normalizedReply.set(exchange.getMessage().getBody());
					Map<String, Object> shared = new LinkedHashMap<>(Map.of("values", new ArrayList<>(List.of("original"))));
					Map<String, Object> reply = new LinkedHashMap<>();
					reply.put("Reply", Map.of("Reply", Map.of("x", 1L), "sibling", 2L, "values", shared.get("values")));
					reply.put("First", shared);
					reply.put("Second", shared);
					responseValues.set(reply);
					exchange.getMessage().setBody(reply);
				});
				from("direct:r0-legacy-transform").setBody().constant(Map.of(
						"Invoke", Map.of("Invoke", Map.of("x", 1L), "sibling", 2L)));
				from("direct:r0-extra").process(exchange -> {
					sinkBodies.add(exchange.getMessage().getBody());
					writes.countDown();
					if (!releaseWrites.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test release timed out");
				});
			}
		};
	}

	private Exchange invoke(String route) {
		return template.request("direct:r0-" + route, exchange -> exchange.getMessage().setBody(Map.of("argument", 1L)));
	}

	@Test
	void validNullProjectionReachesCallerOnce() {
		Exchange result = invoke("null");
		assertEquals(200, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
		assertNull(JsonCodec.parseJson(result.getMessage().getBody(String.class)));
		assertEquals(1, invocations.get());
		assertEquals(1, responseTransforms.get());
	}

	@Test
	void missingPointerFailsMappingBeforeResponseLua() {
		assertMappingFailure(invoke("missing"));
	}

	@Test
	void duplicateNullSourceIdFailsMappingBeforeResponseLua() {
		assertMappingFailure(invoke("duplicate"));
	}

	private void assertMappingFailure(Exchange result) {
		assertEquals(500, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", ((Map<?, ?>) JsonCodec.parseJson(
				result.getMessage().getBody(String.class))).get("code"));
		assertEquals(1, invocations.get());
		assertEquals(0, responseTransforms.get());
	}

	@Test
	void nativeReplyWithAnActionNamedFieldReachesCallerWithoutProjection() {
		Exchange result = invoke("native");
		assertEquals(200, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
		Map<String, Object> expected = new LinkedHashMap<>();
		expected.put("optional", null);
		expected.put("Invoke", Map.of("x", 1L));
		expected.put("sibling", 2L);
		assertEquals(expected, JsonCodec.parseJson(result.getMessage().getBody(String.class)));
		assertEquals(1, invocations.get());
		assertEquals(0, responseTransforms.get());
	}

	@Test
	void legacyReplySelectionHappensOnlyOnceAcrossPreparationAndCompletion() {
		Exchange result = invoke("legacy");
		assertEquals(200, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
		assertEquals(Map.of("Invoke", Map.of("x", 1L), "sibling", 2L),
				JsonCodec.parseJson(result.getMessage().getBody(String.class)));
	}

	@Test
	void mutatingDetachedRoutesCannotChangeCallerSiblingHeadersOrProperties() throws Exception {
		Map<String, Object> tree = new LinkedHashMap<>(Map.of("values", new ArrayList<>(List.of("original"))));
		try {
			long started = System.nanoTime();
			Exchange result = template.request("direct:r0-whole", exchange -> {
				exchange.getMessage().setBody(Map.of("argument", 1L));
				exchange.getMessage().setHeader("payload", tree);
				exchange.setProperty("payload", tree);
			});
			assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2_000,
					"detached sinks must not delay the caller or run on its thread");
			assertEquals(200, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
			assertEquals(Map.of("Reply", Map.of("x", 1L), "sibling", 2L, "values", List.of("original")),
					JsonCodec.parseJson(result.getMessage().getBody(String.class)));
			assertTrue(writes.await(2, TimeUnit.SECONDS));
			assertEquals(List.of("original"), tree.get("values"));
			assertEquals(tree, result.getMessage().getHeader("payload"));
			assertEquals(tree, result.getProperty("payload"));
			assertEquals(2, tapBodies.size());
			for (Object snapshot : tapBodies) assertEquals(Map.of("values", List.of("original")), snapshot);
			for (Object snapshot : tapHeaders) assertEquals(tree, snapshot);
			for (Object snapshot : tapProperties) assertEquals(tree, snapshot);
			assertEquals(2, sinkBodies.size());
			assertEquals(3, responseValues.get().size());
			assertEquals(Map.of("values", List.of("original")), responseValues.get().get("First"));
			assertEquals(Map.of("x", 1L), ((Map<?, ?>) normalizedReply.get()).get("Invoke"));
			assertEquals(2L, ((Map<?, ?>) normalizedReply.get()).get("sibling"));
			assertEquals(1, invocations.get());
			assertEquals(1, responseTransforms.get());
		} finally {
			releaseWrites.countDown();
		}
	}
}
