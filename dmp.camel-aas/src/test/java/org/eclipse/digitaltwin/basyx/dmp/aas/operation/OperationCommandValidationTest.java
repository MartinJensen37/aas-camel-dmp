package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadAssembler;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.PayloadValidationException;
import org.junit.jupiter.api.Test;

class OperationCommandValidationTest {
	@Test
	void commandPayloadValidationIsProtocolNeutral() {
		var schema = Map.of("type", "object", "required", List.of("Uuid", "Mode"),
				"properties", Map.of("Uuid", Map.of("type", "string"),
						"Mode", Map.of("type", "string")));
		var guard = new AffordanceValidation(schema, "declared action input", AffordanceValidation.Mode.ON);
		assertThrows(PayloadValidationException.class, () -> guard.outgoing(Map.of("Uuid", "a")));
		guard.outgoing(Map.of("Uuid", "a", "Mode", "RUN"));
	}

	@Test
	void memberContributionsAssembleOneNamedDevicePayload() {
		Object payload = PayloadAssembler.assemble(Map.of("First", "unit-7", "Second", 2),
				List.of("First", "Second"), Map.of("First", "$.input.Uuid", "Second", "$.input.Speed"));
		assertEquals(Map.of("input", Map.of("Uuid", "unit-7", "Speed", 2)), payload);
	}

	@Test
	void actionAssemblyRejectsMissingAuthoredSinkIds() {
		assertThrows(IllegalArgumentException.class, () -> OperationPayloads.assembleCommand(
				Map.of("Other", Map.of("x", 1)), List.of("Expected"),
				Map.of("Expected", "$.X"), "Action"));
	}

	@Test
	void actionAssemblyUsesExactlyTheAuthoredSinkValue() {
		assertEquals(Map.of("x", 1), OperationPayloads.assembleCommand(
				Map.of("Expected", Map.of("x", 1), "Other", Map.of("wrong", true)),
				List.of("Expected"), Map.of("Expected", "$"), "Action"));
	}

	@Test
	void requestValueIsKeyedByTheAuthoredOperationSourceId() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody("{\"Position\":{\"x\":1}}");
		new OperationRequestProcessor("Invoke").process(exchange);
		Map<?, ?> actual = exchange.getMessage().getBody(Map.class);
		assertEquals(Map.of("Position", Map.of("x", 1L)), actual.get("Invoke"));
	}

	@Test
	void operationRequestProcessorMarksBaSyxArrayBeforeFlattening() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody("[{\"value\":{\"idShort\":\"Uuid\",\"value\":\"abc\"}}]");
		new OperationRequestProcessor("Invoke").process(exchange);
		assertEquals(true, exchange.getProperty(OperationPayloads.BASYX_DELEGATION_BODY));
	}

	@Test
	void actionRunsRequestAndIndependentPerSinkTransformerChainsBeforeOneInvocation() throws Exception {
		DefaultCamelContext camel = new DefaultCamelContext();
		AtomicReference<Object> command = new AtomicReference<>();
		AtomicInteger requestTransformations = new AtomicInteger();
		AtomicInteger invocations = new AtomicInteger();
		camel.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:request-one").process(exchange -> requestTransformations.incrementAndGet()).setBody()
						.constant(Map.of("First", Map.of("v", 1), "Second", Map.of("v", 2)));
				from("direct:request-two").process(exchange -> requestTransformations.incrementAndGet());
				from("direct:first-transformer").setBody().constant(
						Map.of("First", Map.of("x", 10), "Second", Map.of("v", 2)));
				from("direct:second-transformer").setBody().constant(
						Map.of("First", Map.of("v", 1), "Second", Map.of("y", 20)));
				from("direct:device").process(exchange -> {
					invocations.incrementAndGet();
					command.set(exchange.getMessage().getBody());
					exchange.getMessage().setBody(Map.of("Done", true));
				});
			}
		});
		camel.start();
		try {
			OperationContext context = new OperationContext("Move", "direct:request-one", null,
					"direct:device", Map.of(), Map.of("Done", "boolean"), List.of("Done"), "Invoke", "Move", "Invoke",
					List.of("First", "Second"), Map.of("First", "$.X", "Second", "$.Y"),
					false, List.of(), List.of(), Map.of(),
					List.of("direct:request-one", "direct:request-two"),
					Map.of("First", List.of("direct:first-transformer"),
							"Second", List.of("direct:second-transformer")));
			var exchange = new DefaultExchange(camel);
			exchange.getMessage().setBody("[{\"value\":{\"idShort\":\"Invoke\",\"value\":{}}}]");
			new SyncOperationProcessor(camel, context,
					new AffordanceValidation(null, "command"),
					new AffordanceValidation(null, "reply"), List.of(), List.of()).process(exchange);

			assertEquals(Map.of("X", Map.of("x", 10), "Y", Map.of("y", 20)), command.get());
			assertEquals(2, requestTransformations.get(), "every request transformer must run in order");
			assertEquals(1, invocations.get(), "grouped member sinks must make one device invocation");
			assertEquals(true, exchange.getMessage().getBody() instanceof List<?>,
					"BaSyx OperationVariable[] callers must receive the declared array envelope");
		} finally {
			camel.stop();
		}
	}
}
