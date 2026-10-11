package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

class OpcUaSynchronousInvocationAdapterTest {
	@Test
	void legacyJsonStringRequestPreservesRequiredNullAndExactNumbers() throws Exception {
		var signature = new OpcUaMethodMetadataProvider.MethodSignature(List.of(
				new OpcUaMethodMetadataProvider.Argument("Command", "i=12", -1, List.of())), List.of());
		var adapter = new OpcUaSynchronousInvocationAdapter(
				new CamelOpcUaMethodMetadataProvider((context, uri) -> signature));
		Map<String, Object> request = new LinkedHashMap<>();
		request.put("RequiredNullable", null);
		request.put("count", 9007199254740993L);
		request.put("fraction", new java.math.BigDecimal("1.25"));
		new AffordanceValidation(Map.of("type", "object", "required", List.of("RequiredNullable"),
				"properties", Map.of("RequiredNullable", Map.of("type", List.of("string", "null")))),
				"legacy command", AffordanceValidation.Mode.ON).outgoing(request);
		request.put("marker", JsonPointer.JSON_NULL);
		AtomicReference<String> wireRequest = new AtomicReference<>();
		try (var context = new DefaultCamelContext()) {
			context.addRoutes(new RouteBuilder() {
				@Override
				public void configure() {
					from("direct:legacyMethod").process(exchange -> {
						wireRequest.set(exchange.getMessage().getBody(String.class));
						exchange.getMessage().setBody(Map.of("statusCode", Map.of("good", true),
								"outputArguments", List.of()));
					});
				}
			});
			context.start();
			adapter.invoke(context, "direct:legacyMethod", request,
					new SynchronousInvocationAdapter.InvocationSchema(Map.of(), Set.of("RequiredNullable"),
							Map.of(), Set.of(), Map.of()));
		}
		Map<?, ?> decoded = (Map<?, ?>) JsonCodec.parseJson(wireRequest.get());
		assertTrue(decoded.containsKey("RequiredNullable"));
		assertNull(decoded.get("RequiredNullable"));
		assertTrue(decoded.containsKey("marker"));
		assertNull(decoded.get("marker"));
		assertEquals(9007199254740993L, decoded.get("count"));
		assertEquals(1.25, decoded.get("fraction"));
	}

	@Test
	void decodesPositionalCallResultUsingServerDeclaredNames() {
		var signature = new OpcUaMethodMetadataProvider.MethodSignature(List.of(), List.of(
				new OpcUaMethodMetadataProvider.Argument("Outcome", "i=1", -1, List.of()),
				new OpcUaMethodMetadataProvider.Argument("State", "i=12", -1, List.of())));
		Map<String, Object> result = OpcUaSynchronousInvocationAdapter.decode(
				new DefaultCamelContext(),
				Map.of("statusCode", Map.of("good", true),
						"outputArguments", List.of(true, "READY")), signature);
		assertEquals(true, result.get("Outcome"));
		assertEquals("READY", result.get("State"));
	}

	@Test
	void preservesUaFailureAsAConnectorNeutralRemoteOutcome() {
		var signature = new OpcUaMethodMetadataProvider.MethodSignature(List.of(), List.of());
		ConnectorInvocationException failure = assertThrows(ConnectorInvocationException.class,
				() -> OpcUaSynchronousInvocationAdapter.decode(new DefaultCamelContext(),
						Map.of("statusCode", Map.of("good", false, "value", "BadMethodInvalid")), signature));
		assertEquals(ConnectorInvocationException.Category.REMOTE, failure.getCategory());
		assertEquals("METHOD_STATUS_BAD", failure.getConnectorCode());
	}
}
