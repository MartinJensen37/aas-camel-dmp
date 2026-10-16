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
	void aSingleStringArgumentIsMappedByNameLikeAnyOtherArgument() throws Exception {
		var signature = new OpcUaMethodMetadataProvider.MethodSignature(List.of(
				new OpcUaMethodMetadataProvider.Argument("Command", "i=12", -1, List.of())), List.of());
		var adapter = new OpcUaSynchronousInvocationAdapter(
				new CamelOpcUaMethodMetadataProvider((context, uri) -> signature));
		AtomicReference<Object> wireRequest = new AtomicReference<>();
		try (var context = new DefaultCamelContext()) {
			context.addRoutes(new RouteBuilder() {
				@Override
				public void configure() {
					from("direct:method").process(exchange -> {
						wireRequest.set(exchange.getMessage().getBody());
						exchange.getMessage().setBody(Map.of("statusCode", Map.of("good", true),
								"outputArguments", List.of()));
					});
				}
			});
			context.start();
			var schema = new SynchronousInvocationAdapter.InvocationSchema(Map.of("Command", "string"),
					Set.of("Command"), Map.of(), Set.of(), Map.of());
			adapter.invoke(context, "direct:method", Map.of("Command", "start"), schema);
			var variants = (org.eclipse.milo.opcua.stack.core.types.builtin.Variant[]) wireRequest.get();
			assertEquals(1, variants.length);
			assertEquals("start", variants[0].getValue(), "the value itself is sent, not a JSON document");
			var mismatch = org.junit.jupiter.api.Assertions.assertThrows(ConnectorInvocationException.class,
					() -> adapter.invoke(context, "direct:method", Map.of("command", "start"), schema));
			assertEquals("METHOD_ARGUMENT_MISMATCH", mismatch.getConnectorCode());
		}
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
