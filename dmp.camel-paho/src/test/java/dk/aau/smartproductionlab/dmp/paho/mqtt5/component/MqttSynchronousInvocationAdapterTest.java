package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import java.util.Set;

import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.junit.jupiter.api.Test;

public class MqttSynchronousInvocationAdapterTest {
	@Test
	void inventoryIdentifiesDerivedRequestReplyEndpointWithoutPublishing() {
		var adapter = new MqttSynchronousInvocationAdapter();
		var schema = new SynchronousInvocationAdapter.InvocationSchema(Map.of(), Set.of(), Map.of(), Set.of(),
				Map.of("requestReply", true, "responseTopic", "reply/channel", "timeoutSeconds", 4));
		assertEquals(Set.of("paho-mqtt5-inout:command?brokerUrl=tcp://127.0.0.1:1883"
				+ "&replyTopic=reply/channel&replyTimeout=4"),
				adapter.ownedEndpointUris("paho-mqtt5:command?brokerUrl=tcp://127.0.0.1:1883", schema));
	}

	@Test
	void compiledRequestReplyEndpointIsInvokedAsIsAndItsJsonReplyDecoded() throws Exception {
		// The compiler's operation sinks are already paho-mqtt5-inout URIs; the reply arrives
		// as JSON text and must reach the operation as an object, or the caller gets no outputs.
		var adapter = new MqttSynchronousInvocationAdapter();
		String uri = "paho-mqtt5-inout:CMD/Halt?brokerUrl=tcp://127.0.0.1:1883&clientId=c&replyTopic=DATA/Halt&replyTimeout=30";
		var schema = new SynchronousInvocationAdapter.InvocationSchema(Map.of(), Set.of(), Map.of(), Set.of(), Map.of());
		assertEquals(true, adapter.supports(uri));
		assertEquals(Set.of(uri), adapter.ownedEndpointUris(uri, schema));
		var camel = new DefaultCamelContext();
		camel.addComponent("paho-mqtt5-inout", new org.apache.camel.support.DefaultComponent() {
			@Override
			protected org.apache.camel.Endpoint createEndpoint(String endpointUri, String remaining, Map<String, Object> parameters) {
				parameters.clear();
				return new org.apache.camel.support.DefaultEndpoint(endpointUri, this) {
					@Override
					public org.apache.camel.Producer createProducer() {
						return new org.apache.camel.support.DefaultProducer(this) {
							@Override
							public void process(org.apache.camel.Exchange exchange) {
								exchange.getMessage().setBody("{\"State\": \"SUCCESS\", \"Outcome\": 0, \"Uuid\": \"u-1\"}");
							}
						};
					}

					@Override
					public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) {
						throw new UnsupportedOperationException();
					}
				};
			}
		});
		try {
			Map<String, Object> reply = invoke(adapter, camel, uri, Map.of("Uuid", "u-1"), schema);
			assertEquals("SUCCESS", reply.get("State"));
			assertEquals("u-1", reply.get("Uuid"));
			assertEquals(0, ((Number) reply.get("Outcome")).intValue());
		} finally {
			camel.stop();
		}
	}

	@Test
	void requiresDeclaredRequestReplyBeforePublishing() {
		var adapter = new MqttSynchronousInvocationAdapter();
		ConnectorInvocationException failure = assertThrows(ConnectorInvocationException.class,
				() -> invoke(adapter, new DefaultCamelContext(), "paho-mqtt5:command", Map.of("value", 1),
					new SynchronousInvocationAdapter.InvocationSchema(
							Map.of(), Set.of(), Map.of(), Set.of(), Map.of())));
		assertEquals(ConnectorInvocationException.Category.CONFIGURATION, failure.getCategory());
		assertEquals("MQTT_REQUEST_REPLY_NOT_DECLARED", failure.getConnectorCode());
		assertThrows(ConnectorInvocationException.class, () -> adapter.ownedEndpointUris("paho-mqtt5:command",
				new SynchronousInvocationAdapter.InvocationSchema(Map.of(), Set.of(), Map.of(), Set.of(), Map.of())));
	}

	@Test
	void retainsTypedTimeoutWrappedByCamel() {
		var timeout = new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
				"MQTT_REQUEST_TIMEOUT", "timed out", Map.of());
		var wrapped = new org.apache.camel.CamelExecutionException("invoke", null, timeout);
		assertSame(timeout, MqttSynchronousInvocationAdapter.connectorFailure(wrapped));
		assertSame(timeout, MqttSynchronousInvocationAdapter.translateFailure(wrapped));
	}

	@Test
	void translatesCamelWrappedTimeoutToTypedTimeoutOutcome() {
		var wrapped = new org.apache.camel.CamelExecutionException("invoke", null,
				new java.util.concurrent.TimeoutException("correlated reply timed out"));
		var failure = MqttSynchronousInvocationAdapter.translateFailure(wrapped);
		assertEquals(ConnectorInvocationException.Category.TIMEOUT, failure.getCategory());
		assertEquals("MQTT_REQUEST_TIMEOUT", failure.getConnectorCode());
	}

	@Test
	void untypedTransportFailureDoesNotCopyCredentialBearingMessagesIntoDetails() {
		String secret = "paho-mqtt5:command?password=not-public&userName=private";
		var failure = MqttSynchronousInvocationAdapter.translateFailure(new IllegalStateException(secret));
		assertEquals("MQTT_REQUEST_FAILED", failure.getConnectorCode());
		assertEquals(Map.of(), failure.getDetails());
	}


	/** Runs the adapter's invocation steps once, as the bridge's command route does. */
	static Map<String, Object> invoke(SynchronousInvocationAdapter adapter, org.apache.camel.CamelContext camel,
			String uri, Map<String, Object> request, SynchronousInvocationAdapter.InvocationSchema schema) throws Exception {
		String route = "direct:invoke-" + java.util.UUID.randomUUID();
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { adapter.configureInvoke(from(route), uri, schema); }
		});
		if (!camel.isStarted()) camel.start();
		try {
			@SuppressWarnings("unchecked")
			Map<String, Object> reply = camel.createProducerTemplate().requestBody(route, request, Map.class);
			return reply;
		} catch (org.apache.camel.CamelExecutionException failure) {
			if (failure.getCause() instanceof RuntimeException cause) throw cause;
			throw failure;
		}
	}
}
