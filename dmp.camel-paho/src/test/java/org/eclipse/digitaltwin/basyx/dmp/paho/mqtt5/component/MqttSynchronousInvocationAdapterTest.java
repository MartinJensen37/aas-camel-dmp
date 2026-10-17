package org.eclipse.digitaltwin.basyx.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.Map;
import java.util.Set;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.junit.jupiter.api.Test;

public class MqttSynchronousInvocationAdapterTest {
	@Test
	void inventoryIdentifiesDerivedRequestReplyEndpointWithoutPublishing() {
		var adapter = new MqttSynchronousInvocationAdapter();
		var schema = new SynchronousInvocationAdapter.InvocationSchema(Map.of(), Set.of(), Map.of(), Set.of(),
				Map.of("requestReply", true, "responseTopic", "reply/channel", "timeoutSeconds", 4));
		assertEquals(Set.of("paho-mqtt5-inout:command?brokerUrl=tcp://127.0.0.1:1883"
				+ "&replyTopic=reply/channel&replyTimeout=4&skipReplyStates=RUNNING"),
				adapter.ownedEndpointUris("paho-mqtt5:command?brokerUrl=tcp://127.0.0.1:1883", schema));
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
