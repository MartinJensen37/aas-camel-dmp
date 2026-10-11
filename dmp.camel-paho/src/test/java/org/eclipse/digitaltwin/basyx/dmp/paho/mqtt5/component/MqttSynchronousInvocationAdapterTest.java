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
	void requiresDeclaredRequestReplyBeforePublishing() {
		var adapter = new MqttSynchronousInvocationAdapter();
		ConnectorInvocationException failure = assertThrows(ConnectorInvocationException.class,
				() -> adapter.invoke(new DefaultCamelContext(), "paho:command", Map.of("value", 1),
					new SynchronousInvocationAdapter.InvocationSchema(
							Map.of(), Set.of(), Map.of(), Set.of(), Map.of())));
		assertEquals(ConnectorInvocationException.Category.CONFIGURATION, failure.getCategory());
		assertEquals("MQTT_REQUEST_REPLY_NOT_DECLARED", failure.getConnectorCode());
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

}
