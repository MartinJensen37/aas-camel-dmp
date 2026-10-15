package org.eclipse.digitaltwin.basyx.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MqttEndpointIdentityProviderTest {
	@Test
	void perSinkClientIdsShareDestinationOnlyWhenOtherOptionsMatch() {
		String first = "paho-mqtt5:device/command?brokerUrl=tcp://broker:1883&clientId=dmp-uuid&qos=1";
		String sibling = "paho-mqtt5:device/command?brokerUrl=tcp://broker:1883&clientId=dmp-mode&qos=1";
		MqttEndpointIdentityProvider provider = new MqttEndpointIdentityProvider();
		assertTrue(provider.identity(first).equals(provider.identity(sibling)));
		assertFalse(provider.identity(first).equals(provider.identity(
				sibling.replace("device/command", "device/other"))));
		assertFalse(provider.identity(first).equals(provider.identity(sibling.replace("qos=1", "qos=0"))));
	}

	@Test
	void queryOptionOrderDoesNotChangeDestinationIdentity() {
		String first = "paho-mqtt5:device/command?brokerUrl=tcp://broker:1883&clientId=one&qos=1";
		String second = "paho-mqtt5:device/command?qos=1&clientId=two&brokerUrl=tcp://broker:1883";
		MqttEndpointIdentityProvider provider = new MqttEndpointIdentityProvider();
		assertTrue(provider.identity(first).equals(provider.identity(second)));
	}
}
