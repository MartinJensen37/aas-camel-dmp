/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.paho.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.camel.component.paho.mqtt5.PahoMqtt5Endpoint;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.security.BasicCredential;
import dk.aau.smartproductionlab.dmp.paho.mqtt5.component.MqttV5Endpoint;
import org.junit.jupiter.api.Test;

class MqttCredentialBindingTest {
	private static final String SECRET = "pa ss&word#1";

	@Test
	void sourceAndSinkUrisNameTheCredentialsWhileTheEndpointsReceiveThem() throws Exception {
		var source = new MqttDataSourceConfiguration("source", "broker", 1883, "dmp-source", "base/state",
				new BasicCredential("alice", SECRET));
		var sink = new MqttDataSinkConfiguration("sink", "broker", 1883, "base/command", "dmp-sink", "alice", SECRET);
		for (String uri : new String[] { source.getConnectionURI(), sink.getConnectionURI() }) {
			assertFalse(uri.contains(SECRET), uri);
			assertFalse(uri.contains("alice"), uri);
			assertTrue(uri.contains("password=#bean:dmp-credential-"), uri);
		}
		try (var context = new DefaultCamelContext()) {
			context.start();
			source.bind(context);
			sink.bind(context);
			for (String uri : new String[] { source.getConnectionURI(), sink.getConnectionURI() }) {
				var endpoint = context.getEndpoint(uri, PahoMqtt5Endpoint.class);
				assertEquals("alice", endpoint.getConfiguration().getUserName());
				assertEquals(SECRET, endpoint.getConfiguration().getPassword());
				assertFalse(endpoint.getEndpointUri().contains(SECRET), "the registered endpoint keeps the reference");
			}
			source.close();
			sink.close();
			assertTrue(context.getRegistry().findByTypeWithName(String.class).keySet().stream()
					.noneMatch(name -> name.startsWith("dmp-credential-")), "retiring the bundle unbinds the credentials");
		}
	}

	@Test
	void withoutCredentialsNothingIsBoundOrReferenced() throws Exception {
		var source = new MqttDataSourceConfiguration("source", "broker", 1883, "dmp-source", "base/state", null);
		assertFalse(source.getConnectionURI().contains("userName"));
		try (var context = new DefaultCamelContext()) {
			source.bind(context);
			assertTrue(context.getRegistry().findByTypeWithName(String.class).isEmpty());
		}
	}

	@Test
	void mqtt5EndpointsResolveBoundCredentialsByName() throws Exception {
		try (var context = new DefaultCamelContext()) {
			var credential = new dk.aau.smartproductionlab.dmp.core.security.BoundCredential("alice", SECRET);
			credential.bind(context);
			context.start();
			var endpoint = context.getEndpoint("paho-mqtt5-inout:command?brokerUrl=tcp://broker:1883&clientId=dmp-op"
					+ "&replyTopic=reply&credentials=" + credential.name(), MqttV5Endpoint.class);
			assertEquals("alice", endpoint.getUserName());
			assertEquals(SECRET, endpoint.getPassword());
			assertFalse(endpoint.getEndpointUri().contains(SECRET));
			credential.close();
			assertNull(context.getRegistry().lookupByName(credential.name() + ".password"));
		}
	}
}
