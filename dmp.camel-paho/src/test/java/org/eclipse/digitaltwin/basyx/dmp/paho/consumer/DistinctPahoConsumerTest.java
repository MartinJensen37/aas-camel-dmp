/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.paho.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.FailedToStartRouteException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.MqttDataSourceConfiguration;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;

/**
 * W4.7 distinct-consumer regression (plan §4 W4.7): two routes consuming an
 * IDENTICAL paho topic on the same broker must start cleanly and each receive
 * their own deliveries. This guards the URI-collision class fixed by the
 * descriptor's per-source {@code clientId} emission (W3.4) — a consumer URI
 * without a distinguishing {@code clientId} compiles byte-identical for two
 * mappings subscribing to the same topic, and Camel refuses a second consumer
 * on the resulting non-shared singleton endpoint, failing the whole container
 * at start.
 */
class DistinctPahoConsumerTest extends CamelTestSupport {

	private static final String TOPIC = "shared/topic";

	private static Server broker;
	private static int brokerPort;

	@EndpointInject("mock:consumer1")
	private MockEndpoint consumer1;

	@EndpointInject("mock:consumer2")
	private MockEndpoint consumer2;

	@BeforeAll
	static void startBroker() throws Exception {
		brokerPort = freePort();
		broker = new Server();
		Properties properties = new Properties();
		properties.setProperty(io.moquette.BrokerConstants.PORT_PROPERTY_NAME, String.valueOf(brokerPort));
		properties.setProperty(io.moquette.BrokerConstants.HOST_PROPERTY_NAME, "127.0.0.1");
		properties.setProperty(io.moquette.BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "true");
		properties.setProperty(io.moquette.BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, "disabled");
		broker.startServer(new MemoryConfig(properties));
	}

	@AfterAll
	static void stopBroker() {
		broker.stopServer();
	}

	private static int freePort() throws Exception {
		try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	private static String consumerUri(String clientId) {
		return "paho:" + TOPIC + "?brokerUrl=tcp://127.0.0.1:" + brokerPort
				+ (clientId == null ? "" : "&clientId=" + clientId);
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				from(consumerUri("dmp-test-consumer1")).routeId("consumer1").to("mock:consumer1");
				from(consumerUri("dmp-test-consumer2")).routeId("consumer2").to("mock:consumer2");
			}
		};
	}

	/**
	 * The precondition of the collision: a data-source entry without a
	 * {@code clientId} compiles to a URI that carries no distinguishing
	 * parameter, so two mappings on the same topic/broker become
	 * byte-identical. The descriptor (W3.4) must therefore always emit one.
	 */
	@Test
	void missingClientIdCompilesToUndiscriminatedUri() {
		MqttDataSourceConfiguration first = new MqttDataSourceConfiguration("src1", "127.0.0.1", brokerPort, null, TOPIC, null);
		MqttDataSourceConfiguration second = new MqttDataSourceConfiguration("src2", "127.0.0.1", brokerPort, "", TOPIC, null);
		assertEquals(first.getConnectionURI(), second.getConnectionURI());
	}

	/**
	 * The guard itself: distinct clientIds on the same topic/broker yield
	 * distinct consumer URIs, so the mappings are independent Camel endpoints.
	 */
	@Test
	void distinctClientIdsYieldDistinctConsumerUris() {
		MqttDataSourceConfiguration first = new MqttDataSourceConfiguration("src1", "127.0.0.1", brokerPort,
				"dmp-first_mapping_source1", TOPIC, null);
		MqttDataSourceConfiguration second = new MqttDataSourceConfiguration("src2", "127.0.0.1", brokerPort,
				"dmp-first_mapping_source2", TOPIC, null);
		assertNotEquals(first.getConnectionURI(), second.getConnectionURI());
	}

	/**
	 * Full runtime regression: both consumers start in one context (no
	 * multiple-consumer failure) and EACH receives its own copy of a
	 * publication on the shared topic.
	 */
	@Test
	void distinctConsumersOnIdenticalTopicStartCleanlyAndEachReceiveTheirOwnDelivery() throws Exception {
		consumer1.expectedMessageCount(1);
		consumer2.expectedMessageCount(1);

		publishMqttMessage(TOPIC, "hello-both-consumers");

		MockEndpoint.assertIsSatisfied(10, TimeUnit.SECONDS, consumer1, consumer2);
		assertEquals("hello-both-consumers", consumer1.getReceivedExchanges().get(0).getMessage().getBody(String.class));
		assertEquals("hello-both-consumers", consumer2.getReceivedExchanges().get(0).getMessage().getBody(String.class));
	}

	/**
	 * Documents the failure class being guarded: two consumers registered on
	 * the SAME URI fail the context at start.
	 */
	@Test
	void identicalConsumerUriCannotHostTwoConsumers() throws Exception {
		String sharedUri = consumerUri("dmp-collision");
		try (CamelContext context = new DefaultCamelContext()) {
			context.addRoutes(new RouteBuilder() {
				@Override
				public void configure() {
					from(sharedUri).routeId("first").to("mock:first");
					from(sharedUri).routeId("second").to("mock:second");
				}
			});
			assertThrows(FailedToStartRouteException.class, context::start);
		}
	}

	private static void publishMqttMessage(String topic, String payload) throws Exception {
		MqttClient client = new MqttClient("tcp://127.0.0.1:" + brokerPort,
				"dmp-test-publisher-" + System.nanoTime(), new MemoryPersistence());
		client.connect();
		client.publish(topic, new MqttMessage(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
		client.disconnect();
		client.close();
	}
}
