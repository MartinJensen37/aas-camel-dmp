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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.paho.mqtt5.component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Focused tests for the {@code paho-mqtt5-inout:} component: URI parsing,
 * InOut preconditions and the reply matcher (correlation completion + the
 * intermediate RUNNING-ack filter).
 *
 * <p>
 * The transport (publish, reply subscription, reconnects) rides the upstream
 * {@code camel-paho-mqtt5} component; the full correlation round trip is
 * exercised live against hivemq by the DMP's stoppering e2e.
 */
class MqttV5EndpointTest extends CamelTestSupport {

	private static final org.apache.camel.CamelContext PROBE_CONTEXT = new DefaultCamelContext();

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				// no routes — endpoint-level tests only
			}
		};
	}

	@Test
	void endpointParsesInOutOptions() throws Exception {
		MqttV5Endpoint endpoint = context().getEndpoint(
				"paho-mqtt5-inout:NN/x/CMD/Halt?brokerUrl=tcp://127.0.0.1:11883&clientId=test-halt"
						+ "&qos=2&replyTopic=NN/x/DATA/Halt&replyTimeout=7&skipReplyStates=RUNNING",
				MqttV5Endpoint.class);
		assertEquals("NN/x/CMD/Halt", endpoint.getTopic());
		assertEquals("tcp://127.0.0.1:11883", endpoint.getBrokerUrl());
		assertEquals("test-halt", endpoint.getClientId());
		assertEquals(2, endpoint.getQos());
		assertEquals("NN/x/DATA/Halt", endpoint.getReplyTopic());
		assertEquals(7, endpoint.getReplyTimeout());
		assertEquals("RUNNING", endpoint.getSkipReplyStates());
	}

	@Test
	void inOutWithoutReplyTopicIsRejected() throws Exception {
		MqttV5Endpoint endpoint = context().getEndpoint(
				"paho-mqtt5-inout:NN/x/CMD/Halt?brokerUrl=tcp://127.0.0.1:11883&clientId=test-no-reply",
				MqttV5Endpoint.class);
		MqttV5Producer producer = new MqttV5Producer(endpoint);
		Exchange exchange = new DefaultExchange(context(), ExchangePattern.InOut);
		exchange.getIn().setBody("{}");
		IllegalStateException e = assertThrows(IllegalStateException.class,
				() -> producer.process(exchange));
		assertTrue(e.getMessage().contains("replyTopic"));
	}

	@Test
	void intermediateStateFilterDistinguishesReplies() {
		assertTrue(intermediate("{\"State\": \"RUNNING\"}"));
		assertTrue(intermediate("{\"State\": \"running\"}"));
		// terminal replies — never filtered
		assertFalse(intermediate("{\"State\": \"SUCCESS\", \"Outcome\": 0}"));
		assertFalse(intermediate("{\"State\": \"FAILURE\"}"));
		// no State field / non-JSON payloads ride through untouched
		assertFalse(intermediate("{\"Uuid\": \"x\"}"));
		assertFalse(intermediate("[1, 2]"));
		assertFalse(intermediate("not json"));
		assertFalse(intermediate(""));
		// a matcher without the filter treats every reply as terminal
		assertFalse(MqttV5ReplyMatcher.isIntermediate("{\"State\": \"RUNNING\"}", List.of()));
	}

	private static boolean intermediate(String payload) {
		return MqttV5ReplyMatcher.isIntermediate(payload, List.of("RUNNING"));
	}

	@Test
	void replyMatcherCompletesPendingByCorrelation() throws Exception {
		MqttV5ReplyMatcher matcher = new MqttV5ReplyMatcher("RUNNING");

		// a RUNNING progress ack never completes the pending future
		CompletableFuture<String> pending = MqttV5ReplyRegistry.register("corr-match");
		matcher.process(reply("{\"State\": \"RUNNING\", \"Uuid\": \"x\"}", "corr-match"));
		assertFalse(pending.isDone(), "the RUNNING ack must not complete the exchange");

		// the terminal reply with the echoed correlation completes it
		matcher.process(reply("{\"State\": \"SUCCESS\", \"Outcome\": 0}", "corr-match"));
		assertEquals("{\"State\": \"SUCCESS\", \"Outcome\": 0}", pending.getNow(null));

		// an unknown correlation is ignored
		CompletableFuture<String> pending2 = MqttV5ReplyRegistry.register("corr-other");
		matcher.process(reply("{\"State\": \"SUCCESS\"}", "corr-foreign"));
		assertFalse(pending2.isDone());

		// a reply without correlation data is ignored
		Exchange noProps = new DefaultExchange(PROBE_CONTEXT);
		noProps.getMessage().setBody("{}");
		matcher.process(noProps);
	}

	private static Exchange reply(String payload, String correlation) {
		Exchange exchange = new DefaultExchange(PROBE_CONTEXT);
		exchange.getMessage().setBody(payload);
		MqttProperties properties = new MqttProperties();
		properties.setCorrelationData(correlation.getBytes(StandardCharsets.UTF_8));
		exchange.getMessage().setHeader(
				org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES,
				properties);
		return exchange;
	}
}