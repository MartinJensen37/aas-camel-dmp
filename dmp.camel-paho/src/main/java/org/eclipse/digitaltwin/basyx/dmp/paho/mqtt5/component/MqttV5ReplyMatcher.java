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
import java.util.ArrayList;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The terminal-reply matcher of the {@code paho-mqtt5:} reply-consumer route
 * the bridge creator emits per DECLARED request/reply interaction: the
 * upstream consumer delivers the reply with its v5 properties on the
 * {@code CamelPahoMqtt5MsgProperties} header — this matcher completes the
 * pending future whose correlation matches, skipping intermediate replies
 * (the PackML {@code State: RUNNING} progress ack never completes an
 * exchange, like camel-jms skips intermediate progress replies).
 *
 * <p>
 * The {@code skipReplyStates} option comes from the interaction's InOut
 * endpoint options (the bridge creator passes them through).
 *
 * @author aas-camel-dmp
 */
public class MqttV5ReplyMatcher implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(MqttV5ReplyMatcher.class);
	private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER =
			new com.fasterxml.jackson.databind.ObjectMapper();

	private final List<String> skipReplyStates;
	private final MqttV5ReplyRegistry registry;
	private final org.apache.camel.CamelContext context;
	private final String broker;
	private final String replyTopic;

	MqttV5ReplyMatcher(MqttV5ReplyRegistry registry, org.apache.camel.CamelContext context,
			String broker, String replyTopic, String skipReplyStates) {
		this.registry = registry;
		this.context = context;
		this.broker = broker;
		this.replyTopic = replyTopic;
		this.skipReplyStates = parseStates(skipReplyStates);
	}

	@Override
	public void process(Exchange exchange) {
		if (exchange.getContext() != context || !replyTopic.equals(exchange.getMessage().getHeader(
				org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.MQTT_TOPIC, String.class))) {
			return;
		}
		String payload = exchange.getMessage().getBody(String.class);
		if (isIntermediate(payload, skipReplyStates)) {
			// the PackML intermediate progress ack never completes an exchange
			LOGGER.debug("paho-mqtt5-inout: intermediate reply on {} — exchange not completed",
					exchange.getMessage().getHeader(org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.MQTT_TOPIC));
			return;
		}
		String correlation = correlationOf(exchange);
		if (correlation == null) {
			LOGGER.debug("paho-mqtt5-inout: reply without correlation data — ignored");
			return;
		}
		if (!registry.complete(broker, replyTopic, correlation, payload)) {
			LOGGER.debug("paho-mqtt5-inout: reply with unknown correlation {} — ignored", correlation);
			return;
		}
		LOGGER.debug("paho-mqtt5-inout: terminal reply for correlation {} ({} bytes)",
				correlation, payload == null ? 0 : payload.length());
	}

	private static String correlationOf(Exchange exchange) {
		Object header = exchange.getMessage().getHeader(
				org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES);
		if (!(header instanceof MqttProperties properties)
				|| properties.getCorrelationData() == null) {
			return null;
		}
		return new String(properties.getCorrelationData(), StandardCharsets.UTF_8);
	}

	/**
	 * A reply whose top-level {@code State} field carries an intermediate
	 * state does not complete an exchange. A non-JSON payload can never be
	 * an intermediate progress ack. (Static core, unit-testable.)
	 */
	static boolean isIntermediate(String payload, List<String> skipStates) {
		if (skipStates == null || skipStates.isEmpty()) {
			return false;
		}
		if (payload == null || payload.isBlank() || payload.charAt(0) != '{') {
			return false;
		}
		com.fasterxml.jackson.databind.JsonNode node;
		try {
			node = JSON_MAPPER.readTree(payload);
		} catch (Exception e) {
			return false;
		}
		com.fasterxml.jackson.databind.JsonNode state = node.get("State");
		if (state == null || !state.isTextual()) {
			return false;
		}
		return skipStates.contains(state.asText().toUpperCase());
	}

	static List<String> parseStates(String skipReplyStates) {
		List<String> states = new ArrayList<>();
		if (skipReplyStates != null) {
			for (String s : skipReplyStates.split(",")) {
				String trimmed = s.trim();
				if (!trimmed.isEmpty()) {
					states.add(trimmed.toUpperCase());
				}
			}
		}
		return states;
	}
}
