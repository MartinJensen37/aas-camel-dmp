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
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import org.apache.camel.CamelContext;
import org.apache.camel.spi.annotations.Component;
import org.apache.camel.support.DefaultComponent;

import java.util.Map;

/**
 * The {@code paho-mqtt5-inout:} Camel component — MQTT 5 request/reply
 * (InOut) semantics on top of the upstream {@code camel-paho-mqtt5}
 * transport.
 *
 * <p>
 * Upstream {@code camel-paho-mqtt5} is plain pub/sub: its producer is
 * fire-and-forget (no reply topic, no correlation, no timeout) — an InOut
 * exchange completes immediately with a null reply. This component closes
 * that gap for the DMP's delegated operations (ADR-022): when the exchange
 * pattern is InOut the producer mints a correlation, publishes the command
 * THROUGH the upstream {@code paho-mqtt5:} producer (the v5 ResponseTopic +
 * CorrelationData PUBLISH properties ride the {@code CamelPahoMqtt5MsgProperties}
 * header) and awaits the first terminal reply on the declared reply topic —
 * delivered by the {@code paho-mqtt5:} consumer route the bridge creator
 * emits per interaction, whose {@link MqttV5ReplyMatcher} completes the
 * pending future with the first reply carrying the request's correlation
 * data. The wire protocol — connections, reconnects,
 * subscriptions, persistence — stays upstream's; this component owns only
 * the InOut semantics.
 *
 * <p>
 * URI syntax: {@code paho-mqtt5-inout:<publishTopic>?brokerUrl=tcp://h:1883
 * &clientId=<id>&qos=2&retained=false&replyTopic=<reply>&replyTimeout=30}
 *
 * @author aas-camel-dmp
 */
@Component("paho-mqtt5-inout")
public class MqttV5Component extends DefaultComponent {

	public MqttV5Component() {
	}

	public MqttV5Component(CamelContext context) {
		super(context);
	}

	@Override
	protected MqttV5Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		MqttV5Endpoint endpoint = new MqttV5Endpoint(uri, this);
		endpoint.setTopic(remaining);
		setProperties(endpoint, parameters);
		if (endpoint.getClientId() == null || endpoint.getClientId().isBlank()) {
			throw new IllegalArgumentException("paho-mqtt5-inout endpoint '" + uri + "' requires a clientId");
		}
		if (endpoint.getBrokerUrl() == null || endpoint.getBrokerUrl().isBlank()) {
			endpoint.setBrokerUrl("tcp://localhost:1883");
		}
		return endpoint;
	}
}