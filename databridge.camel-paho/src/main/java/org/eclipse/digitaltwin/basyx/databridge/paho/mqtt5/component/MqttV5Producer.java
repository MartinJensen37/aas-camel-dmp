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
package org.eclipse.digitaltwin.basyx.databridge.paho.mqtt5.component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.digitaltwin.basyx.databridge.core.operation.OperationDelegationTimeoutException;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producer for {@code paho-mqtt5-inout:} endpoints.
 *
 * <p>
 * An InOut exchange IS the camel RequestReply EIP: the command is published
 * THROUGH the upstream {@code paho-mqtt5:} producer (derived from this
 * endpoint's shared transport options) with the MQTT 5 ResponseTopic +
 * CorrelationData properties riding the {@code CamelPahoMqtt5MsgProperties}
 * header, and the exchange completes with the first terminal reply. The reply
 * subscription is this producer's own upstream {@code paho-mqtt5:} CONSUMER
 * on the declared reply topic (registered lazily before the first request,
 * camel-jms ReplyManager style) whose {@link MqttV5ReplyMatcher} completes
 * {@link MqttV5ReplyRegistry} pending futures by the echoed CorrelationData,
 * skipping intermediate {@code State}: "RUNNING" acks. A one-way exchange
 * stays a plain upstream publish.
 *
 * <p>
 * The wire protocol — connections, reconnects, persistence — stays upstream's;
 * this component owns only the InOut semantics. The correlation data rides
 * the optional {@code correlationHeader} exchange header when the endpoint
 * declares it: a non-blank header value IS the correlation, otherwise a UUID
 * is minted and exposed on the header.
 *
 * @author aas-camel-dmp
 */
public class MqttV5Producer extends DefaultProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MqttV5Producer.class);

	private final MqttV5Endpoint endpoint;

	private volatile org.apache.camel.ProducerTemplate publisher;
	private final AtomicBoolean replyRouteRegistered = new AtomicBoolean(false);

	public MqttV5Producer(MqttV5Endpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		Object body = exchange.getMessage().getBody();
		byte[] payload = body == null ? new byte[0]
				: (body instanceof byte[] b ? b : exchange.getMessage().getBody(String.class)
						.getBytes(StandardCharsets.UTF_8));

		if (!org.apache.camel.support.ExchangeHelper.isOutCapable(exchange)) {
			// plain publish through the upstream transport (one-way /
			// data-sink exchanges)
			LOGGER.debug("paho-mqtt5-inout: one-way publish on {}", endpoint.getTopic());
			publisher().sendBodyAndHeader(upstreamPublishUri(), payload,
					org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.CAMEL_PAHO_MSG_QOS,
					endpoint.getQos());
			return;
		}

		String replyTopic = endpoint.getReplyTopic();
		if (replyTopic == null || replyTopic.isBlank()) {
			throw new IllegalStateException("paho-mqtt5-inout endpoint '" + endpoint.getEndpointUri()
					+ "' was invoked InOut without a replyTopic option");
		}
		ensureReplyRouteRegistered();

		String correlation = correlationOf(exchange);
		if (correlation == null || correlation.isBlank()) {
			correlation = UUID.randomUUID().toString();
		}
		MqttProperties properties = new MqttProperties();
		properties.setResponseTopic(replyTopic);
		properties.setCorrelationData(correlation.getBytes(StandardCharsets.UTF_8));

		var future = MqttV5ReplyRegistry.register(correlation);
		try {
			publisher().sendBodyAndHeader(upstreamPublishUri(), payload,
					org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES,
					properties);
			LOGGER.debug("paho-mqtt5-inout: published request on {} (replyTopic={}, correlation={})",
					endpoint.getTopic(), replyTopic, correlation);
			String reply = future.get(endpoint.getReplyTimeout() * 1_000L
					+ MqttV5ReplyRegistry.WAIT_MARGIN_MILLIS,
					java.util.concurrent.TimeUnit.MILLISECONDS);
			exchange.getMessage().setBody(reply);
		} catch (java.util.concurrent.TimeoutException e) {
			throw new OperationDelegationTimeoutException("operation request on '"
					+ endpoint.getTopic() + "' timed out after " + endpoint.getReplyTimeout()
					+ "s: no correlated reply on '" + replyTopic + "'");
		} catch (java.util.concurrent.ExecutionException e) {
			throw new IllegalStateException("correlated reply failed: " + e.getMessage(), e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted while awaiting the correlated reply", e);
		} finally {
			MqttV5ReplyRegistry.unregister(correlation, future);
		}
	}

	private String correlationOf(Exchange exchange) {
		String header = endpoint.getCorrelationHeader();
		if (header == null || header.isBlank()) {
			return null; // a fresh correlation per invocation
		}
		String value = exchange.getMessage().getHeader(header, String.class);
		if (value != null && !value.isBlank()) {
			return value;
		}
		String minted = UUID.randomUUID().toString();
		exchange.getMessage().setHeader(header, minted);
		return minted;
	}

	/**
	 * The upstream {@code paho-mqtt5:} CONSUMER URI for the declared reply
	 * topic — same broker and QoS as the publish leg, its own client id
	 * (Paho forbids two connections sharing one id). Reconnects and
	 * resubscriptions are the upstream client's job.
	 */
	private String upstreamReplyUri() {
		StringBuilder uri = new StringBuilder("paho-mqtt5:").append(endpoint.getReplyTopic());
		uri.append("?brokerUrl=").append(endpoint.getBrokerUrl());
		uri.append("&clientId=").append(endpoint.getClientId()).append("-reply");
		uri.append("&qos=").append(endpoint.getQos());
		if (endpoint.getUserName() != null && !endpoint.getUserName().isBlank()) {
			uri.append("&userName=").append(endpoint.getUserName());
		}
		if (endpoint.getPassword() != null) {
			uri.append("&password=").append(endpoint.getPassword());
		}
		return uri.toString();
	}

	/**
	 * The upstream {@code paho-mqtt5:} producer URI derived from this
	 * endpoint's shared transport options — same broker, client id, QoS and
	 * retain flag; the v5 properties ride the CamelPahoMqtt5MsgProperties
	 * header.
	 */
	private String upstreamPublishUri() {
		StringBuilder uri = new StringBuilder("paho-mqtt5:").append(endpoint.getTopic());
		uri.append("?brokerUrl=").append(endpoint.getBrokerUrl());
		uri.append("&clientId=").append(endpoint.getClientId());
		uri.append("&qos=").append(endpoint.getQos());
		uri.append("&retained=").append(endpoint.isRetained());
		if (endpoint.getUserName() != null && !endpoint.getUserName().isBlank()) {
			uri.append("&userName=").append(endpoint.getUserName());
		}
		if (endpoint.getPassword() != null) {
			uri.append("&password=").append(endpoint.getPassword());
		}
		return uri.toString();
	}

	/**
	 * Registers the reply-consumer route lazily (before the first InOut
	 * publish) so the subscription exists before the request rides the wire —
	 * an immediate reply cannot race the subscription. Route addition at
	 * runtime is the supported pattern (the route builder phase is long over).
	 */
	private void ensureReplyRouteRegistered() throws Exception {
		if (!replyRouteRegistered.compareAndSet(false, true)) {
			return;
		}
		String replyUri = upstreamReplyUri();
		String routeId = "paho-mqtt5-inout-reply-" + Integer.toHexString(
				endpoint.getEndpointKey().hashCode());
		endpoint.getCamelContext().addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override
			public void configure() {
				from(replyUri)
						.routeId(routeId)
						.process(new MqttV5ReplyMatcher(endpoint.getSkipReplyStates()));
			}
		});
		LOGGER.info("paho-mqtt5-inout: reply consumer registered {}", replyUri);
	}

	private org.apache.camel.ProducerTemplate publisher() {
		org.apache.camel.ProducerTemplate template = publisher;
		if (template == null) {
			synchronized (this) {
				if (template == null) {
					template = endpoint.getCamelContext().createProducerTemplate();
					publisher = template;
				}
			}
		}
		return template;
	}

	@Override
	protected void doStop() throws Exception {
		super.doStop();
		if (publisher != null) {
			publisher.stop();
			publisher = null;
		}
	}

}
