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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
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
 * Wire encoding stays upstream's; this component owns finite client waits and
 * request/reply lifetime. Reply connections can reconnect, but command connections
 * never automatically reconnect or replay outstanding QoS publications. The correlation data rides
 * the optional {@code correlationHeader} exchange header when the endpoint
 * declares it: a non-blank header value IS the correlation, otherwise a UUID
 * is minted and exposed on the header.
 *
 * @author aas-camel-dmp
 */
public class MqttV5Producer extends DefaultProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(MqttV5Producer.class);

	private final MqttV5Endpoint endpoint;

	private boolean stopping;
	private volatile boolean cleanupPending;
	private long generation;

	public MqttV5Producer(MqttV5Endpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		long deadline = endpoint.invocationDeadline();
		long invocationGeneration;
		synchronized (this) {
			assertAdmitted(generation);
			invocationGeneration = generation;
		}
		Object body = exchange.getMessage().getBody();
		byte[] payload = body == null ? new byte[0]
				: (body instanceof byte[] b ? b : (body == JsonPointer.JSON_NULL
						|| body instanceof Map<?, ?> || body instanceof List<?>
						|| body instanceof Number || body instanceof Boolean
						? JsonCodec.toJson(body) : exchange.getMessage().getBody(String.class))
						.getBytes(StandardCharsets.UTF_8));

		if (!org.apache.camel.support.ExchangeHelper.isOutCapable(exchange)) {
			// plain publish through the upstream transport (one-way /
			// data-sink exchanges)
			LOGGER.debug("paho-mqtt5-inout: one-way publish on {}", endpoint.getTopic());
			lockTransport(deadline);
			try {
				synchronized (this) { assertAdmitted(invocationGeneration); }
				publish(payload, org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.CAMEL_PAHO_MSG_QOS,
						endpoint.getQos(), deadline);
			} finally {
				finishTransport(invocationGeneration);
			}
			return;
		}

		String replyTopic = endpoint.getReplyTopic();
		if (replyTopic == null || replyTopic.isBlank()) {
			throw new IllegalStateException("MQTT InOut invocation requires a replyTopic option");
		}
		String correlation = correlationOf(exchange);
		if (correlation == null || correlation.isBlank()) {
			correlation = UUID.randomUUID().toString();
		}
		MqttProperties properties = new MqttProperties();
		properties.setResponseTopic(replyTopic);
		properties.setCorrelationData(correlation.getBytes(StandardCharsets.UTF_8));

		CompletableFuture<String> future = null;
		try {
			lockTransport(deadline);
			try {
				synchronized (this) { assertAdmitted(invocationGeneration); }
				endpoint.acquireReplySubscription(this, upstreamReplyUri(), deadline);
				synchronized (this) {
					assertAdmitted(invocationGeneration);
					future = endpoint.replies.register(this, endpoint.getBrokerUrl(), replyTopic, correlation);
				}
				// Publication is admitted once. Stop can cancel its reply but cannot replay it.
				publish(payload, org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES,
						properties, deadline);
			} finally {
				finishTransport(invocationGeneration);
			}
			LOGGER.debug("paho-mqtt5-inout: published request on {} (replyTopic={}, correlation={})",
					endpoint.getTopic(), replyTopic, correlation);
			String reply = future.get(MqttV5Endpoint.remainingMillis(deadline),
					java.util.concurrent.TimeUnit.MILLISECONDS);
			exchange.getMessage().setBody(reply);
		} catch (java.util.concurrent.TimeoutException e) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"MQTT_REQUEST_TIMEOUT", "Timed out awaiting correlated MQTT reply",
					java.util.Map.of("topic", endpoint.getTopic(), "responseTopic", replyTopic,
							"timeoutSeconds", endpoint.getReplyTimeout()));
		} catch (java.util.concurrent.ExecutionException e) {
			throw new IllegalStateException("MQTT correlated reply failed", e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted while awaiting the correlated reply", e);
		} catch (Exception e) {
			for (Throwable cause = e; cause != null; cause = cause.getCause()) {
				if (cause instanceof org.eclipse.paho.mqttv5.common.MqttException mqtt
						&& mqtt.getReasonCode() == org.eclipse.paho.mqttv5.client.MqttClientException.REASON_CODE_CLIENT_TIMEOUT) {
					throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
							"MQTT_REQUEST_TIMEOUT", "MQTT transport deadline expired", Map.of());
				}
			}
			throw e;
		} finally {
			if (future != null) {
				endpoint.replies.unregister(this, endpoint.getBrokerUrl(), replyTopic, correlation, future);
			}
		}
	}

	private void finishTransport(long invocationGeneration) throws Exception {
		try {
			boolean retiring;
			synchronized (this) { retiring = stopping && generation == invocationGeneration; }
			// Camel regards a failed stop as stopped and will not call stop again.
			// The finite admitted transport call must therefore release its own owner.
			if (retiring) releaseOwnedResources();
		} finally {
			endpoint.transportLock.unlock();
		}
	}

	private void lockTransport(long deadline) throws Exception {
		if (!endpoint.transportLock.tryLock(MqttV5Endpoint.remainingMillis(deadline),
				java.util.concurrent.TimeUnit.MILLISECONDS)) {
			throw new java.util.concurrent.TimeoutException("MQTT transport admission deadline expired");
		}
	}

	private void publish(byte[] payload, String header, Object value, long deadline) throws Exception {
		try {
			String uri = upstreamPublishUri();
			endpoint.acquirePublisher(this);
			org.apache.camel.Producer publisher = endpoint.preparePublisher(uri, deadline);
			Exchange message = publisher.getEndpoint().createExchange();
			message.getMessage().setBody(payload);
			message.getMessage().setHeader(header, value);
			publisher.process(message);
			if (message.getException() != null) throw message.getException();
		} catch (Exception failure) {
			// Paho leaves timed-out QoS tokens alive. Retire the connection rather
			// than allow a reconnect or another invocation to resend that command.
			try { endpoint.discardPublisher(); }
			catch (Exception cleanup) { failure.addSuppressed(cleanup); }
			throw failure;
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
	protected String upstreamReplyUri() {
		StringBuilder uri = new StringBuilder("paho-mqtt5:").append(endpoint.getReplyTopic());
		uri.append("?brokerUrl=").append(endpoint.getBrokerUrl());
		uri.append("&clientId=").append(endpoint.replyClientId());
		uri.append("&qos=").append(endpoint.getQos());
		appendCredentials(uri);
		return uri.toString();
	}

	/**
	 * The upstream {@code paho-mqtt5:} producer URI derived from this
	 * endpoint's shared transport options — same broker, client id, QoS and
	 * retain flag; the v5 properties ride the CamelPahoMqtt5MsgProperties
	 * header.
	 */
	protected String upstreamPublishUri() {
		StringBuilder uri = new StringBuilder("paho-mqtt5:").append(endpoint.getTopic());
		uri.append("?brokerUrl=").append(endpoint.getBrokerUrl());
		uri.append("&clientId=").append(endpoint.getClientId());
		uri.append("&qos=").append(endpoint.getQos());
		uri.append("&retained=").append(endpoint.isRetained());
		appendCredentials(uri);
		return uri.toString();
	}

	/** Bound credentials are passed on by reference; only unmanaged endpoints carry them inline. */
	private void appendCredentials(StringBuilder uri) {
		if (endpoint.getCredentials() != null) {
			uri.append("&userName=#bean:").append(endpoint.getCredentials()).append(".user");
			uri.append("&password=#bean:").append(endpoint.getCredentials()).append(".password");
			return;
		}
		if (endpoint.getUserName() != null && !endpoint.getUserName().isBlank()) {
			uri.append("&userName=").append(endpoint.getUserName());
		}
		if (endpoint.getPassword() != null) {
			uri.append("&password=").append(endpoint.getPassword());
		}
	}

	private void assertAdmitted(long invocationGeneration) {
		if (stopping || generation != invocationGeneration) {
			throw new IllegalStateException("MQTT producer invocation belongs to a stopped generation");
		}
	}

	@Override
	protected synchronized void doStart() throws Exception {
		if (cleanupPending) throw new IllegalStateException("MQTT producer cleanup remains pending");
		super.doStart();
		generation++;
		stopping = false;
	}

	@Override
	protected void doStop() throws Exception {
		synchronized (this) {
			stopping = true;
			cleanupPending = true;
			endpoint.replies.cancelOwner(this);
		}
		if (!endpoint.transportLock.tryLock(MqttV5Endpoint.STOP_TIMEOUT_MILLIS,
				java.util.concurrent.TimeUnit.MILLISECONDS)) {
			throw new java.util.concurrent.TimeoutException("MQTT transport cleanup remains in flight");
		}
		try {
			releaseOwnedResources();
		} finally {
			endpoint.transportLock.unlock();
			super.doStop();
		}
	}

	private void releaseOwnedResources() throws Exception {
		Exception failure = null;
		try { endpoint.releaseReplySubscription(this); }
		catch (Exception e) { failure = e; }
		try { endpoint.releasePublisher(this); }
		catch (Exception e) {
			if (failure == null) failure = e;
			else failure.addSuppressed(e);
		}
		if (failure != null) throw failure;
		cleanupPending = false;
	}

}
