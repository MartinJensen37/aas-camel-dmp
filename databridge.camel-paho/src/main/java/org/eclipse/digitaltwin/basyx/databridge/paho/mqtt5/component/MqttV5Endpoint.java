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

import org.apache.camel.ExchangePattern;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;

/**
 * Endpoint for the {@code paho-mqtt5-inout:} scheme.
 *
 * <p>
 * InOut options: {@code replyTopic} (the DECLARED {@code mqv_response_topic}),
 * {@code replyTimeout} (seconds), {@code correlationHeader} (optional — when
 * set, the generated correlation id is exposed on that exchange header and a
 * non-blank header value is used as the correlation instead of a fresh UUID),
 * and {@code skipReplyStates} (intermediate reply filter — payloads whose
 * top-level {@code State} field matches are not treated as terminal replies).
 * The publish itself rides the upstream {@code paho-mqtt5:} producer derived
 * from the shared transport options (brokerUrl, clientId, qos, retained,
 * userName, password).
 *
 * @author aas-camel-dmp
 */
@UriEndpoint(firstVersion = "1.0.0-SNAPSHOT", scheme = "paho-mqtt5-inout", title = "Paho MQTT v5 InOut",
		syntax = "paho-mqtt5-inout:topic", producerOnly = true, category = { org.apache.camel.Category.MESSAGING })
public class MqttV5Endpoint extends org.apache.camel.support.DefaultEndpoint {

	@UriPath(description = "MQTT topic to publish the request on (absolute topic path)")
	@Metadata(required = true)
	private String topic;

	@Metadata(required = true, description = "MQTT broker URL (tcp://host:port)")
	private String brokerUrl = "tcp://localhost:1883";

	@Metadata(required = true, description = "Client id of the request/reply connection")
	private String clientId;

	@Metadata(defaultValue = "2", description = "Request QoS (reply subscription uses the same QoS)")
	private int qos = 2;

	@Metadata(defaultValue = "false", description = "Retain the published request message")
	private boolean retained = false;

	@Metadata(description = "Reply topic (the declared mqv_response_topic); REQUIRED for InOut")
	private String replyTopic;

	@Metadata(defaultValue = "30", description = "Reply wait budget in seconds")
	private long replyTimeout = 30;

	@Metadata(description = "Optional exchange header name for the correlation id; "
			+ "when present and non-blank its value IS the correlation data, otherwise "
			+ "a UUID is minted and exposed on this header")
	private String correlationHeader;

	@Metadata(defaultValue = "", description = "Comma-separated reply 'State' values treated "
			+ "as intermediate (non-terminal) replies — the exchange is NOT completed by them")
	private String skipReplyStates;

	@Metadata(description = "MQTT user name (optional)")
	private String userName;

	@Metadata(description = "MQTT password (optional)")
	private String password;

	private volatile MqttV5Producer producer;

	public MqttV5Endpoint(String endpointUri, MqttV5Component component) {
		super(endpointUri, component);
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getBrokerUrl() {
		return brokerUrl;
	}

	public void setBrokerUrl(String brokerUrl) {
		this.brokerUrl = brokerUrl;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public int getQos() {
		return qos;
	}

	public void setQos(int qos) {
		this.qos = qos;
	}

	public boolean isRetained() {
		return retained;
	}

	public void setRetained(boolean retained) {
		this.retained = retained;
	}

	public String getReplyTopic() {
		return replyTopic;
	}

	public void setReplyTopic(String replyTopic) {
		this.replyTopic = replyTopic;
	}

	public long getReplyTimeout() {
		return replyTimeout;
	}

	public void setReplyTimeout(long replyTimeout) {
		this.replyTimeout = replyTimeout;
	}

	public String getCorrelationHeader() {
		return correlationHeader;
	}

	public void setCorrelationHeader(String correlationHeader) {
		this.correlationHeader = correlationHeader;
	}

	public String getSkipReplyStates() {
		return skipReplyStates;
	}

	public void setSkipReplyStates(String skipReplyStates) {
		this.skipReplyStates = skipReplyStates;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public ExchangePattern getExchangePattern() {
		return ExchangePattern.InOut;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) throws Exception {
		throw new UnsupportedOperationException("The paho-mqtt5 component is producer-only "
				+ "(InOut request/reply leg); replies are correlated by the producer.");
	}

	@Override
	public org.apache.camel.Producer createProducer() throws Exception {
		synchronized (this) {
			if (producer == null) {
				producer = new MqttV5Producer(this);
			}
			return producer;
		}
	}
}
