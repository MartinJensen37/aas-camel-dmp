/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.paho.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.security.BasicCredential;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;

/**
 * An implementation of MQTT consumer configuration
 * @author haque
 *
 */
public class MqttDataSourceConfiguration extends DataSourceConfiguration implements ManagedRouteResource {
	
	private static final String MQTT_PROTOCOL_NAME = "paho-mqtt5:";
	private static final String BROKER_URL_PARAM_NAME = "brokerUrl";
	private static final String BROKER_URL_PREFIX = "tcp://";
	private static final String USERNAME_PARAMETER = "userName";
	private static final String PASSWORD_PARAMETER = "password";
	
	private String topic;
	private String clientId;
	private BasicCredential credentials;
	private BoundCredential bound;
	/** DECLARED subscription quality of service (mqv_qos) — absent stays unset. */
	private Integer qos;

	public MqttDataSourceConfiguration() {}

	@Override
	public String getSourceKind() {
		String configured = super.getSourceKind();
		return configured == null || configured.isBlank() ? "mqtt" : configured;
	}
	
	public Integer getQos() {
		return qos;
	}

	public void setQos(Integer qos) {
		this.qos = qos;
	}

	public MqttDataSourceConfiguration(String uniqueId, String serverUrl, int serverPort, String clientId, String topic, BasicCredential credentials) {
		super(uniqueId, serverUrl, serverPort);
		this.topic = topic;
		this.clientId = clientId;
		this.credentials = credentials;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public BasicCredential getCredentials() {
		return credentials;
	}

	public String getConnectionURI() {
		
		StringBuilder connectionUriBuilder = new StringBuilder();
		connectionUriBuilder.append(MQTT_PROTOCOL_NAME);
		connectionUriBuilder.append(topic);
		connectionUriBuilder.append("?");
		connectionUriBuilder.append(getMqttBrokerUrl(getServerUrl(), getServerPort()));
		
		addCredentialsIfConfigured(connectionUriBuilder);
		if (qos != null) {
			connectionUriBuilder.append("&qos=").append(qos.intValue());
		}
		
		return connectionUriBuilder.toString();
	}
	
	/** The URI only names the credentials; the owning bundle binds them in the registry. */
	private void addCredentialsIfConfigured(StringBuilder connectionUriBuilder) {
		BoundCredential credential = boundCredential();
		if (credential == null)
			return;
		
		connectionUriBuilder.append("&" + USERNAME_PARAMETER + "=");
		connectionUriBuilder.append(credential.userNameReference());
		connectionUriBuilder.append("&" + PASSWORD_PARAMETER + "=");
		connectionUriBuilder.append(credential.passwordReference());
	}

	private synchronized BoundCredential boundCredential() {
		if (bound == null && credentials != null && credentials.getUserName() != null && !credentials.getUserName().isBlank())
			bound = new BoundCredential(credentials.getUserName(), credentials.getPassword());
		return bound;
	}

	@Override
	public void bind(CamelContext context) {
		BoundCredential credential = boundCredential();
		if (credential != null) credential.bind(context);
	}

	@Override
	public synchronized void close() {
		if (bound != null) bound.close();
	}

	private String getMqttBrokerUrl(String serverUrl, int port) {
		
		StringBuilder mqttBrokerUrlBuilder = new StringBuilder();
		mqttBrokerUrlBuilder.append(BROKER_URL_PARAM_NAME);
		mqttBrokerUrlBuilder.append("=");
		mqttBrokerUrlBuilder.append(BROKER_URL_PREFIX);
		mqttBrokerUrlBuilder.append(serverUrl);
		mqttBrokerUrlBuilder.append(":");
		mqttBrokerUrlBuilder.append(port);
		
		addClientIdIfConfigured(mqttBrokerUrlBuilder);
		
		return mqttBrokerUrlBuilder.toString();
	}

	private void addClientIdIfConfigured(StringBuilder mqttBrokerUrlBuilder) {
		
		if (clientId == null || clientId.isBlank())
			return;
		
		mqttBrokerUrlBuilder.append("&clientId=");
		mqttBrokerUrlBuilder.append(clientId);
	}
	
}
