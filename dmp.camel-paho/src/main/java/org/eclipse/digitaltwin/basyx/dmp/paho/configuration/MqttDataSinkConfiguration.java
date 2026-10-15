/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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

package org.eclipse.digitaltwin.basyx.dmp.paho.configuration;

import org.apache.camel.CamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource;
import org.eclipse.digitaltwin.basyx.dmp.core.security.BoundCredential;

/**
 * An implementation of MQTT producer configuration
 * @author rana
 *
 */
public class MqttDataSinkConfiguration extends DataSinkConfiguration implements ManagedRouteResource {
		
	private String serverUrl;
	private int serverPort;
	private String topic;
	private String userName;
	private String password;
	private String clientId;
	private BoundCredential bound;
	/** DECLARED MQTT qualifiers (mqv_qos/mqv_retain) — absent stays unset. */
	private Integer qos;
	private Boolean retained;

	public MqttDataSinkConfiguration() {
	}

	public MqttDataSinkConfiguration(String uniqueId, String serverUrl, int serverPort, String topic, String clientId,
			String userName, String password) {
		super(uniqueId);
		setServerUrl(serverUrl);
		setServerPort(serverPort);
		setTopic(topic);
		setClientId(clientId);
		setUserName(userName);
		setPassword(password);
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
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

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	
	private String buildOptions() {
		
		StringBuilder options = new StringBuilder();
		options.append("?brokerUrl=tcp://");
		options.append(getServerUrl());
		options.append(":");
		options.append(Integer.toString(getServerPort()));
		options.append("&clientId=");
		options.append(getClientId());
		
		if (getQos() != null) {
			options.append("&qos=");
			options.append(getQos().intValue());
		}
		if (getRetained() != null) {
			options.append("&retained=");
			options.append(getRetained().booleanValue());
		}
		// The URI only names the credentials; the owning bundle binds them in the registry.
		BoundCredential credential = boundCredential();
		if (credential != null) {
			options.append("&userName=");
			options.append(credential.userNameReference());
			options.append("&password=");
			options.append(credential.passwordReference());
		}
		return options.toString();
	}

	private synchronized BoundCredential boundCredential() {
		if (bound == null && userName != null && !userName.isBlank()) bound = new BoundCredential(userName, password);
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

	public Integer getQos() {
		return qos;
	}

	public void setQos(Integer qos) {
		this.qos = qos;
	}

	public Boolean getRetained() {
		return retained;
	}

	public void setRetained(Boolean retained) {
		this.retained = retained;
	}

	@Override
	public String getConnectionURI() {
		// brokerUrl already carries the port; do NOT append it again.
		StringBuilder connUri = new StringBuilder();
		connUri.append("paho-mqtt5:");
		connUri.append(getTopic());
		connUri.append(buildOptions());

		return connUri.toString();
	}

	@Override
	public boolean requiresJsonWireEncoding() {
		return hasJsonSchema(getPayloadSchema()) || hasJsonSchema(getSchema());
	}

	private static boolean hasJsonSchema(Object value) {
		if (!(value instanceof java.util.Map<?, ?> schema) || schema.isEmpty()) return false;
		return schema.containsKey("type") || schema.containsKey("properties") || schema.containsKey("items")
				|| schema.containsKey("const") || schema.containsKey("enum") || schema.containsKey("oneOf")
				|| schema.containsKey("anyOf") || schema.containsKey("allOf");
	}
}
