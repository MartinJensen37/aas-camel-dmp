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
package dk.aau.smartproductionlab.dmp.management.configuration;

import java.nio.charset.StandardCharsets;

import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;

/** One-operation MQTT 5 publisher configuration: correlated request/reply, or one-way without a reply topic. */
public final class MqttV5OperationDataSinkConfiguration extends DataSinkConfiguration implements ManagedRouteResource {
	private final String brokerUrl;
	private final String topic;
	private final String clientId;
	private final int qos;
	private final boolean retained;
	private final String replyTopic;
	private final long replyTimeoutSeconds;
	private final String username;
	private final String password;
	private BoundCredential bound;

	public MqttV5OperationDataSinkConfiguration(String uniqueId,String brokerUrl,String topic,String clientId,
			int qos,boolean retained,String replyTopic,long replyTimeoutSeconds,String username,String password) {
		super(uniqueId);
		if(brokerUrl==null || !brokerUrl.startsWith("tcp://") || clientId==null || clientId.isBlank()
				|| topic==null || topic.isBlank() || topic.contains("+") || topic.contains("#")
				|| qos<0 || qos>2 || replyTimeoutSeconds<1) {
			throw new IllegalArgumentException("invalid MQTT 5 operation target settings");
		}
		// No reply topic: an invoke-only action, published one way.
		if(replyTopic!=null && (replyTopic.isBlank() || replyTopic.contains("+") || replyTopic.contains("#")))
			throw new IllegalArgumentException("MQTT 5 InOut requires a concrete reply topic");
		this.brokerUrl=brokerUrl;this.topic=topic;this.clientId=clientId;this.qos=qos;this.retained=retained;
		this.replyTopic=replyTopic;this.replyTimeoutSeconds=replyTimeoutSeconds;this.username=username;this.password=password;
	}

	@Override public String getConnectionURI() {
		StringBuilder uri=new StringBuilder("paho-mqtt5-inout:").append(encodeTopic(topic)).append("?")
				.append("brokerUrl=").append(encodeValue(brokerUrl))
				.append("&clientId=").append(encodeValue(clientId))
				.append("&qos=").append(qos)
				.append("&retained=").append(retained)
				.append("&replyTimeout=").append(replyTimeoutSeconds);
		if(replyTopic!=null)uri.append("&replyTopic=").append(encodeValue(replyTopic));
		// The URI only names the credentials; the owning bundle binds them in the registry.
		BoundCredential credential=boundCredential();
		if(credential!=null)uri.append("&credentials=").append(credential.name());
		return uri.toString();
	}

	private synchronized BoundCredential boundCredential() {
		if(bound==null && username!=null && !username.isBlank())bound=new BoundCredential(username,password);
		return bound;
	}

	@Override public void bind(CamelContext context) {
		BoundCredential credential=boundCredential();
		if(credential!=null)credential.bind(context);
	}

	@Override public synchronized void close() {
		if(bound!=null)bound.close();
	}

	private static String encodeValue(String value) {
		return java.net.URLEncoder.encode(value,StandardCharsets.UTF_8).replace("+","%20");
	}

	private static String encodeTopic(String value) {
		StringBuilder encoded=new StringBuilder();
		for(byte octet:value.getBytes(StandardCharsets.UTF_8)) {
			int ch=octet&0xff;
			if((ch>='a'&&ch<='z')||(ch>='A'&&ch<='Z')||(ch>='0'&&ch<='9')||"-._~/".indexOf(ch)>=0) {
				encoded.append((char)ch);
			} else {
				encoded.append('%').append(Character.forDigit((ch>>>4)&0xf,16)).append(Character.forDigit(ch&0xf,16));
			}
		}
		return encoded.toString();
	}
}
