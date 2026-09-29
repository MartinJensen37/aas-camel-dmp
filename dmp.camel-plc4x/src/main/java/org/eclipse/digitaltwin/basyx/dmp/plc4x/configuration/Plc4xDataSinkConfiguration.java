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
package org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration;

import java.util.List;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.deserializer.OptionDeserializer;

/**
 * PLC4X write sink (DMP v3): the camel-plc4x producer writes the transformed
 * tag values (e.g. Modbus coils/registers via the modbus-tcp driver). Mirrors
 * {@link Plc4xConsumerConfiguration} — same connection URI shape, but a
 * datasink.
 *
 * @author aas-camel-dmp
 */
public class Plc4xDataSinkConfiguration extends DataSinkConfiguration {

	private static final String PLC4X_PROTOCOL_NAME = "plc4x:";

	private String serverUrl;
	private int serverPort;
	private String driver;
	private String servicePath = "";
	private Object options;
	private List<Tag> tags;

	public Plc4xDataSinkConfiguration() {
	}

	public Plc4xDataSinkConfiguration(String uniqueId, String serverUrl, int serverPort, String driver,
			String servicePath, Object options, List<Tag> tags) {
		super(uniqueId);
		this.serverUrl = serverUrl;
		this.serverPort = serverPort;
		this.driver = driver;
		this.servicePath = servicePath;
		this.options = options;
		this.tags = tags;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public int getServerPort() {
		return serverPort;
	}

	public String getServicePath() {
		return servicePath;
	}

	public String getDriver() {
		return driver;
	}

	public List<Option> getOptions() {
		return new OptionDeserializer().deserialize(options);
	}

	public List<Tag> getTags() {
		return tags;
	}

	@Override
	public String getConnectionURI() {
		org.apache.http.client.utils.URIBuilder uriBuilder = new org.apache.http.client.utils.URIBuilder();
		uriBuilder.setScheme(PLC4X_PROTOCOL_NAME + driver);
		uriBuilder.setHost(getServerUrl());
		uriBuilder.setPort(getServerPort());
		uriBuilder.setPath(servicePath);

		for (Option option : getOptions()) {
			uriBuilder.addParameter(option.getName(), option.getValue());
		}
		for (Tag tag : tags) {
			uriBuilder.addParameter("tag." + tag.getName(), tag.getValue());
		}

		return uriBuilder.toString();
	}
}
