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
package dk.aau.smartproductionlab.dmp.opcua.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An implementation of OpcUa consumer configuration
 *
 * @author Daniele Rossi
 * @see <a href="https://camel.apache.org/components/3.20.x/milo-client-component.html">Documentation Apache Camel - OPC UA Client Parameters</a>
 *
 */
public class OpcuaDataSourceConfiguration extends DataSourceConfiguration {
	protected static Logger logger = LoggerFactory.getLogger(OpcuaDataSourceConfiguration.class);

	private String pathToService;
	private String nodeInformation;
	private String username;
	private String password;
	private Map<String, String> configuration = new HashMap<>();

	public OpcuaDataSourceConfiguration() {
	}

	@Override
	public String getSourceKind() {
		String configured = super.getSourceKind();
		return configured == null || configured.isBlank() ? "opcua" : configured;
	}

	public OpcuaDataSourceConfiguration(
			String uniqueId,
			String serverUrl,
			int serverPort,
			String pathToService,
			String nodeInformation,
			String username,
			String password,
			Map<String, String> configuration
	) {
		super(uniqueId, serverUrl, serverPort);

		this.pathToService = pathToService;
		this.nodeInformation = nodeInformation;
		this.username = username;
		this.password = password;
		this.configuration = configuration;

		if (!this.configuration.containsKey("allowedSecurityPolicies"))
			this.configuration.put("allowedSecurityPolicies", "None");
		// The server is reached at the AID's base: the host a server reports in its endpoints (often its
		// hostname, e.g. 4diac FORTE's) need not resolve where the DMP runs, such as in a container.
		if (!this.configuration.containsKey("overrideHost"))
			this.configuration.put("overrideHost", "true");
		if (!this.configuration.containsKey("requestedPublishingInterval"))
			this.configuration.put("requestedPublishingInterval", "1000");
	}

	public String getPathToService() {
		return pathToService;
	}

	public void setPathToService(String pathToService) {
		this.pathToService = pathToService;
	}

	public String getNodeInformation() {
		return nodeInformation;
	}

	public void setNodeInformation(String nodeInformation) {
		this.nodeInformation = nodeInformation;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Map<String, String> getConfiguration() {
		return configuration;
	}

	public void setConfiguration(Map<String, String> configuration) {
		this.configuration = configuration;
	}

	/** A node named by an AID browse path (starting with {@code /}) is the {@code browsePath} option, else {@code node}. */
	static String nodeParameter(String node) {
		return node != null && node.startsWith("/") ? "browsePath" : "node";
	}

	public String getConnectionURI() {
		String credentials = username == null || password == null ? "" : username + ":" + password + "@";
		String parameters = this.configuration.entrySet()
				.parallelStream()
				.filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
				.map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
				.collect(Collectors.joining("&"));

		return String.format(
				"dmp-milo-client:opc.tcp://%s%s:%d/%s?%s=RAW(%s)%s",
				credentials,
				getServerUrl(),
				getServerPort(),
				getPathToService(),
				nodeParameter(getNodeInformation()),
				getNodeInformation(),
				parameters.length() > 0 ? "&" + parameters : ""
		);
	}
}
