/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.snmp.component.DmpSnmpComponent;

/** An SNMP object written with SET. */
public class SnmpDataSinkConfiguration extends DataSinkConfiguration {
	private String host;
	private int port;
	private String oid;
	private String version;
	private String community;

	public SnmpDataSinkConfiguration() {}

	public SnmpDataSinkConfiguration(String uniqueId, String host, int port, String oid, String version, String community) {
		super(uniqueId);
		this.host = host;
		this.port = port;
		this.oid = oid;
		this.version = version;
		this.community = community;
	}

	public String getOid() { return oid; }

	@Override
	public String getConnectionURI() {
		return DmpSnmpComponent.uri(host, port, oid, version, community);
	}
}
