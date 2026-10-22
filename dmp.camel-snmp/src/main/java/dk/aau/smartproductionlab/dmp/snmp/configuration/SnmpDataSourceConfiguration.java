/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.snmp.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.snmp.component.DmpSnmpComponent;

/** An SNMP object read by polling (GET). */
public class SnmpDataSourceConfiguration extends DataSourceConfiguration {
	private String oid;
	private String version;
	private String community;

	public SnmpDataSourceConfiguration() {}

	public SnmpDataSourceConfiguration(String uniqueId, String host, int port, String oid, String version, String community) {
		super(uniqueId, host, port);
		this.oid = oid;
		this.version = version;
		this.community = community;
		setDelivery("sync");
	}

	public String getOid() { return oid; }

	@Override
	public String getConnectionURI() {
		return DmpSnmpComponent.uri(getServerUrl(), getServerPort(), oid, version, community);
	}
}
