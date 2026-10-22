/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.configuration;

import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;
import dk.aau.smartproductionlab.dmp.influxdb2.component.DmpInfluxDb2Component;

/** A TimeSeries LinkedSegment read by polling: its latest row. */
public class InfluxDb2DataSourceConfiguration extends DataSourceConfiguration {
	private String measurement;
	private String bucket;
	private String timePath;
	private Map<String, String> tags;
	private List<RecordColumn> columns;

	public InfluxDb2DataSourceConfiguration() {}

	public InfluxDb2DataSourceConfiguration(String uniqueId, String host, int port, String measurement, String bucket,
			String timePath, Map<String, String> tags, List<RecordColumn> columns) {
		super(uniqueId, host, port);
		this.measurement = measurement;
		this.bucket = bucket;
		this.timePath = timePath;
		this.tags = Map.copyOf(tags);
		this.columns = List.copyOf(columns);
		setDelivery("sync");
	}

	public String getMeasurement() { return measurement; }
	public String getBucket() { return bucket; }
	public Map<String, String> getTags() { return tags; }
	public List<RecordColumn> getColumns() { return columns; }

	@Override
	public String getConnectionURI() {
		return DmpInfluxDb2Component.uri(measurement, bucket, timePath, tags, columns);
	}
}
