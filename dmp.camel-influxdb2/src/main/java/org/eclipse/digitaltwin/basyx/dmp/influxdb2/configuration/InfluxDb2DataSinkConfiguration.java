/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.influxdb2.configuration;

import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.influxdb2.RecordColumn;
import org.eclipse.digitaltwin.basyx.dmp.influxdb2.component.DmpInfluxDb2Component;

/** A TimeSeries LinkedSegment written one point per row. */
public class InfluxDb2DataSinkConfiguration extends DataSinkConfiguration {
	private String measurement;
	private String bucket;
	private String timePath;
	private Map<String, String> tags;
	private List<RecordColumn> columns;

	public InfluxDb2DataSinkConfiguration() {}

	public InfluxDb2DataSinkConfiguration(String uniqueId, String measurement, String bucket, String timePath,
			Map<String, String> tags, List<RecordColumn> columns) {
		super(uniqueId);
		this.measurement = measurement;
		this.bucket = bucket;
		this.timePath = timePath;
		this.tags = Map.copyOf(tags);
		this.columns = List.copyOf(columns);
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
