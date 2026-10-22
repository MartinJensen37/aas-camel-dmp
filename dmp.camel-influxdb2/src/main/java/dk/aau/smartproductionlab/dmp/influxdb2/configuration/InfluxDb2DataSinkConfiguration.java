/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.configuration;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;
import dk.aau.smartproductionlab.dmp.influxdb2.component.DmpInfluxDb2Component;

/** A TimeSeries LinkedSegment written one point per row. */
public class InfluxDb2DataSinkConfiguration extends DataSinkConfiguration {
	private String measurement;
	private String bucket;
	private String timePath;
	private Map<String, String> tags;
	private List<RecordColumn> columns;
	private Instant start;
	private Instant end;

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
	public Instant getStart() { return start; }
	public Instant getEnd() { return end; }

	/** The segment's authored period, either end {@code null} where it has none. */
	public void setPeriod(Instant start, Instant end) {
		this.start = start;
		this.end = end;
	}

	@Override
	public String getConnectionURI() {
		return DmpInfluxDb2Component.uri(measurement, bucket, timePath, tags, columns, start, end);
	}
}
