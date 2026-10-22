/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2.component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Endpoint;
import org.apache.camel.component.influxdb2.InfluxDb2Component;
import org.apache.camel.util.PropertiesHelper;
import dk.aau.smartproductionlab.dmp.influxdb2.InfluxSettings;
import dk.aau.smartproductionlab.dmp.influxdb2.RecordColumn;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;

/**
 * {@code dmp-influxdb2:<measurement>?bucket=…&time=Time&tag.<key>=…&field.<path>=<valueType>[&start=…][&stop=…]}:
 * the points of one TimeSeries LinkedSegment, within its authored period when it has one. A write is one point per row, acknowledged by InfluxDB; a rejected or failed write
 * raises. camel-influxdb2 writes through the asynchronous batching API, whose failures never reach the exchange,
 * and creates missing orgs and buckets, so the DMP ships this component (SPEC §9). All endpoints share the one
 * configured InfluxDB ({@link #configure}); its token never appears in a URI.
 */
public final class DmpInfluxDb2Component extends InfluxDb2Component {
	public static final String SCHEME = "dmp-influxdb2";

	private InfluxSettings settings;

	/** The endpoint URI of one segment. Values are passed RAW so Camel leaves them unchanged. */
	public static String uri(String measurement, String bucket, String timePath, Map<String, String> tags,
			List<RecordColumn> columns, Instant start, Instant stop) {
		StringBuilder uri = new StringBuilder(SCHEME).append(':').append(encode(measurement))
				.append("?bucket=RAW(").append(bucket).append(')')
				.append("&time=RAW(").append(timePath).append(')');
		tags.forEach((key, value) -> uri.append("&tag.").append(encode(key)).append("=RAW(").append(value).append(')'));
		for (RecordColumn column : columns)
			uri.append("&field.").append(encode(column.path())).append('=').append(column.valueType());
		if (start != null) uri.append("&start=RAW(").append(start).append(')');
		if (stop != null) uri.append("&stop=RAW(").append(stop).append(')');
		return uri.toString();
	}

	/** Sets the InfluxDB every endpoint writes to; replaces the client of an earlier configuration. */
	public synchronized void configure(InfluxSettings settings) {
		InfluxDBClient previous = getInfluxDBClient();
		this.settings = settings;
		setInfluxDBClient(settings == null ? null
				: InfluxDBClientFactory.create(settings.url().toString(), settings.token().toCharArray(), settings.org()));
		if (previous != null) previous.close();
	}

	synchronized InfluxSettings settings() { return settings; }

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		if (remaining == null || remaining.isBlank()) throw new IllegalArgumentException("dmp-influxdb2 endpoint must name a measurement");
		String bucket = getAndRemoveParameter(parameters, "bucket", String.class);
		if (bucket == null || bucket.isBlank()) throw new IllegalArgumentException("dmp-influxdb2 endpoint requires a bucket");
		String timePath = getAndRemoveParameter(parameters, "time", String.class);
		if (timePath == null || timePath.isBlank()) throw new IllegalArgumentException("dmp-influxdb2 endpoint requires the Record's time path");
		Map<String, String> tags = new LinkedHashMap<>();
		PropertiesHelper.extractProperties(parameters, "tag.").forEach((key, value) -> tags.put(key, String.valueOf(value)));
		List<RecordColumn> columns = PropertiesHelper.extractProperties(parameters, "field.").entrySet().stream()
				.map(entry -> new RecordColumn(entry.getKey(), String.valueOf(entry.getValue())))
				.toList();
		if (columns.isEmpty()) throw new IllegalArgumentException("dmp-influxdb2 endpoint needs at least one Record field");
		Instant start = instant(getAndRemoveParameter(parameters, "start", String.class), "start");
		Instant stop = instant(getAndRemoveParameter(parameters, "stop", String.class), "stop");
		DmpInfluxDb2Endpoint endpoint = new DmpInfluxDb2Endpoint(uri, this, remaining, bucket, timePath, tags, columns,
				start, stop);
		setProperties(endpoint, parameters);
		return endpoint;
	}

	@Override
	protected synchronized void doStop() throws Exception {
		InfluxDBClient client = getInfluxDBClient();
		setInfluxDBClient(null);
		if (client != null) client.close();
		super.doStop();
	}

	private static Instant instant(String value, String name) {
		if (value == null || value.isBlank()) return null;
		try { return Instant.parse(value); }
		catch (java.time.format.DateTimeParseException invalid) {
			throw new IllegalArgumentException("dmp-influxdb2 " + name + " must be an instant such as 2026-10-21T00:00:00Z");
		}
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}
}
