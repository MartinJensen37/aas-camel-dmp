/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2;

import java.util.Objects;

/**
 * One field of a TimeSeries Record: its idShortPath below the Record ({@code Position.x}, list members by index,
 * {@code Waypoints[0].x}), which is both the key Lua uses and the InfluxDB field it is stored in, and its AAS
 * valueType.
 */
public record RecordColumn(String path, String valueType) {
	public RecordColumn {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(valueType, "valueType");
		if (InfluxValues.kind(valueType) == null) throw new IllegalArgumentException("unsupported Record valueType " + valueType);
	}
}
