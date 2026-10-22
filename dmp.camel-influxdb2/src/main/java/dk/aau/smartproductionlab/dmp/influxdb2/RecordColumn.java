/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.influxdb2;

import java.util.Objects;

/**
 * One field of a TimeSeries Record: its idShort path below the Record ({@code Position.X}, the key Lua uses), the
 * InfluxDB field it is stored in (the field's authored {@code Column}) and its AAS valueType.
 */
public record RecordColumn(String path, String column, String valueType) {
	public RecordColumn {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(column, "column");
		Objects.requireNonNull(valueType, "valueType");
		if (InfluxValues.kind(valueType) == null) throw new IllegalArgumentException("unsupported Record valueType " + valueType);
	}
}
