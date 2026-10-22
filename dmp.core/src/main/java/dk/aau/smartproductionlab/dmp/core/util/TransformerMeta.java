/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.util;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The shape of Lua's global {@code meta} (SPEC §8): {@code sources} and {@code sinks} keyed by SourceId and SinkId,
 * and, in an Operation's transformations, {@code invocation}. A source carries the {@code time} its value was
 * acquired, a sink the {@code kind} of connector it writes through (the scheme of its endpoint, e.g. {@code aas},
 * {@code dmp-influxdb2}).
 */
public final class TransformerMeta {
	private TransformerMeta() { }

	public static Map<String, Object> of(Map<String, Object> sources, Map<String, Object> sinks,
			Map<String, Object> invocation) {
		Map<String, Object> meta = new LinkedHashMap<>();
		meta.put("sources", sources == null ? Map.of() : sources);
		meta.put("sinks", sinks == null ? Map.of() : sinks);
		if (invocation != null) meta.put("invocation", invocation);
		return meta;
	}

	/** A source acquired at {@code time}. */
	public static Map<String, Object> source(Instant time) {
		return Map.of("time", time.toString());
	}

	/** A sink writing through {@code endpointUri}. */
	public static Map<String, Object> sink(String endpointUri) {
		int colon = endpointUri == null ? -1 : endpointUri.indexOf(':');
		return Map.of("kind", colon <= 0 ? "unknown" : endpointUri.substring(0, colon));
	}
}
