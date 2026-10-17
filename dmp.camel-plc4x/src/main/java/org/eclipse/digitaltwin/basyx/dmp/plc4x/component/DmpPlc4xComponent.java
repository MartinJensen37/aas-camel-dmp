/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.plc4x.component;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Endpoint;
import org.apache.camel.component.plc4x.Plc4XComponent;
import org.apache.camel.util.PropertiesHelper;

/**
 * {@code dmp-plc4x:<driver>://<host>:<port>?tag.<name>=<address>&<driver options>}: camel-plc4x
 * with acknowledged writes and checked reads (see {@link DmpPlc4xEndpoint}). URIs take the same
 * tag, trigger and period parameters as {@code plc4x:}; every other parameter that is not a Camel
 * endpoint option is a PLC4X driver option and becomes part of the connection string.
 */
@org.apache.camel.spi.annotations.Component("dmp-plc4x")
public class DmpPlc4xComponent extends Plc4XComponent {
	private static final Set<String> CAMEL_OPTIONS = Set.of("autoReconnect", "lazyStartProducer",
			"bridgeErrorHandler", "exceptionHandler", "exchangePattern");

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
		Map<String, String> tags = new LinkedHashMap<>();
		@SuppressWarnings("unchecked")
		Map<String, Object> declared = getAndRemoveOrResolveReferenceParameter(parameters, "tags", Map.class);
		if (declared != null) declared.forEach((name, address) -> tags.put(name, String.valueOf(address)));
		PropertiesHelper.extractProperties(parameters, "tag.").forEach((name, address) -> tags.put(name, String.valueOf(address)));
		String trigger = getAndRemoveOrResolveReferenceParameter(parameters, "trigger", String.class);
		Integer period = getAndRemoveOrResolveReferenceParameter(parameters, "period", Integer.class);

		StringBuilder connection = new StringBuilder(remaining);
		String separator = remaining.contains("?") ? "&" : "?";
		for (var option : new LinkedHashMap<>(parameters).entrySet()) {
			if (CAMEL_OPTIONS.contains(option.getKey())) continue;
			connection.append(separator).append(URLEncoder.encode(option.getKey(), StandardCharsets.UTF_8))
					.append('=').append(URLEncoder.encode(String.valueOf(option.getValue()), StandardCharsets.UTF_8));
			separator = "&";
			parameters.remove(option.getKey());
		}

		DmpPlc4xEndpoint endpoint = new DmpPlc4xEndpoint(uri, this, connection.toString());
		endpoint.setTags(tags);
		if (trigger != null) endpoint.setTrigger(trigger);
		if (period != null) endpoint.setPeriod(period);
		setProperties(endpoint, parameters);
		return endpoint;
	}
}
