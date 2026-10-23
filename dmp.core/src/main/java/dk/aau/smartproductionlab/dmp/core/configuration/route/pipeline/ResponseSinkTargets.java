/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.routebuilder.DmpRouteBuilder;

/**
 * A mapping's ResponseSinks as {@link ResponseSinkFanOutProcessor} targets: each binding with the endpoint it
 * writes. An AAS element is written through an adapter route that wraps the value in the connector's
 * {@code {sinkId: value}} envelope, since an AAS endpoint writes one element per destination.
 */
public final class ResponseSinkTargets {
	private ResponseSinkTargets() { }

	public static List<Map<String, Object>> resolve(RouteBuilder routeBuilder, RoutesConfiguration routesConfiguration,
			String routeId, List<Map<String, Object>> bindings) {
		List<Map<String, Object>> targets = new ArrayList<>();
		int aasRouteIndex = 0;
		for (Map<String, Object> binding : bindings) {
			String dataSinkId = String.valueOf(binding.get("dataSinkId"));
			String endpoint = RouteCreatorHelper.getDataSinkEndpoint(routesConfiguration, dataSinkId, routeId);
			Map<String, Object> target = new LinkedHashMap<>(binding);
			if (Boolean.TRUE.equals(binding.get("aas"))) {
				String sinkId = String.valueOf(binding.get("sinkId"));
				String adapterRouteId = (routeId + "-aas-response-" + aasRouteIndex++).replaceAll("[^A-Za-z0-9_.-]", "-");
				if (routeBuilder instanceof DmpRouteBuilder managed) managed.registerRouteId(adapterRouteId);
				String adapterUri = "direct:" + adapterRouteId;
				routeBuilder.from(adapterUri)
						.routeId(adapterRouteId)
						.process(exchange -> {
							Map<String, Object> envelope = new LinkedHashMap<>();
							envelope.put(sinkId, exchange.getMessage().getBody());
							exchange.getMessage().setBody(envelope);
						})
						.to(endpoint);
				target.put("endpoint", adapterUri);
				// The AAS endpoint's own valuePath extracts SinkId from the envelope above.
				target.put("payloadGroup", null);
				target.put("valuePath", null);
				target.put("jsonPayload", false);
			} else {
				target.put("endpoint", endpoint);
			}
			target.remove("aas");
			targets.add(Collections.unmodifiableMap(target));
		}
		return List.copyOf(targets);
	}
}
