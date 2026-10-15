package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;

/** Projects a validated device reply into the authored response SourceIds. */
public final class ResponseProjectionProcessor implements Processor {
	private final List<Map<String, Object>> bindings;

	public ResponseProjectionProcessor(List<Map<String, Object>> bindings) {
		this.bindings = List.copyOf(bindings);
	}

	@Override
	public void process(Exchange exchange) {
		exchange.getMessage().setBody(project(exchange.getMessage().getBody(), bindings));
	}

	public static Map<String, Object> project(Object parsed, List<Map<String, Object>> bindings) {
		Map<String, Object> sources = new LinkedHashMap<>();
		for (Map<String, Object> binding : bindings) {
			String sourceId = String.valueOf(binding.get("sourceId"));
			if (sources.containsKey(sourceId)) {
				throw projectionError("duplicate ResponseSource id " + sourceId);
			}
			Object value = parsed == JsonPointer.JSON_NULL ? null : parsed;
			Object path = binding.get("valuePath");
			if (path != null && !String.valueOf(path).isBlank()) {
				Object selected = JsonPointer.extract(parsed, String.valueOf(path))
						.orElseThrow(() -> projectionError("missing ResponseSource valuePath " + path
								+ " for " + sourceId));
				value = selected == JsonPointer.JSON_NULL ? null : selected;
			}
			sources.put(sourceId, value);
		}
		return sources;
	}

	private static PayloadValidationException projectionError(String message) {
		return new PayloadValidationException(PayloadValidationException.Stage.MAPPING,
				message, List.of());
	}
}
