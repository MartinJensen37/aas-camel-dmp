package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;

/** Protocol-neutral assembly of SinkId values into one declared object payload. */
public final class PayloadAssembler {
	private PayloadAssembler() {}

	public static Object assemble(Map<String, Object> sinkValues, List<String> sinkIds,
			Map<String, String> valuePaths) {
		if (sinkIds == null || sinkIds.isEmpty()) return sinkValues;
		Map<String, Object> result = new LinkedHashMap<>();
		for (String sinkId : sinkIds) {
			if (!sinkValues.containsKey(sinkId)) {
				throw new IllegalArgumentException("mapping result has no value for SinkId " + sinkId);
			}
			Object value = sinkValues.get(sinkId);
			String path = valuePaths == null ? null : valuePaths.get(sinkId);
			if (path == null || path.isBlank()) {
				if (sinkIds.size() != 1) {
					throw new IllegalArgumentException("multiple SinkIds require distinct member valuePaths");
				}
				return value;
			}
			putAt(result, JsonPointer.parse(path).tokens(), value);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private static void putAt(Map<String, Object> root, List<String> tokens, Object value) {
		if (tokens.isEmpty()) {
			if (!(value instanceof Map<?, ?> map)) {
				throw new IllegalArgumentException("root contribution must be an object");
			}
			mergeInto(root, (Map<String, Object>) map);
			return;
		}
		Map<String, Object> current = root;
		for (int i = 0; i < tokens.size(); i++) {
			String token = tokens.get(i);
			if (token.startsWith("[") && token.endsWith("]")) {
				throw new IllegalArgumentException("array-index grouped valuePaths are unsupported");
			}
			if (i == tokens.size() - 1) {
				if (current.containsKey(token)) mergeValue(current.get(token), value, token);
				else current.put(token, value);
			} else {
				Object child = current.computeIfAbsent(token, ignored -> new LinkedHashMap<String, Object>());
				if (!(child instanceof Map<?, ?>)) {
					throw new IllegalArgumentException("overlapping valuePaths at " + token);
				}
				current = (Map<String, Object>) child;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static void mergeValue(Object existing, Object incoming, String key) {
		if (existing instanceof Map<?, ?> left && incoming instanceof Map<?, ?> right) {
			mergeInto((Map<String, Object>) left, (Map<String, Object>) right);
		} else if (!java.util.Objects.equals(existing, incoming)) {
			throw new IllegalArgumentException("conflicting values at " + key);
		}
	}

	private static void mergeInto(Map<String, Object> target, Map<String, Object> source) {
		source.forEach((key, value) -> {
			if (target.containsKey(key)) mergeValue(target.get(key), value, key);
			else target.put(key, value);
		});
	}
}
