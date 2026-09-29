/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.opcua.canonical;

import java.util.Base64;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Canonical OPC UA payload forms (SPEC §10.6, ADR-007 — normalize at the
 * protocol boundary).
 *
 * <p>
 * The canonical JSON value handed to the Lua blob for an OPC UA source is:
 *
 * <pre>
 * { "nodeId": &lt;id&gt;, "value": &lt;typed&gt;, "status": &lt;code&gt;, "timestamp": &lt;time&gt; }
 * </pre>
 *
 * For the write direction, a canonical JSON object ({@code {"value": …}}) is
 * converted back into a Milo {@link Variant} so the milo-client producer can
 * write it (SPEC §10.2, MQTT → OPC UA commands).
 *
 * @author aas-camel-dmp
 */
public final class OpcUaCanonical {

	/** Exchange header carrying the node id (used by the canonical converters). */
	public static final String NODE_ID_HEADER = "CamelMiloNodeId";
	/** Explicit opt-in for decoding the canonical envelope at a route boundary. */
	public static final String CANONICAL_HEADER = "CamelOpcUaCanonical";

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private OpcUaCanonical() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Converts a Milo {@link DataValue} to the canonical JSON map.
	 *
	 * @param value  the monitored/read value
	 * @param nodeId node identifier or {@code null} (optional metadata)
	 */
	public static Map<String, Object> toCanonical(DataValue value, String nodeId) {
		Map<String, Object> canonical = new LinkedHashMap<>();
		if (nodeId != null) {
			canonical.put("nodeId", nodeId);
		}

		Variant variant = value.getValue();
		canonical.put("value", sanitize(variant != null ? variant.getValue() : null));

		StatusCode status = value.getStatusCode();
		canonical.put("status", status != null ? status.getValue() : null);

		DateTime sourceTime = value.getSourceTime();
		DateTime serverTime = value.getServerTime();
		DateTime time = sourceTime != null && !sourceTime.isNull() ? sourceTime : serverTime;
		canonical.put("timestamp", time != null && !time.isNull() ? time.getJavaInstant().toString() : null);

		return canonical;
	}

	/** Serializes a canonical map to JSON. */
	public static String toCanonicalJson(DataValue value, String nodeId) {
		try {
			return MAPPER.writeValueAsString(toCanonical(value, nodeId));
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Cannot serialize OPC UA value to canonical JSON", e);
		}
	}

	/**
	 * Produces the Milo {@link Variant} a milo-client producer writes from a
	 * canonical {@code value} (SPEC §10.6).
	 */
	public static Variant toVariant(Object canonicalValue) {
		return canonicalValue == null ? Variant.ofNull() : new Variant(canonicalValue);
	}

	/**
	 * Keeps JSON-friendly values as-is; binary payloads become base64 strings.
	 */
	static Object sanitize(Object raw) {
		if (raw == null) {
			return null;
		}
		if (raw instanceof Number || raw instanceof Boolean || raw instanceof String) {
			return raw;
		}
		if (raw instanceof byte[]) {
			return Base64.getEncoder().encodeToString((byte[]) raw);
		}
		if (raw instanceof Map<?, ?> map) {
			Map<String, Object> copy = new LinkedHashMap<>();
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				copy.put(String.valueOf(entry.getKey()), sanitize(entry.getValue()));
			}
			return copy;
		}
		if (raw instanceof Collection<?> collection) {
			java.util.List<Object> copy = new java.util.ArrayList<>(collection.size());
			for (Object value : collection) {
				copy.add(sanitize(value));
			}
			return copy;
		}
		if (raw.getClass().isArray()) {
			int length = Array.getLength(raw);
			java.util.List<Object> copy = new java.util.ArrayList<>(length);
			for (int i = 0; i < length; i++) {
				copy.add(sanitize(Array.get(raw, i)));
			}
			return copy;
		}
		return raw.toString();
	}
}
