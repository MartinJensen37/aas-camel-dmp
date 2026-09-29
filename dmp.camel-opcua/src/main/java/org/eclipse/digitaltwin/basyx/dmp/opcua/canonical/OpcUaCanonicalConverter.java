/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.opcua.canonical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Converter;
import org.apache.camel.Exchange;
import org.apache.camel.TypeConverters;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodResult;
import org.eclipse.milo.opcua.stack.core.types.structured.WriteResponse;

/**
 * Camel type converters (SPEC §10.1 / §10.6) that normalize Milo
 * {@link DataValue} — and the awaited UA Call/Write results — into the
 * canonical JSON form consumed by the Lua blob.
 *
 * <p>
 * The milo-client consumer puts the raw {@link DataValue} on the message body.
 * When the Lua component (or any other consumer) requests a {@link String} or
 * {@code Map<String,Object>} body, Camel invokes these converters
 * automatically — so a DMP route
 * {@code from("milo-client:...").to("lua:...")} needs no extra processor to
 * see the canonical payload.
 *
 * <p>
 * The awaited method-call path (CamelMiloAwait) leaves the raw
 * {@link CallMethodResult} on the body; the canonical map carries the transport
 * status plus the POSITIONAL output arguments, which the operation processors
 * zip with the DECLARED response types.
 *
 * <p>
 * Registration is handled by {@link OpcUaCanonicalConverterLoader} (referenced
 * from {@code META-INF/services/org/apache/camel/TypeConverterLoader}). The
 * {@code @Converter} annotations and {@link TypeConverters} marker mirror the
 * camel-milo {@code ValueConverter} pattern for the camel build tools.
 *
 * @author aas-camel-dmp
 */
public final class OpcUaCanonicalConverter implements TypeConverters {

	/**
	 * Camel instantiates converter classes from the registry; a public no-arg
	 * constructor is required (the conversion methods are static).
	 */
	public OpcUaCanonicalConverter() {
	}

	/**
	 * {@code DataValue -> Map<String,Object>} with the canonical fields
	 * {@code value}, {@code status}, {@code timestamp} and — when the exchange
	 * carries a {@code CamelMiloNodeId} header — {@code nodeId}.
	 */
	@Converter
	public static Map<String, Object> toCanonicalMap(DataValue value, Exchange exchange) {
		return OpcUaCanonical.toCanonical(value, nodeIdOf(exchange));
	}

	/**
	 * {@code DataValue -> String} — the canonical JSON blob input (SPEC §10.6).
	 */
	@Converter
	public static String toCanonicalJson(DataValue value, Exchange exchange) {
		return OpcUaCanonical.toCanonicalJson(value, nodeIdOf(exchange));
	}

	/**
	 * {@code CallMethodResult -> Map<String,Object>} — the awaited UA Call
	 * result: the transport status plus the POSITIONAL output arguments —
	 * {@code {"statusCode": {"value": <long>, "good": <bool>},
	 * "outputArguments": [<java value>...], "inputArgumentResults":
	 * [<long>...]}}. The operation processors zip the output arguments with
	 * the DECLARED response types.
	 */
	@Converter
	public static Map<String, Object> toCanonicalMap(CallMethodResult result, Exchange exchange) {
		Map<String, Object> canonical = new LinkedHashMap<>();
		StatusCode status = result.getStatusCode();
		if (status != null) {
			Map<String, Object> statusEntry = new LinkedHashMap<>();
			statusEntry.put("value", status.getValue());
			statusEntry.put("good", status.isGood());
			canonical.put("statusCode", statusEntry);
		}
		List<Object> outputs = new ArrayList<>();
		Variant[] outputArguments = result.getOutputArguments();
		if (outputArguments != null) {
			for (Variant argument : outputArguments) {
				outputs.add(valueToJava(argument));
			}
		}
		canonical.put("outputArguments", outputs);
		List<Object> argumentResults = new ArrayList<>();
		StatusCode[] inputArgumentResults = result.getInputArgumentResults();
		if (inputArgumentResults != null) {
			for (StatusCode argumentResult : inputArgumentResults) {
				argumentResults.add(argumentResult == null ? null : argumentResult.getValue());
			}
		}
		canonical.put("inputArgumentResults", argumentResults);
		return canonical;
	}

	/**
	 * {@code WriteResponse -> Map<String,Object>} — the awaited UA node write
	 * result: the per-node status codes — {@code {"results": [<long>...]}}.
	 * A WRITE-status reply is not a value; the write-delegation processor
	 * falls back to the written (accepted) command for the effective value.
	 */
	@Converter
	public static Map<String, Object> toCanonicalMap(WriteResponse response, Exchange exchange) {
		Map<String, Object> canonical = new LinkedHashMap<>();
		List<Object> results = new ArrayList<>();
		StatusCode[] statuses = response.getResults();
		if (statuses != null) {
			for (StatusCode status : statuses) {
				results.add(status == null ? null : status.getValue());
			}
		}
		canonical.put("results", results);
		return canonical;
	}

	/**
	 * The Java value of one UA Variant: numbers, strings and booleans ride
	 * as-is, the common builtin wrappers are unwrapped to their Java form so
	 * the Lua blob sees plain JSON values.
	 */
	static Object valueToJava(Variant variant) {
		if (variant == null) {
			return null;
		}
		Object value = variant.getValue();
		if (value == null) {
			return null;
		}
		if (value instanceof StatusCode statusCode) {
			return statusCode.getValue();
		}
		if (value instanceof byte[] bytes) {
			return java.util.Base64.getEncoder().encodeToString(bytes);
		}
		return value;
	}

	private static String nodeIdOf(Exchange exchange) {
		if (exchange == null) {
			return null;
		}
		return exchange.getMessage().getHeader(OpcUaCanonical.NODE_ID_HEADER, String.class);
	}
}