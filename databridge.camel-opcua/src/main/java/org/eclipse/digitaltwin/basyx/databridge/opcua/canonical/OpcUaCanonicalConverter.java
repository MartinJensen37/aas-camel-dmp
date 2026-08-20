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
package org.eclipse.digitaltwin.basyx.databridge.opcua.canonical;

import java.util.Map;

import org.apache.camel.Converter;
import org.apache.camel.Exchange;
import org.apache.camel.TypeConverters;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;

/**
 * Camel type converters (SPEC §10.1 / §10.6) that normalize a Milo
 * {@link DataValue} into the canonical JSON form consumed by the Lua blob.
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

	private static String nodeIdOf(Exchange exchange) {
		if (exchange == null) {
			return null;
		}
		return exchange.getMessage().getHeader(OpcUaCanonical.NODE_ID_HEADER, String.class);
	}
}
