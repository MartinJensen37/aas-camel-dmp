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

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.spi.TypeConverterLoader;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.support.SimpleTypeConverter;
import org.apache.camel.support.SimpleTypeConverter.ConversionMethod;
import org.apache.camel.TypeConverterLoaderException;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;

/**
 * Registers the canonical OPC UA type converters with Camel 4's type converter
 * registry.
 *
 * <p>
 * Camel 4 no longer scans {@code META-INF/services/org/apache/camel/TypeConverter}
 * files; converters are registered programmatically by a
 * {@link TypeConverterLoader} referenced from
 * {@code META-INF/services/org/apache/camel/TypeConverterLoader} (same
 * mechanism as camel-milo's {@code ValueConverterLoader}).
 *
 * @author aas-camel-dmp
 */
public final class OpcUaCanonicalConverterLoader implements TypeConverterLoader, CamelContextAware {

	private CamelContext camelContext;

	public OpcUaCanonicalConverterLoader() {
	}

	@Override
	public void setCamelContext(CamelContext camelContext) {
		this.camelContext = camelContext;
	}

	@Override
	public CamelContext getCamelContext() {
		return camelContext;
	}

	@Override
	public void load(TypeConverterRegistry registry) throws TypeConverterLoaderException {
		registerConverters(registry);
	}

	private void registerConverters(TypeConverterRegistry registry) {
		addTypeConverter(registry, String.class, DataValue.class, false,
				(type, exchange, value) -> OpcUaCanonicalConverter.toCanonicalJson((DataValue) value, exchange));
		addTypeConverter(registry, java.util.Map.class, DataValue.class, false,
				(type, exchange, value) -> OpcUaCanonicalConverter.toCanonicalMap((DataValue) value, exchange));
	}

	private static void addTypeConverter(TypeConverterRegistry registry, Class<?> toType, Class<?> fromType,
			boolean allowNull, ConversionMethod method) {
		registry.addTypeConverter(toType, fromType, new SimpleTypeConverter(allowNull, method));
	}
}
