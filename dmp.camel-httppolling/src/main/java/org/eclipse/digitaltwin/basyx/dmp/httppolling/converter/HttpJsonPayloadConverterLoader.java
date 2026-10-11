package org.eclipse.digitaltwin.basyx.dmp.httppolling.converter;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.TypeConverterLoaderException;
import org.apache.camel.spi.TypeConverterLoader;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.support.SimpleTypeConverter;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;

/** Registers JSON value-tree conversion only for the HTTP entity stream boundary. */
public final class HttpJsonPayloadConverterLoader implements TypeConverterLoader, CamelContextAware {
	private CamelContext camelContext;

	@Override
	public void setCamelContext(CamelContext camelContext) { this.camelContext = camelContext; }

	@Override
	public CamelContext getCamelContext() { return camelContext; }

	@Override
	public void load(TypeConverterRegistry registry) throws TypeConverterLoaderException {
		registry.addTypeConverter(InputStream.class, Map.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> HttpJsonPayloadConverter.mapToInputStream((Map<?, ?>) value)));
		registry.addTypeConverter(InputStream.class, List.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> HttpJsonPayloadConverter.listToInputStream((List<?>) value)));
		registry.addTypeConverter(InputStream.class, Number.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> HttpJsonPayloadConverter.numberToInputStream((Number) value)));
		registry.addTypeConverter(InputStream.class, Boolean.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> HttpJsonPayloadConverter.booleanToInputStream((Boolean) value)));
		registry.addTypeConverter(InputStream.class, JsonPointer.JSON_NULL.getClass(), new SimpleTypeConverter(false,
				(type, exchange, value) -> HttpJsonPayloadConverter.jsonNullToInputStream(value)));
		registry.addTypeConverter(InputStream.class, ConnectorJsonPayload.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> HttpJsonPayloadConverter.jsonPayloadToInputStream((ConnectorJsonPayload) value)));
	}
}
