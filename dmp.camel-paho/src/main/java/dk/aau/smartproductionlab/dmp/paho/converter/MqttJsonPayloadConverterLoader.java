package dk.aau.smartproductionlab.dmp.paho.converter;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.TypeConverterLoaderException;
import org.apache.camel.spi.TypeConverterLoader;
import org.apache.camel.spi.TypeConverterRegistry;
import org.apache.camel.support.SimpleTypeConverter;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import dk.aau.smartproductionlab.dmp.core.util.ConnectorJsonPayload;

/** Registers JSON value-tree conversion only for the Paho byte payload boundary. */
public final class MqttJsonPayloadConverterLoader implements TypeConverterLoader, CamelContextAware {
	private CamelContext camelContext;

	@Override
	public void setCamelContext(CamelContext camelContext) { this.camelContext = camelContext; }

	@Override
	public CamelContext getCamelContext() { return camelContext; }

	@Override
	public void load(TypeConverterRegistry registry) throws TypeConverterLoaderException {
		registry.addTypeConverter(byte[].class, Map.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> MqttJsonPayloadConverter.mapToBytes((Map<?, ?>) value)));
		registry.addTypeConverter(byte[].class, List.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> MqttJsonPayloadConverter.listToBytes((List<?>) value)));
		registry.addTypeConverter(byte[].class, Number.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> MqttJsonPayloadConverter.numberToBytes((Number) value)));
		registry.addTypeConverter(byte[].class, Boolean.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> MqttJsonPayloadConverter.booleanToBytes((Boolean) value)));
		registry.addTypeConverter(byte[].class, JsonPointer.JSON_NULL.getClass(), new SimpleTypeConverter(false,
				(type, exchange, value) -> MqttJsonPayloadConverter.jsonNullToBytes(value)));
		registry.addTypeConverter(byte[].class, ConnectorJsonPayload.class, new SimpleTypeConverter(false,
				(type, exchange, value) -> MqttJsonPayloadConverter.jsonPayloadToBytes((ConnectorJsonPayload) value)));
	}
}
