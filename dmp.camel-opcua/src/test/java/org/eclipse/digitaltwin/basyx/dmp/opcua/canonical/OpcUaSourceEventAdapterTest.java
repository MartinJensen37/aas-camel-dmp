package org.eclipse.digitaltwin.basyx.dmp.opcua.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SourceEventAdapter;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSourceConfiguration;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

class OpcUaSourceEventAdapterTest {
	@Test
	void aSampleContributesItsValueOnly() throws Exception {
		var configuration = new OpcuaDataSourceConfiguration();
		configuration.setNodeInformation("ns=2;s=Machine.State");
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(new DataValue(new Variant("READY")));

		new OpcUaSourceEventAdapter().normalizer(configuration).process(exchange);

		assertEquals("READY", exchange.getMessage().getBody(), "the value only, as an MQTT or HTTP source delivers it");
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY)));
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY)));
	}

	@Test
	void anEventNotificationIsAlreadyItsSelectedFields() throws Exception {
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(Map.of("ToState", "Halted", "Message", "calibration done"));

		new OpcUaSourceEventAdapter().normalizer(new OpcuaDataSourceConfiguration()).process(exchange);

		assertEquals(Map.of("ToState", "Halted", "Message", "calibration done"), exchange.getMessage().getBody());
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY)));
		assertTrue(Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY)));
	}

	@Test
	void badStatusDoesNotContributeToLatestSourceState() throws Exception {
		var configuration = new OpcuaDataSourceConfiguration();
		var exchange = new DefaultExchange(new DefaultCamelContext());
		exchange.getMessage().setBody(new DataValue(new Variant("BAD"), StatusCode.BAD));

		new OpcUaSourceEventAdapter().normalizer(configuration).process(exchange);

		assertEquals(Boolean.FALSE, exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY));
		assertInstanceOf(DataValue.class, exchange.getMessage().getBody());
	}
}
