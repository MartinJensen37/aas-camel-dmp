/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.plc4x.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.ExchangePattern;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Plc4xDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.plc4x.configuration.Tag;
import dk.aau.smartproductionlab.dmp.plc4x.testing.ModbusTestServer;
import org.junit.jupiter.api.Test;

/** {@code dmp-plc4x:} writes and reads against a real Modbus TCP socket. */
class DmpPlc4xComponentTest {
	private static String uri(int port, String tag, String address) {
		return new Plc4xDataSinkConfiguration("sink", "127.0.0.1", port, "modbus-tcp", "", List.of(),
				List.of(new Tag(tag, address))).getConnectionURI();
	}

	@Test
	void writesTheBodyToTheEndpointsOneTagAndAnswersARequestWithAnEmptyReply() throws Exception {
		try (ModbusTestServer device = new ModbusTestServer(); DefaultCamelContext camel = new DefaultCamelContext()) {
			camel.start();
			var template = camel.createProducerTemplate();
			template.sendBody(uri(device.port(), "Start", "holding-register:5"), 42);
			Object reply = template.sendBody(uri(device.port(), "Start", "holding-register:5"), ExchangePattern.InOut,
					Map.of("Start", 43));
			assertEquals(List.of("register:4=42", "register:4=43"), device.writes(), "a value, or a value per tag");
			assertEquals(Map.of(), reply);
		}
	}

	@Test
	void aWriteToAnUnreachableDeviceFails() throws Exception {
		int port;
		try (ModbusTestServer device = new ModbusTestServer()) { port = device.port(); }
		try (DefaultCamelContext camel = new DefaultCamelContext()) {
			camel.start();
			var failure = assertThrows(CamelExecutionException.class,
					() -> camel.createProducerTemplate().sendBody(uri(port, "Start", "coil:1"), true));
			var cause = assertInstanceOf(ConnectorInvocationException.class, failure.getCause());
			assertEquals(ConnectorInvocationException.Category.TRANSPORT, cause.getCategory());
		}
	}

	@Test
	void pollEnrichReadsTheTagValueAndARejectedReadFails() throws Exception {
		try (ModbusTestServer device = new ModbusTestServer(); DefaultCamelContext camel = new DefaultCamelContext()) {
			String endpoint = uri(device.port(), "Done", "holding-register:2");
			camel.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					from("direct:read").pollEnrich(endpoint, 3000);
				}
			});
			camel.start();
			device.register(1, 7);
			var template = camel.createProducerTemplate();
			assertEquals(7, ((Number) template.requestBody("direct:read", (Object) null)).intValue());
			device.failReads(true);
			var failure = assertThrows(CamelExecutionException.class, () -> template.requestBody("direct:read", (Object) null));
			// pollEnrich reports "Error during poll" with the connector's failure as its cause.
			assertEquals(ConnectorInvocationException.Category.REMOTE,
					assertInstanceOf(ConnectorInvocationException.class, failure.getCause().getCause()).getCategory());
		}
	}

	@Test
	void driverOptionsBecomePartOfTheConnectionString() throws Exception {
		try (DefaultCamelContext camel = new DefaultCamelContext()) {
			camel.start();
			var endpoint = camel.getEndpoint("dmp-plc4x:modbus-tcp://127.0.0.1:502?tag.Done=holding-register:2"
					+ "&unit-identifier=2&autoReconnect=true", DmpPlc4xEndpoint.class);
			assertEquals("modbus-tcp://127.0.0.1:502?unit-identifier=2", endpoint.getUri());
			assertEquals(Map.of("Done", "holding-register:2"), endpoint.getTags());
			assertEquals(true, endpoint.isAutoReconnect());
		}
	}
}
