package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

class OperationCallerTypeRouteTest {
	@Test
	void directCallerUsesCciTypesAfterDeviceNormalizationAndOneResponseLua() throws Exception {
		assertCallerReply(false);
	}

	@Test
	void basyxCallerUsesCciNamesTypesAndOrderAfterDeviceNormalizationAndOneResponseLua() throws Exception {
		assertCallerReply(true);
	}

	private void assertCallerReply(boolean basyx) throws Exception {
		AtomicInteger sends = new AtomicInteger();
		AtomicInteger responseTransforms = new AtomicInteger();
		try (var camel = new DefaultCamelContext()) {
			var lua = new LuaTransformerConfiguration("caller-types", LuaCompiledSource.fromBytes((
					"function aimc_main(sources) "
					+ "assert(type(sources.Device.Count) == 'number') "
					+ "return { Reply = { Count = string.format('%03d', sources.Device.Count), "
					+ "Display = sources.Device.Count / 2, Flag = sources.Device.DeviceFlag == 'true' } } end")
					.getBytes(StandardCharsets.UTF_8), "caller-types", 4096));
			lua.bind(camel);
			try {
				camel.addRoutes(new RouteBuilder() {
					@Override public void configure() throws Exception {
						int port;
						try (var probe = new java.net.ServerSocket(0)) { port = probe.getLocalPort(); }
						restConfiguration().component("jetty").host("localhost").port(port).inlineRoutes(false);
						var routes = new RoutesConfiguration(List.of(), List.of(
								new ProtocolBridgeRouteCreatorTest.FixedTransformer("request", "direct:types-request"),
								new ProtocolBridgeRouteCreatorTest.FixedTransformer("response", "direct:types-response")),
								List.of(new ProtocolBridgeRouteCreatorTest.FixedSink("command", "direct:types-device")), List.of());
						var bridge = new ProtocolBridgeRouteConfiguration(new RouteConfiguration(
								"bridge", List.of(), List.of("request"), List.of("command")));
						bridge.setRouteId("caller-types");
						var data = bridge.getTriggerData();
						data.put("restPath", "/caller-types");
						data.put("action", "Invoke");
						data.put("callerInputSourcePaths", Map.of("Request", "$"));
						data.put("commandSinkIds", List.of("command"));
						data.put("commandSinkBindings", List.of(Map.of("sinkId", "Command", "dataSinkId", "command")));
						data.put("callerReplySinkId", "Reply");
						data.put("explicitResponseMapping", true);
						data.put("responseTransformer", "response");
						data.put("responseTypes", Map.of("Count", "integer", "DeviceFlag", "string"));
						data.put("callerResponseTypes", Map.of("Count", "xs:string", "Display", "xs:double", "Flag", "xs:boolean"));
						data.put("callerOutputOrder", List.of("Flag", "Count", "Display"));
						data.put("responseSourceBindings", List.of(Map.of("sourceId", "Device")));
						new ProtocolBridgeRouteCreator(this, routes).addRouteToRouteBuilder(bridge);
						from("direct:types-request").setBody().constant(Map.of("Command", Map.of("argument", 1L)));
						from("direct:types-device").process(exchange -> {
							sends.incrementAndGet();
							// Simulated connector reply already normalized to the AID's device types.
							exchange.getMessage().setBody(Map.of("Count", 7L, "DeviceFlag", "true"));
						});
						from("direct:types-response").process(exchange -> responseTransforms.incrementAndGet())
								.to(lua.getConnectionURI());
					}
				});
				camel.start();
				try (var producer = camel.createProducerTemplate()) {
					var result = producer.request("direct:caller-types", exchange -> exchange.getMessage().setBody(
							basyx ? "[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"argument\",\"valueType\":\"xs:int\",\"value\":\"1\"}}]" : "{\"argument\":1}"));
					assertNull(result.getException());
					assertEquals(200, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
					Object expected = basyx ? List.of(
							Map.of("value", Map.of("modelType", "Property", "idShort", "Flag", "valueType", "xs:boolean", "value", "true")),
							Map.of("value", Map.of("modelType", "Property", "idShort", "Count", "valueType", "xs:string", "value", "007")),
							Map.of("value", Map.of("modelType", "Property", "idShort", "Display", "valueType", "xs:double", "value", "3.5")))
							: Map.of("Count", "007", "Display", 3.5, "Flag", true);
					assertEquals(expected, JsonCodec.parseJson(result.getMessage().getBody(String.class)));
				}
				assertEquals(1, sends.get());
				assertEquals(1, responseTransforms.get());
			} finally {
				camel.stop();
				lua.close();
			}
		}
	}
}
