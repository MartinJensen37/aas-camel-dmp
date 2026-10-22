package dk.aau.smartproductionlab.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.aas4j.v3.model.AasSubmodelElements;
import org.eclipse.digitaltwin.aas4j.v3.model.OperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElementList;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultOperationVariable;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultProperty;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementCollection;
import org.eclipse.digitaltwin.aas4j.v3.model.impl.DefaultSubmodelElementList;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.core.audit.AuditLog;
import dk.aau.smartproductionlab.dmp.core.audit.AuditPoint;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

/** Operation variables declared as collections and lists, nested, in all three categories. */
class OperationStructuredVariableRouteTest {

	private static Property property(String idShort, String valueType) {
		return new DefaultProperty.Builder().idShort(idShort).valueType(Aas.xsd(valueType)).build();
	}

	private static SubmodelElementCollection collection(String idShort, SubmodelElement... children) {
		return new DefaultSubmodelElementCollection.Builder().idShort(idShort).value(List.of(children)).build();
	}

	private static SubmodelElementList intList(String idShort) {
		return new DefaultSubmodelElementList.Builder().idShort(idShort).typeValueListElement(AasSubmodelElements.PROPERTY)
				.valueTypeListElement(Aas.xsd("xs:int")).build();
	}

	// Input: Target {x, y} and Codes [int]; InOut: Plan [{id, speed}]; output: Result {ok, codes [int]}.
	private static final SubmodelElement TARGET = collection("Target", property("x", "xs:double"), property("y", "xs:double"));
	private static final SubmodelElement CODES = intList("Codes");
	private static final SubmodelElement PLAN = new DefaultSubmodelElementList.Builder().idShort("Plan")
			.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
			.value(List.of(collection(null, property("id", "xs:string"), property("speed", "xs:int")))).build();
	private static final SubmodelElement RESULT = collection("Result", property("ok", "xs:boolean"), intList("codes"));

	private static final String REQUEST_LUA = "function aimc_main(sources) local r = sources.Request "
			+ "assert(r.Target.x == 1.5 and r.Target.y == 2, 'typed collection members') "
			+ "assert(#r.Codes == 2 and r.Codes[2] == 4, 'typed Property list') "
			+ "assert(r.Plan[1].id == 'a' and r.Plan[1].speed == 5, 'typed list of collections') "
			+ "return { Command = { sum = r.Target.x + r.Target.y, speed = r.Plan[1].speed } } end";

	private static final String RESPONSE_LUA = "function aimc_main(sources) local d = sources.Device "
			+ "return { Reply = { Plan = { { id = 'a', speed = d.speed + 1 } }, "
			+ "Result = { ok = true, codes = { 7, 8 }, undeclared = 1 } } } end";

	@Test
	void basyxCallerSendsAndReceivesDeclaredStructures() throws Exception {
		String request = Aas.writeList(List.of(
				variable(collection("Target", value(property("x", "xs:double"), "1.5"), value(property("y", "xs:double"), "2"))),
				variable(new DefaultSubmodelElementList.Builder().idShort("Codes").typeValueListElement(AasSubmodelElements.PROPERTY)
						.valueTypeListElement(Aas.xsd("xs:int"))
						.value(List.of(value(property(null, "xs:int"), "3"), value(property(null, "xs:int"), "4"))).build()),
				variable(new DefaultSubmodelElementList.Builder().idShort("Plan")
						.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
						.value(List.of(collection(null, value(property("id", "xs:string"), "a"),
								value(property("speed", "xs:int"), "5")))).build())));
		AtomicReference<Object> command = new AtomicReference<>();
		String reply = invoke(request, command, RESPONSE_LUA);

		assertEquals(Map.of("sum", 3.5, "speed", 5L), command.get());
		List<OperationVariable> variables = Aas.readList(reply, OperationVariable.class);
		assertEquals(List.of("Plan", "Result"), variables.stream().map(v -> v.getValue().getIdShort()).toList());

		var plan = assertInstanceOf(SubmodelElementList.class, variables.get(0).getValue());
		assertEquals(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION, plan.getTypeValueListElement());
		var step = assertInstanceOf(SubmodelElementCollection.class, plan.getValue().get(0));
		assertNull(step.getIdShort(), "AASd-120: list members have no idShort");
		assertEquals(Map.of("id", "a", "speed", "6"), values(step));
		assertEquals("xs:int", Aas.xsd(((Property) step.getValue().get(1)).getValueType()));

		var result = assertInstanceOf(SubmodelElementCollection.class, variables.get(1).getValue());
		assertEquals(List.of("ok", "codes"), result.getValue().stream().map(SubmodelElement::getIdShort).toList(),
				"undeclared members are dropped, declared order is kept");
		assertEquals("true", ((Property) result.getValue().get(0)).getValue());
		var codes = assertInstanceOf(SubmodelElementList.class, result.getValue().get(1));
		assertEquals("xs:int", Aas.xsd(codes.getValueTypeListElement()));
		assertEquals(List.of("7", "8"), codes.getValue().stream().map(item -> ((Property) item).getValue()).toList());
	}

	@Test
	void directCallerReceivesTypedNestedJson() throws Exception {
		AtomicReference<Object> command = new AtomicReference<>();
		String reply = invoke("{\"Target\":{\"x\":1.5,\"y\":2},\"Codes\":[3,4],\"Plan\":[{\"id\":\"a\",\"speed\":5}]}",
				command, RESPONSE_LUA);
		assertEquals(Map.of("sum", 3.5, "speed", 5L), command.get());
		assertEquals(Map.of("Plan", List.of(Map.of("id", "a", "speed", 6L)),
				"Result", Map.of("ok", true, "codes", List.of(7L, 8L), "undeclared", 1L)), JsonCodec.parseJson(reply));
	}

	@Test
	void replyThatContradictsTheDeclaredShapeFails() throws Exception {
		String basyx = basyxRequest();
		String scalarForList = "function aimc_main(sources) return { Reply = { Plan = 'not a list' } } end";
		var reply = JsonCodec.parseJson(invoke(basyx, new AtomicReference<>(), scalarForList, 500));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", ((Map<?, ?>) reply).get("code"));
	}

	@Test
	void callsAndRepliesAreLoggedWithOneInvocationId() throws Exception {
		List<AuditPoint> points = new java.util.concurrent.CopyOnWriteArrayList<>();
		AuditLog capture = points::add;
		AuditLog.install(capture);
		try {
			String request = "{\"Target\":{\"x\":1.5,\"y\":2},\"Codes\":[3,4],\"Plan\":[{\"id\":\"a\",\"speed\":5}]}";
			invoke(request, new AtomicReference<>(), RESPONSE_LUA);
			invoke(basyxRequest(), new AtomicReference<>(), "function aimc_main(sources) return { Reply = { Plan = 'x' } } end", 500);
		} finally {
			AuditLog.uninstall(capture);
		}
		assertEquals(List.of("aas_invoke", "aas_reply", "aas_invoke", "aas_reply"),
				points.stream().map(AuditPoint::measurement).toList());
		AuditPoint invoke = points.get(0), reply = points.get(1), failed = points.get(3);
		assertEquals(Map.of("caller", "rest", "skill", "Move", "restPath", "/structured", "submodel", "urn:cci",
				"path", "Skills.Move.operation"), invoke.tags());
		assertEquals(Map.of("Target", Map.of("x", 1.5, "y", 2L), "Codes", List.of(3L, 4L),
				"Plan", List.of(Map.of("id", "a", "speed", 5L))), JsonCodec.parseJson((String) invoke.fields().get("value_json")));
		assertEquals(invoke.fields().get("invocation"), reply.fields().get("invocation"));
		assertEquals("ok", reply.tags().get("outcome"));
		assertEquals(200L, reply.fields().get("httpStatus"));
		assertEquals("failed", failed.tags().get("outcome"));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", failed.fields().get("errorCode"));
		assertEquals(500L, failed.fields().get("httpStatus"));
		assertEquals(points.get(2).fields().get("invocation"), failed.fields().get("invocation"));
	}

	private static String basyxRequest() {
		return Aas.writeList(List.of(variable(collection("Target", value(property("x", "xs:double"), "1.5"),
				value(property("y", "xs:double"), "2"))), variable(new DefaultSubmodelElementList.Builder().idShort("Codes")
						.typeValueListElement(AasSubmodelElements.PROPERTY).valueTypeListElement(Aas.xsd("xs:int"))
						.value(List.of(value(property(null, "xs:int"), "3"), value(property(null, "xs:int"), "4"))).build()),
				variable(new DefaultSubmodelElementList.Builder().idShort("Plan")
						.typeValueListElement(AasSubmodelElements.SUBMODEL_ELEMENT_COLLECTION)
						.value(List.of(collection(null, value(property("id", "xs:string"), "a"),
								value(property("speed", "xs:int"), "5")))).build())));
	}

	private static OperationVariable variable(SubmodelElement value) {
		return new DefaultOperationVariable.Builder().value(value).build();
	}

	private static Property value(Property property, String value) {
		property.setValue(value);
		return property;
	}

	private static Map<String, String> values(SubmodelElementCollection collection) {
		Map<String, String> out = new java.util.LinkedHashMap<>();
		for (SubmodelElement child : collection.getValue()) out.put(child.getIdShort(), ((Property) child).getValue());
		return out;
	}

	private static String invoke(String body, AtomicReference<Object> command, String responseLua) throws Exception {
		return invoke(body, command, responseLua, 200);
	}

	private static String invoke(String body, AtomicReference<Object> command, String responseLua, int status)
			throws Exception {
		try (var camel = new DefaultCamelContext()) {
			var request = lua(camel, "structured-request", REQUEST_LUA);
			var response = lua(camel, "structured-response", responseLua);
			try {
				camel.addRoutes(new RouteBuilder() {
					@Override public void configure() throws Exception {
						int port;
						try (var probe = new java.net.ServerSocket(0)) { port = probe.getLocalPort(); }
						restConfiguration().component("jetty").host("localhost").port(port).inlineRoutes(false);
						var routes = new RoutesConfiguration(List.of(), List.of(
								new ProtocolBridgeRouteCreatorTest.FixedTransformer("request", "direct:structured-request"),
								new ProtocolBridgeRouteCreatorTest.FixedTransformer("response", "direct:structured-response")),
								List.of(new ProtocolBridgeRouteCreatorTest.FixedSink("command", "direct:structured-device")), List.of());
						var bridge = new ProtocolBridgeRouteConfiguration(new RouteConfiguration(
								"bridge", List.of(), List.of("request"), List.of("command")));
						bridge.setRouteId("structured");
						var data = bridge.getTriggerData();
						data.put("restPath", "/structured");
						data.put("operationSubmodelId", "urn:cci");
						data.put("operationPath", "Skills.Move.operation");
						data.put("action", "Move");
						data.put("operationSourceId", "Request");
						data.put("commandSinkId", "Command");
						data.put("callerReplySinkId", "Reply");
						data.put("explicitResponseMapping", true);
						data.put("responseTransformer", "response");
						data.put("callerResponseTypes", Map.of());
						data.put("callerOutputOrder", List.of("Plan", "Result"));
						data.put("callerInputTemplates", Map.of("Target", Aas.write(TARGET), "Codes", Aas.write(CODES),
								"Plan", Aas.write(PLAN)));
						data.put("callerOutputTemplates", Map.of("Plan", Aas.write(PLAN), "Result", Aas.write(RESULT)));
						data.put("responseSourceBindings", List.of(Map.of("sourceId", "Device")));
						new ProtocolBridgeRouteCreator(this, routes).addRouteToRouteBuilder(bridge);
						from("direct:structured-request").to(request.getConnectionURI());
						from("direct:structured-device").process(exchange -> {
							command.set(exchange.getMessage().getBody());
							exchange.getMessage().setBody(Map.of("speed", 5L));
						});
						from("direct:structured-response").to(response.getConnectionURI());
					}
				});
				camel.start();
				try (var producer = camel.createProducerTemplate()) {
					var result = producer.request("direct:structured",
							exchange -> exchange.getMessage().setBody(body));
					assertNull(result.getException());
					assertEquals(status, result.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
					return result.getMessage().getBody(String.class);
				}
			} finally {
				camel.stop();
				request.close();
				response.close();
			}
		}
	}

	private static LuaTransformerConfiguration lua(DefaultCamelContext camel, String id, String source) throws Exception {
		var lua = new LuaTransformerConfiguration(id, LuaCompiledSource.fromBytes(source.getBytes(StandardCharsets.UTF_8), id, 4096));
		lua.setKeyedInput(true);
		lua.bind(camel);
		return lua;
	}
}
