package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

/**
 * The reply leg must honour the declared types exactly as the request leg does.
 *
 * <p>The request leg already stringifies a correlation Uuid before it reaches
 * the device (native stations validate {@code Uuid} against a string schema),
 * so the reply leg has to agree: a device echoing {@code "1"} for a
 * {@code xs:string} Uuid must get {@code "1"} back, not {@code 1.0}.
 */
class OperationReplyProcessorTest {

	/** Field order carries no meaning in a JSON reply, so compare structure. */
	private static void assertJson(String expected, Object actual) {
		assertEquals(JsonCodec.parseJson(expected), envelope(actual));
	}

	/** The reply as the caller receives it: OperationVariables in AAS JSON, any other value as JSON. */
	private static Object envelope(Object body) {
		return JsonCodec.parseJson(OperationPayloads.encodeReply(body));
	}

	private static Exchange exchangeWith(Object body) {
		DefaultCamelContext context = new DefaultCamelContext();
		Exchange exchange = new DefaultExchange(context);
		exchange.getMessage().setBody(body);
		return exchange;
	}

	private static OperationContext context(Map<String, String> responseTypes) {
		return new OperationContext("Occupy", "req.lua", "res.lua", "mock:cmd",
				Map.of(), responseTypes);
	}

	@Test
	void aNumericLookingUuidComesBackAsAString() {
		// what the station actually sends back after a stringified Uuid
		Exchange exchange = exchangeWith(Map.of("State", "SUCCESS", "Uuid", "1",
				"TimeStamp", "2026-09-30T07:43:32.365Z"));

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("State", "string"))))
				.process(exchange);

		assertJson("{\"State\":\"SUCCESS\",\"Uuid\":\"1\","
				+ "\"TimeStamp\":\"2026-09-30T07:43:32.365Z\"}", exchange.getMessage().getBody());
		assertEquals("1", String.valueOf(((Map<?, ?>) exchange.getMessage().getBody()).get("Uuid")),
				"a numeric-looking string Uuid must not come back as a double");
	}

	@Test
	void aNonNumericFieldIsUnaffected() {
		Exchange exchange = exchangeWith(Map.of("State", "SUCCESS", "Uuid", "1"));

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("State", "string"))))
				.process(exchange);

		assertJson("{\"State\":\"SUCCESS\",\"Uuid\":\"1\"}", exchange.getMessage().getBody());
	}

	@Test
	void anIntegralNumberStaysIntegral() {
		Exchange exchange = exchangeWith(Map.of("count", 858383));

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("count", "integer"))))
				.process(exchange);

		assertEquals(858383L, ((Number) ((Map<?, ?>) exchange.getMessage().getBody()).get("count")).longValue(),
				"an integral count must not become 858383.0");
	}

	@Test
	void aGenuineFractionKeepsItsFractionalForm() {
		Exchange exchange = exchangeWith(Map.of("v", 103.5585973));

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("v", "number"))))
				.process(exchange);

		assertEquals(103.5585973, ((Number) ((Map<?, ?>) exchange.getMessage().getBody()).get("v")).doubleValue());
	}

	@Test
	void directScalarArrayAndNullRepliesKeepTheirJsonTypes() throws Exception {
		for (Object body : java.util.Arrays.asList("READY", List.of(1L, 2L), null)) {
			Exchange exchange = exchangeWith(body);
			new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("State", "string"))))
					.process(exchange);
			assertEquals(body, exchange.getMessage().getBody());
		}
	}

	@Test
	void wholeCommandPayloadIsNotUnwrappedByItsOwnMemberName() {
		Map<String, Object> command = OperationPayloads.assembleCommand(
				Map.of("Expected", Map.of("Expected", "inner", "sibling", 1L)),
				List.of("Expected"), Map.of(), "Expected");
		assertEquals(Map.of("Expected", "inner", "sibling", 1L), command);
	}

	@Test
	void untransformedNativeReplyIsNotAnEnvelopeEvenWhenItsOnlyFieldMatchesTheAction() {
		OperationContext ctx = new OperationContext("Invoke", "req.lua", null, "mock:cmd");
		for (Object body : java.util.Arrays.asList(
				Map.of("Invoke", Map.of("x", 1L), "sibling", "001"),
				Map.of("Invoke", List.of(false, 2L, "002")),
				Map.of("Invoke", Map.of("Invoke", true)),
				java.util.Collections.singletonMap("Invoke", null),
				List.of(false, 2L, "002"), "001", null)) {
			Exchange exchange = exchangeWith(body);
			new OperationReplyProcessor(ctx).process(exchange);
			assertEquals(body, exchange.getMessage().getBody());
			assertEquals(200, exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
		}
	}

	@Test
	void untransformedNativeReplyKeepsActionNamedAndSiblingBasyxOutputs() {
		OperationContext ctx = new OperationContext("Invoke", "req.lua", null, "mock:cmd",
				Map.of(), Map.of("Invoke", "xs:string", "State", "xs:boolean"), List.of("State", "Invoke"),
				"Request", "Command", "Invoke", List.of("Command"), Map.of(), false,
				List.of(), List.of(), Map.of());
		Exchange exchange = exchangeWith(Map.of("Invoke", "001", "State", true));
		exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, true);
		new OperationReplyProcessor(ctx).process(exchange);
		assertJson("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\","
				+ "\"valueType\":\"xs:boolean\",\"value\":\"true\"}},{\"value\":{\"modelType\":\"Property\","
				+ "\"idShort\":\"Invoke\",\"valueType\":\"xs:string\",\"value\":\"001\"}}]",
				exchange.getMessage().getBody());
	}

	@Test
	void selectedWholeReplyPreservesMemberNamedLikeTheReplySink() {
		for (boolean explicit : List.of(false, true)) {
			OperationContext ctx = new OperationContext("Invoke", "req.lua", "res.lua", "mock:cmd",
					Map.of(), Map.of(), List.of(), "Request", "Command", "Reply", List.of("Command"),
					Map.of(), explicit, List.of(), List.of(), Map.of());
			Map<String, Object> whole = Map.of("Reply", Map.of("x", 1L), "sibling", 2L);
			Exchange exchange = exchangeWith(Map.of("Reply", whole, "Audit", "not caller output"));
			new OperationReplyProcessor(ctx).process(exchange);
			assertEquals(whole, exchange.getMessage().getBody());
		}
	}

	@Test
	void explicitNullCallerReplyIsPresentButMissingReplyFails() {
		OperationContext ctx = new OperationContext("Invoke", "req.lua", "res.lua", "mock:cmd",
				Map.of(), Map.of(), List.of(), "Request", "Command", "Reply", List.of("Command"),
				Map.of(), true, List.of(), List.of(), Map.of());
		Map<String, Object> values = new LinkedHashMap<>();
		values.put("Reply", null);
		Exchange exchange = exchangeWith(values);
		new OperationReplyProcessor(ctx).process(exchange);
		assertNull(exchange.getMessage().getBody());
		OperationValidationException failure = assertThrows(OperationValidationException.class,
				() -> new OperationReplyProcessor(ctx).process(exchangeWith(Map.of("Audit", "ignored"))));
		assertEquals("DMP_RESPONSE_MAPPING_FAILED", failure.getCode());
	}

	@Test
	void basyxScalarReplyStillUsesDeclaredOutputEnvelope() {
		Exchange exchange = exchangeWith("READY");
		exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, true);
		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("State", "string"))))
				.process(exchange);
		assertJson("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\","
				+ "\"valueType\":\"xs:string\",\"value\":\"READY\"}}]",
				exchange.getMessage().getBody());
	}

	@Test
	void basyxUuidInoutputPrecedesOutputsAndKeepsLexicalStringAndNull() {
		Map<String, Object> reply = new LinkedHashMap<>();
		reply.put("State", null);
		reply.put("Uuid", "00000000-0000-0000-0000-000000000001");
		OperationContext ctx = new OperationContext("Occupy", "req.lua", null, "mock:cmd",
				Map.of(), Map.of(), List.of("Uuid", "State"), "Request", "Command", "Occupy",
				List.of("Command"), Map.of(), false, List.of(), List.of(), Map.of(),
				List.of("req.lua"), Map.of(), Map.of("Uuid", "xs:string", "State", "xs:string"));
		Exchange exchange = exchangeWith(reply);
		exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, true);
		new OperationReplyProcessor(ctx).process(exchange);
		assertJson("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Uuid\","
				+ "\"valueType\":\"xs:string\",\"value\":\"00000000-0000-0000-0000-000000000001\"}},"
				+ "{\"value\":{\"modelType\":\"Property\",\"idShort\":\"State\","
				+ "\"valueType\":\"xs:string\"}}]",
				exchange.getMessage().getBody());
	}

	@Test
	void declaredOutputTypesStillApply() {
		Exchange exchange = exchangeWith(Map.of("State", 1));
		OperationContext ctx = context(new LinkedHashMap<>(Map.of("State", "string")));

		new OperationReplyProcessor(ctx).process(exchange);

		assertEquals(Map.of("State", "1"), exchange.getMessage().getBody(),
				"a declared xs:string output must be stringified in the reply too");
	}

	@Test
	void theSharedCodecKeepsTheNumericContractInOnePlace() {
		assertEquals("{\"a\":1}", org.eclipse.digitaltwin.basyx.dmp.core.util
				.JsonCodec.toJson(org.eclipse.digitaltwin.basyx.dmp.core.util
						.JsonCodec.parseJson("{\"a\":1}")));
		assertEquals(List.of(1L, 2L), org.eclipse.digitaltwin.basyx.dmp.core.util
				.JsonCodec.parseJson("{\"a\":[1,2]}") instanceof Map
						? ((Map<?, ?>) org.eclipse.digitaltwin.basyx.dmp.core.util
								.JsonCodec.parseJson("{\"a\":[1,2]}")).get("a")
						: null);
	}

	private static OperationContext separateTypes(Map<String, String> callerTypes, List<String> order) {
		return new OperationContext("Invoke", "req.lua", "res.lua", "mock:cmd",
				Map.of(), Map.of("Count", "integer", "DeviceFlag", "string"), order,
				"Request", "Command", "Reply", List.of("Command"), Map.of(), true,
				List.of(), List.of(), Map.of(), List.of("req.lua"), Map.of(), callerTypes);
	}

	@Test
	void callerTypesAndOrderAreIndependentOfSameNameAndDifferentlyNamedDeviceOutputs() {
		Map<String, String> callerTypes = Map.of("Count", "string", "Display", "number", "Flag", "boolean");
		OperationContext ctx = separateTypes(callerTypes, List.of("Flag", "Count", "Display"));
		for (boolean basyx : List.of(false, true)) {
			Exchange exchange = exchangeWith(Map.of("Reply",
					Map.of("Count", "001", "Display", "1.5", "Flag", "true", "DeviceFlag", "not caller output")));
			exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, basyx);
			new OperationReplyProcessor(ctx).process(exchange);
			if (basyx) {
				assertEquals(List.of(
						Map.of("value", Map.of("modelType", "Property", "idShort", "Flag", "valueType", "xs:boolean", "value", "true")),
						Map.of("value", Map.of("modelType", "Property", "idShort", "Count", "valueType", "xs:string", "value", "001")),
						Map.of("value", Map.of("modelType", "Property", "idShort", "Display", "valueType", "xs:double", "value", "1.5"))),
						envelope(exchange.getMessage().getBody()));
			} else {
				assertEquals(Map.of("Count", "001", "Display", 1.5, "Flag", true), exchange.getMessage().getBody());
			}
		}
		assertEquals(Map.of("Count", "integer", "DeviceFlag", "string"), ctx.responseTypes);
		assertEquals(callerTypes, ctx.callerResponseTypes);
	}

	@Test
	void scalarBasyxReplyUsesCallerOutputNameAndTypeRatherThanDeviceOutput() {
		Exchange exchange = exchangeWith(Map.of("Reply", "true"));
		exchange.setProperty(OperationPayloads.BASYX_DELEGATION_BODY, true);
		new OperationReplyProcessor(separateTypes(Map.of("Flag", "boolean"), List.of("Flag"))).process(exchange);
		assertEquals(List.of(Map.of("value", Map.of("modelType", "Property", "idShort", "Flag",
				"valueType", "xs:boolean", "value", "true"))), envelope(exchange.getMessage().getBody()));
	}

	@Test
	void explicitEmptyCallerTypeMapDoesNotFallBackToDeviceTypes() {
		Exchange exchange = exchangeWith(Map.of("Reply", Map.of("Count", "001")));
		new OperationReplyProcessor(separateTypes(Map.of(), List.of("Count"))).process(exchange);
		assertEquals(Map.of("Count", "001"), exchange.getMessage().getBody());
	}
}
