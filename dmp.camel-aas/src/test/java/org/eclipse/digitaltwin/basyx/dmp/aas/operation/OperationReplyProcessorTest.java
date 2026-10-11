package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
		assertEquals(JsonCodec.parseJson(expected), actual);
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
}
