package org.eclipse.digitaltwin.basyx.dmp.core.operation;

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
	private static void assertJson(String expected, String actual) {
		assertEquals(JsonCodec.parse(expected), JsonCodec.parse(actual));
	}

	private static Exchange exchangeWith(String body) {
		DefaultCamelContext context = new DefaultCamelContext();
		Exchange exchange = new DefaultExchange(context);
		exchange.getMessage().setBody(body);
		return exchange;
	}

	private static OperationContext context(Map<String, String> responseTypes) {
		return new OperationContext("Occupy", "req.lua", "res.lua", "mock:cmd",
				30L, "POST", "mqtt", Map.of(), responseTypes);
	}

	@Test
	void aNumericLookingUuidComesBackAsAString() {
		// what the station actually sends back after a stringified Uuid
		Exchange exchange = exchangeWith("{\"State\":\"SUCCESS\",\"Uuid\":\"1\","
				+ "\"TimeStamp\":\"2026-09-30T07:43:32.365Z\"}");

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("State", "string"))))
				.process(exchange);

		assertJson("{\"State\":\"SUCCESS\",\"Uuid\":\"1\","
				+ "\"TimeStamp\":\"2026-09-30T07:43:32.365Z\"}",
				exchange.getMessage().getBody(String.class));
		assertEquals("1", String.valueOf(JsonCodec.parse(
				exchange.getMessage().getBody(String.class)) instanceof Map
						? ((Map<?, ?>) JsonCodec.parse(exchange.getMessage().getBody(String.class))).get("Uuid")
						: null),
				"a numeric-looking string Uuid must not come back as a double");
	}

	@Test
	void aNonNumericFieldIsUnaffected() {
		Exchange exchange = exchangeWith("{\"State\":\"SUCCESS\",\"Uuid\":\"1\"}");

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("State", "string"))))
				.process(exchange);

		assertEquals("{\"State\":\"SUCCESS\",\"Uuid\":\"1\"}",
				exchange.getMessage().getBody(String.class));
	}

	@Test
	void anIntegralNumberStaysIntegral() {
		Exchange exchange = exchangeWith("{\"count\":858383}");

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("count", "integer"))))
				.process(exchange);

		assertEquals("{\"count\":858383}", exchange.getMessage().getBody(String.class),
				"an integral count must not become 858383.0");
	}

	@Test
	void aGenuineFractionKeepsItsFractionalForm() {
		Exchange exchange = exchangeWith("{\"v\":103.5585973}");

		new OperationReplyProcessor(context(new LinkedHashMap<>(Map.of("v", "number"))))
				.process(exchange);

		assertJson("{\"v\":103.5585973}", exchange.getMessage().getBody(String.class));
	}

	@Test
	void declaredOutputTypesStillApply() {
		Exchange exchange = exchangeWith("{\"State\":1}");
		OperationContext ctx = context(new LinkedHashMap<>(Map.of("State", "string")));

		new OperationReplyProcessor(ctx).process(exchange);

		assertEquals("{\"State\":\"1\"}", exchange.getMessage().getBody(String.class),
				"a declared xs:string output must be stringified in the reply too");
	}

	@Test
	void theSharedCodecKeepsTheNumericContractInOnePlace() {
		assertEquals("{\"a\":1}", org.eclipse.digitaltwin.basyx.dmp.core.util
				.JsonCodec.toJson(org.eclipse.digitaltwin.basyx.dmp.core.util
						.JsonCodec.parse("{\"a\":1}")));
		assertEquals(List.of(1L, 2L), org.eclipse.digitaltwin.basyx.dmp.core.util
				.JsonCodec.parse("{\"a\":[1,2]}") instanceof Map
						? ((Map<?, ?>) org.eclipse.digitaltwin.basyx.dmp.core.util
								.JsonCodec.parse("{\"a\":[1,2]}")).get("a")
						: null);
	}
}
