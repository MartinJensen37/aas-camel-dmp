package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

/**
 * The shared numeric contract for decoded Java payload values.
 *
 * <p>Gson decodes every JSON number to a {@code Double}. Left alone, an
 * integral value re-encodes as {@code 858383.0}. Connector JSON decoding uses
 * JsonCodec; KeyedSnapshot then retains the decoded Java value without parsing
 * or stringifying it.
 */
class KeyedSnapshotNumberContractTest {

	@Test
	void integralNumbersStayIntegral() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse(JsonCodec.parseJson("{\"count\":858383}"));
		assertEquals(858383L, snapshot.value("count"));
		assertTrue(snapshot.value("count") instanceof Long,
				"an integral value must not become a Double");
		assertEquals("{\"count\":858383}", JsonCodec.toJson(snapshot.root()));
	}

	@Test
	void negativeAndZeroStayIntegral() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse(JsonCodec.parseJson("{\"a\":0,\"b\":-42,\"c\":7}"));
		assertEquals("{\"a\":0,\"b\":-42,\"c\":7}", JsonCodec.toJson(snapshot.root()));
	}

	@Test
	void genuineFractionsKeepTheirFractionalForm() {
		assertEquals("{\"v\":103.5585973}", JsonCodec.toJson(KeyedSnapshot.parse(JsonCodec.parseJson("{\"v\":103.5585973}")).root()));
		assertEquals("{\"v\":0.5}", JsonCodec.toJson(KeyedSnapshot.parse(JsonCodec.parseJson("{\"v\":0.5}")).root()));
	}

	@Test
	void numbersInsideCollectionsAreNormalizedToo() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse(JsonCodec.parseJson("{\"q\":[1,2.5,{\"deep\":3}]}"));
		assertEquals("{\"q\":[1,2.5,{\"deep\":3}]}", JsonCodec.toJson(snapshot.root()));
	}

	@Test
	void valuesBeyondTheExactIntegerRangeStayFloatingPoint() {
		// 2^53 and beyond cannot be narrowed to a long without losing the value
		String text = "{\"big\":9007199254740992}";
		assertEquals(text, JsonCodec.toJson(KeyedSnapshot.parse(JsonCodec.parseJson(text)).root()));
	}

	@Test
	void aDeliveredNullIsPreserved() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse(JsonCodec.parseJson("{\"a\":null}"));
		assertTrue(snapshot.has("a"), "a stored null is still a present key");
		assertEquals("{\"a\":null}", JsonCodec.toJson(snapshot.root()));
	}

	@Test
	void nonJsonTextPassesThroughUnchanged() {
		assertEquals("IDLE", KeyedSnapshot.parse("IDLE").root());
	}

	@Test
	void snapshotStaysImmutable() {
		Map<String, Object> source = Map.of("a", 1L);
		KeyedSnapshot snapshot = KeyedSnapshot.of(source);
		assertTrue(snapshot.isKeyed());
		assertEquals(1L, snapshot.value("a"));
		assertFalse(snapshot.ids().isEmpty());
		org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
				() -> snapshot.asMap().put("b", 2L));
	}

	@Test
	void numericLookingStringValueStaysAStringAfterBoundaryDecode() {
		// The example fixture stores pressure as a JSON string with
		// valueType anyType. Nothing on this boundary may "helpfully" turn it
		// into a number: the transformation engines preserve the type they are
		// given, and a string is not a number.
		String property = "{\"modelType\":{\"name\":\"Property\"},\"idShort\":\"pressure\","
				+ "\"valueType\":\"anyType\",\"value\":\"103.5585973\"}";
		KeyedSnapshot snapshot = KeyedSnapshot.parse(JsonCodec.parseJson(property));
		Object value = ((Map<?, ?>) snapshot.root()).get("value");
		assertEquals("103.5585973", value);
		assertTrue(value instanceof String, "a quoted JSON string must not become a number");
		assertEquals(property, JsonCodec.toJson(snapshot.root()));
	}

	@Test
	void nonKeyedRootsAreSupported() {
		assertEquals(7, KeyedSnapshot.parse(7).root());
		assertFalse(KeyedSnapshot.parse(7).isKeyed());
		assertTrue(KeyedSnapshot.parse(java.util.List.of()).ids().isEmpty());
	}

	@Test
	void jsonLookingStringRemainsOpaqueUntilConnectorDecode() {
		String text = "{\"count\":7}";
		KeyedSnapshot snapshot = KeyedSnapshot.parse(text);
		assertEquals(text, snapshot.root());
		assertFalse(snapshot.isKeyed());
	}
}
