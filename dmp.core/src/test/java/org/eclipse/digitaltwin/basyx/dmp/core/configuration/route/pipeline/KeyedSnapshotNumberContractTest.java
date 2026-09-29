package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * The shared numeric contract between the two transformation engines.
 *
 * <p>Gson decodes every JSON number to a {@code Double}. Left alone, an
 * integral value re-encodes as {@code 858383.0}, and both engines inherit that:
 * jsonata echoes the Double it is handed, and LuaJ stores every number as a
 * double. The snapshot therefore restores the integer/floating distinction
 * before anything else sees the value, using the same rule the Lua boundary
 * applies on the way out.
 */
class KeyedSnapshotNumberContractTest {

	@Test
	void integralNumbersStayIntegral() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse("{\"count\":858383}");
		assertEquals(858383L, snapshot.value("count"));
		assertTrue(snapshot.value("count") instanceof Long,
				"an integral value must not become a Double");
		assertEquals("{\"count\":858383}", snapshot.toJson());
	}

	@Test
	void negativeAndZeroStayIntegral() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse("{\"a\":0,\"b\":-42,\"c\":7}");
		assertEquals("{\"a\":0,\"b\":-42,\"c\":7}", snapshot.toJson());
	}

	@Test
	void genuineFractionsKeepTheirFractionalForm() {
		assertEquals("{\"v\":103.5585973}", KeyedSnapshot.parse("{\"v\":103.5585973}").toJson());
		assertEquals("{\"v\":0.5}", KeyedSnapshot.parse("{\"v\":0.5}").toJson());
	}

	@Test
	void numbersInsideCollectionsAreNormalizedToo() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse("{\"q\":[1,2.5,{\"deep\":3}]}");
		assertEquals("{\"q\":[1,2.5,{\"deep\":3}]}", snapshot.toJson());
	}

	@Test
	void valuesBeyondTheExactIntegerRangeStayFloatingPoint() {
		// 2^53 and beyond cannot be narrowed to a long without losing the value
		String text = "{\"big\":9007199254740992}";
		assertEquals(text, KeyedSnapshot.parse(text).toJson());
	}

	@Test
	void aDeliveredNullIsPreserved() {
		KeyedSnapshot snapshot = KeyedSnapshot.parse("{\"a\":null}");
		assertTrue(snapshot.has("a"), "a stored null is still a present key");
		assertEquals("{\"a\":null}", snapshot.toJson());
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
	void nonKeyedRootsAreSupported() {
		assertEquals("7", KeyedSnapshot.parse("7").toJson());
		assertFalse(KeyedSnapshot.parse("7").isKeyed());
		assertTrue(KeyedSnapshot.parse("[]").ids().isEmpty());
	}
}
