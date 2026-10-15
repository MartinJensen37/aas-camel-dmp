/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link JsonPointer} (SPEC §6.5.1): object keys, 0-based array
 * indices, RFC 6901 and JSONPath-ish syntax, missing-path semantics.
 */
class JsonPointerTest {

	private final Map<String, Object> payload = Map.of(
			"State", "EXECUTE",
			"ProcessQueue", Map.of("mode", "AUTO", "depth", 3),
			"WorkOrders", List.of(Map.of("id", 7), Map.of("id", 8)));

	@Test
	void extractsTopLevelKeyWithDollarSyntax() {
		assertEquals(Optional.of("EXECUTE"), JsonPointer.extract(payload, "$.State"));
	}

	@Test
	void extractsNestedKeyWithDottedSyntax() {
		assertEquals(Optional.of("AUTO"), JsonPointer.extract(payload, "$.ProcessQueue.mode"));
		assertEquals(Optional.of(3), JsonPointer.extract(payload, "$.ProcessQueue.depth"));
	}

	@Test
	void extractsBareDottedSyntax() {
		assertEquals(Optional.of("EXECUTE"), JsonPointer.extract(payload, "State"));
		assertEquals(Optional.of("AUTO"), JsonPointer.extract(payload, "ProcessQueue.mode"));
	}

	@Test
	void extractsRfc6901Syntax() {
		assertEquals(Optional.of("EXECUTE"), JsonPointer.extract(payload, "/State"));
		assertEquals(Optional.of("AUTO"), JsonPointer.extract(payload, "/ProcessQueue/mode"));
	}

	@Test
	void extractsArrayIndex() {
		assertEquals(Optional.of(7), JsonPointer.extract(payload, "$.WorkOrders[0].id"));
		assertEquals(Optional.of(8), JsonPointer.extract(payload, "$.WorkOrders[1].id"));
	}

	@Test
	void extractRollsBackOnMissingKey() {
		assertTrue(JsonPointer.extract(payload, "$.Missing").isEmpty());
		assertTrue(JsonPointer.extract(payload, "$.ProcessQueue.missing").isEmpty());
	}

	@Test
	void extractRollsBackOnOutOfRangeIndex() {
		assertTrue(JsonPointer.extract(payload, "$.WorkOrders[5].id").isEmpty());
	}

	@Test
	void extractRollsBackOnNegativeIndex() {
		assertTrue(JsonPointer.extract(payload, "$.WorkOrders[-1].id").isEmpty());
	}

	@Test
	void extractRollsBackWhenNavigatingIntoScalar() {
		assertTrue(JsonPointer.extract(payload, "$.State.nested").isEmpty());
	}

	@Test
	void emptyPointerExtractsRoot() {
		assertEquals(Optional.of(payload), JsonPointer.extract(payload, ""));
		assertEquals(Optional.of(payload), JsonPointer.extract(payload, "   "));
	}

	@Test
	void leafKeyReturnsLastObjectKey() {
		assertEquals("SetPoint", JsonPointer.parse("$.SetPoint").leafKey());
		assertEquals("mode", JsonPointer.parse("$.ProcessQueue.mode").leafKey());
		assertEquals("id", JsonPointer.parse("$.WorkOrders[0].id").leafKey());
		assertEquals("", JsonPointer.parse("").leafKey());
	}

	@Test
	void malformedExpressionThrows() {
		assertTrue(assertThrows(() -> JsonPointer.parse("$.a[")));
	}

	@Test
	void quotedBracketKeySupportsSpecialNames() {
		Map<String, Object> data = Map.of("a b", 1, "ke/y", 2, "c~d", 3);
		assertEquals(Optional.of(1), JsonPointer.extract(data, "$['a b']"));
		assertEquals(Optional.of(2), JsonPointer.extract(data, "$['ke/y']"));
		assertEquals(Optional.of(3), JsonPointer.extract(data, "$['c~d']"));
	}

	@Test
	void rfc6901EscapesDecode() {
		Map<String, Object> data = Map.of("a/b", 1, "m~n", 2);
		assertEquals(Optional.of(1), JsonPointer.extract(data, "/a~1b"));
		assertEquals(Optional.of(2), JsonPointer.extract(data, "/m~0n"));
	}

	@Test
	void pathishEscapesDecodeToo() {
		// T1.12: ~0/~1 previously decoded only in RFC-style syntax
		Map<String, Object> data = Map.of("a/b", 1, "m~n", 2);
		assertEquals(Optional.of(1), JsonPointer.extract(data, "$.a~1b"));
		assertEquals(Optional.of(2), JsonPointer.extract(data, "$.m~0n"));
	}

	@Test
	void quotedKeysKeepLiteralsVerbatim() {
		Map<String, Object> data = Map.of("a~1b", 9);
		assertEquals(Optional.of(9), JsonPointer.extract(data, "$['a~1b']"));
	}

	@Test
	void nullLeafIsPresentButNull() {
		// T1.12: a JSON null leaf must not be indistinguishable from a missing path
		Map<String, Object> inner = new HashMap<>();
		inner.put("value", null);
		Map<String, Object> data = new HashMap<>();
		data.put("State", null);
		data.put("Nested", inner);
		Optional<Object> leaf = JsonPointer.extract(data, "$.State");
		assertTrue(leaf.isPresent(), "null leaf is present");
		assertSame(JsonPointer.JSON_NULL, leaf.get());
		assertSame(JsonPointer.JSON_NULL, JsonPointer.extract(data, "$.Nested.value").get());
		// navigating below a null node is missing, and so is an absent key
		assertTrue(JsonPointer.extract(data, "$.State.child").isEmpty());
		assertTrue(JsonPointer.extract(data, "$.Missing").isEmpty());
	}

	@Test
	void nullRootExtractsAsPresentNull() {
		// navigating below a null root is missing
		assertTrue(JsonPointer.extract(null, "$.x").isEmpty());
		// an empty pointer addresses the root itself: present, but null
		assertSame(JsonPointer.JSON_NULL, JsonPointer.extract(null, "").get());
	}

	@Test
	void toStringEscapesAndRoundTrips() {
		assertEquals("$.SetPoint", JsonPointer.parse("$.SetPoint").toString());
		assertEquals("$.a~1b.c~0d", JsonPointer.parse("/a~1b/c~0d").toString());
		// round trip through the pathish parser yields the same tokens
		assertEquals(
				JsonPointer.parse("/a~1b/c~0d").tokens(),
				JsonPointer.parse(JsonPointer.parse("/a~1b/c~0d").toString()).tokens());
	}

	private boolean assertThrows(Runnable runnable) {
		try {
			runnable.run();
			return false;
		} catch (IllegalArgumentException e) {
			return true;
		}
	}
}
