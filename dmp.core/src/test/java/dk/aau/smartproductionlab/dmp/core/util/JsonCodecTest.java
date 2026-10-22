package dk.aau.smartproductionlab.dmp.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Map;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.junit.jupiter.api.Test;

class JsonCodecTest {

	@Test
	void strictParsingPreservesNumbersStringsAndNulls() {
		Map<?, ?> value = assertInstanceOf(Map.class, JsonCodec.parseJson(
				" {\"integer\":858383,\"large\":9007199254740993,\"fraction\":0.5,"
						+ "\"integral\":1.0,\"string\":\"01\",\"null\":null,\"array\":[true,null]} \n"));
		assertEquals(858383L, value.get("integer"));
		assertEquals(9007199254740993L, value.get("large"));
		assertEquals(0.5, value.get("fraction"));
		assertEquals(1L, value.get("integral"));
		assertEquals("01", value.get("string"));
		assertTrue(value.containsKey("null"));
		assertNull(value.get("null"));
		assertEquals(Arrays.asList(true, null), value.get("array"));
		assertEquals("\"quoted\"", JsonCodec.parseJson("\"\\\"quoted\\\"\""));
		assertEquals(42L, JsonCodec.parseJson("42"));
		assertEquals(false, JsonCodec.parseJson("false"));
		assertNull(JsonCodec.parseJson("null"));
		assertNull(JsonCodec.parseJson(null));
	}

	@Test
	void rejectsMalformedLenientAndTrailingInput() {
		for (String text : new String[] { "", " \n", "bare text", "bare", "01", "NaN", "Infinity",
				"/* comment */ {}", "{} // comment", "{unquoted:1}", "{'single':1}",
				"{\"a\":}", "{\"a\":1", "[1,]", "[1", "{} {}", "null false",
				"\"valid\" trailing", "{} trailing", "null trailing" }) {
			assertThrows(JsonSyntaxException.class, () -> JsonCodec.parseJson(text), text);
		}
	}

	@Test
	void treeAndWireEncodingKeepConfiguredNullAndNumericContract() {
		Object value = JsonCodec.fromJsonTree(JsonParser.parseString("{\"n\":null,\"i\":1.0,\"s\":\"1\"}"));
		assertEquals("{\"n\":null,\"i\":1,\"s\":\"1\"}", JsonCodec.toJson(value));
		assertEquals("null", JsonCodec.toJson(JsonPointer.JSON_NULL));
	}
}
