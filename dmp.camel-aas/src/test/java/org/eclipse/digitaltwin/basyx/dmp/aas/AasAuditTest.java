package org.eclipse.digitaltwin.basyx.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AasAuditTest {
	@Test
	void submodelIdIsDecodedFromTheEndpoint() {
		String id = "https://example.org/ids/sm/Variables";
		String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8));
		assertEquals(id, AasAudit.submodelId("http://aas:8081/submodels/" + encoded));
		assertEquals(id, AasAudit.submodelId("http://aas:8081/submodels/" + encoded + "/"));
		assertEquals("http://aas:8081/other", AasAudit.submodelId("http://aas:8081/other"));
	}

	@Test
	void readElementsAreLoggedAsTheirValues() {
		assertEquals(42L, AasValues.elementValue(Map.of("modelType", "Property", "valueType", "xs:int", "value", "42")));
		assertEquals(Map.of("x", 1.5, "on", true), AasValues.elementValue(Map.of("modelType", "SubmodelElementCollection",
				"value", List.of(Map.of("modelType", "Property", "idShort", "x", "valueType", "xs:double", "value", "1.5"),
						Map.of("modelType", "Property", "idShort", "on", "valueType", "xs:boolean", "value", "true")))));
		assertEquals(List.of("a", "b"), AasValues.elementValue(Map.of("modelType", "SubmodelElementList",
				"value", List.of(Map.of("modelType", "Property", "valueType", "xs:string", "value", "a"),
						Map.of("modelType", "Property", "valueType", "xs:string", "value", "b")))));
		Object range = Map.of("modelType", "Range", "min", "1");
		assertEquals(range, AasValues.elementValue(range), "other element types are logged whole");
	}
}
