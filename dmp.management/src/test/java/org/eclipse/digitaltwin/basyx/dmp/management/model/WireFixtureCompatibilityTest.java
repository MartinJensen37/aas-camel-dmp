package org.eclipse.digitaltwin.basyx.dmp.management.model;

import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.junit.jupiter.api.Test;

class WireFixtureCompatibilityTest {
	@Test void allRelevantSubmodelCasesMatchTheirDeclaredProjectionOutcome() throws Exception {
		Map<?,?> manifest=(Map<?,?>)JsonCodec.parseJson(resource("manifest.json"));
		List<?> cases=(List<?>)manifest.get("cases");
		assertEquals(15,cases.size());
		for(Object raw:cases) {
			Map<?,?> contract=(Map<?,?>)raw;
			String aimc=(String)contract.get("aimc");
			String aid=(String)contract.get("aid");
			if ("reject-retired-declaration".equals(contract.get("expectedOutcome")))
				assertThrows(WireProjectionException.class, () -> WireProjection.aimc(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(resource(aimc), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)), aimc);
			else assertDoesNotThrow(() -> WireProjection.aimc(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(resource(aimc), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)),aimc);
			assertDoesNotThrow(() -> WireProjection.aid(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(resource(aid), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)),aid);
		}
	}
	private static String resource(String name) throws IOException {
		try(var input=WireFixtureCompatibilityTest.class.getResourceAsStream("/runtime-model-contracts/"+name)) {
			assertNotNull(input,"required Java raw fixture "+name);
			return new String(input.readAllBytes(),StandardCharsets.UTF_8);
		}
	}
}
