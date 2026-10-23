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
package dk.aau.smartproductionlab.dmp.management.compiler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import dk.aau.smartproductionlab.dmp.aas.model.Aas;
import dk.aau.smartproductionlab.dmp.management.model.WireModel;
import dk.aau.smartproductionlab.dmp.management.model.WireProjection;
import dk.aau.smartproductionlab.dmp.management.repository.AasReferenceResolver;
import dk.aau.smartproductionlab.dmp.management.repository.AasRepositoryClient;
import dk.aau.smartproductionlab.dmp.management.repository.FakeAasRepository;
import org.junit.jupiter.api.Test;

class MappingPlanCompilerTest {
	private static final String OWNER = "urn:fixture:asset";
	private static final String AID_ID = OWNER + "/submodels/AID";
	private static final String DATA_ID = OWNER + "/submodels/State";
	private static final String AIMC_ID = OWNER + "/submodels/AIMC";

	private static FakeAasRepository repository() throws Exception {
		return new FakeAasRepository().put(aid()).put(data());
	}

	private static WireModel.Aimc parse(String json) {
		return WireProjection.aimc(Aas.read(json, Submodel.class));
	}

	private static long reads(FakeAasRepository repository, String id) {
		String path = "/repo/submodels/" + AasRepositoryClient.base64UrlIdentifier(id);
		return repository.requests().stream().filter(request -> request.equals(path) || request.startsWith(path + "/")).count();
	}

	@Test
	void cachesReadsAcrossCompilationsAndRetriesOnlyEvictedFailures() throws Exception {
		var original = parse(aimc());
		var mapping = original.mappings().get(0);
		var missing = new WireModel.Endpoint("missing", Aas.read(refProperty("urn:missing", "Value"), org.eclipse.digitaltwin.aas4j.v3.model.Reference.class),
				mapping.sources().get(0).element());
		var blocked = new WireModel.Mapping(Optional.of("blocked"), 1, List.of(missing), List.of(missing),
				mapping.defaultPollingInterval(), mapping.transformation(), WireModel.Presence.absent(),
				WireModel.Presence.absent(), WireModel.Presence.absent(), mapping.element());
		var aimc = new WireModel.Aimc(original.submodel(), List.of(mapping, blocked));
		try (var repository = repository()) {
			var resolver = new AasReferenceResolver(repository.client());
			var compiler = new MappingPlanCompiler(resolver);
			for (int pass = 1; pass <= 2; pass++) {
				var desired = compiler.compileDesired(aimc);
				assertTrue(desired.mappings().get(0).valid());
				assertFalse(desired.mappings().get(1).valid());
				assertEquals(2, desired.mappings().get(1).failures().size());
				assertEquals(2, reads(repository, AID_ID), "an AID is read ($metadata, whole) once and then cached");
				assertEquals(2, reads(repository, DATA_ID), "a direct target is read ($metadata, element) once and then cached");
				assertEquals(pass, reads(repository, "urn:missing"), "a failed read is retried after evictFailures");
				resolver.evictFailures();
			}
		}
	}

	@Test
	void compilesTypedAasAndAidBindingsWithoutGeneratedDescriptors() throws Exception {
		try (var repository = repository()) {
			var result = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(parse(aimc()));
			assertEquals(AasRepositoryClient.Status.FOUND, result.status());
			assertEquals(AIMC_ID, result.value().aimcSubmodelId());
			var mapping = result.value().mappings().get(0);
			assertEquals("mapping_0", mapping.identity(), "the compiler must not invent a missing wire idShort");
			assertEquals(0, mapping.listIndex());
			assertEquals("temperature", mapping.sources().get(0).id());
			assertEquals("Value", mapping.sinks().get(0).id());
			var aidTarget = assertInstanceOf(MappingPlanCompiler.AidTarget.class, mapping.sources().get(0).target());
			assertEquals("interface_http", aidTarget.resolved().owner().idShort());
			assertEquals(List.of(WireModel.ProtocolFamily.HTTP), aidTarget.resolved().owner().protocolFamilies());
			assertEquals("Temperature", aidTarget.resolved().affordance().idShort().value());
			var directTarget = assertInstanceOf(MappingPlanCompiler.DirectTarget.class, mapping.sinks().get(0).target());
			assertEquals("Property", Aas.modelType(directTarget.resolved().element()));
			assertFalse(mapping.responseSources().present());
			assertFalse(mapping.sources().get(0).evaluation().present());
		}
	}

	@Test
	void ordinaryPropertyValueChangesDoNotChangeEffectiveRouteFingerprint() throws Exception {
		try (var repository = repository()) {
			var resolver = new AasReferenceResolver(repository.client());
			var compiler = new MappingPlanCompiler(resolver);
			String directAimc = aimc().replace(ref(AID_ID, "interface_http", "InteractionMetadata", "properties", "Temperature"),
					refProperty(DATA_ID, "Value"));
			var aimc = parse(directAimc);
			var settings = new ConnectorConfigurationFactory.RuntimeSettings(repository.uri());
			var factory = new ConnectorConfigurationFactory();
			var first = factory.createDesiredBundles(compiler.compileDesired(aimc), settings).get(0);
			assertTrue(first.valid(), first.issues().toString());
			repository.put(data("STOPPED"));
			resolver.evict(List.of(DATA_ID));
			var second = factory.createDesiredBundles(compiler.compileDesired(aimc), settings).get(0);
			assertTrue(second.valid(), second.issues().toString());
			assertEquals(first.fingerprint().exported(), second.fingerprint().exported());
			assertTrue(first.fingerprint().sameEffectiveConfiguration(second.fingerprint()));
		}
	}

	@Test void isolatesFailedMappingsAndRetainsEveryRequestAndResponseDependency() throws Exception {
		var original=parse(aimc());
		var good=original.mappings().get(0);
		var missing=Aas.read(refProperty("urn:missing:request","Value"), org.eclipse.digitaltwin.aas4j.v3.model.Reference.class);
		var remote=Aas.read(refProperty("urn:missing:response","Value"), org.eclipse.digitaltwin.aas4j.v3.model.Reference.class);
		var blocked=new WireModel.Mapping(Optional.of("blocked"),1,
				List.of(new WireModel.Endpoint("bad",missing,good.sources().get(0).element())),good.sinks(),
				good.defaultPollingInterval(),good.transformation(),
				WireModel.Presence.of(List.of(new WireModel.Endpoint("reply",remote,good.sources().get(0).element()))),
				WireModel.Presence.absent(),WireModel.Presence.absent(),good.element());
		try (var repository = repository()) {
			var compiler=new MappingPlanCompiler(new AasReferenceResolver(repository.client()));
			var desired=compiler.compileDesired(new WireModel.Aimc(original.submodel(),List.of(blocked,good)));
			assertEquals(2,desired.mappings().size());
			assertFalse(desired.mappings().get(0).valid());assertTrue(desired.mappings().get(1).valid());
			assertEquals(2,desired.mappings().get(0).failures().size());
			assertEquals(Set.of(new MappingPlanCompiler.Dependency("submodel",AIMC_ID),
					new MappingPlanCompiler.Dependency("submodel",DATA_ID),new MappingPlanCompiler.Dependency("submodel","urn:missing:request"),
					new MappingPlanCompiler.Dependency("submodel","urn:missing:response")),desired.mappings().get(0).dependencies());
			assertEquals(1,desired.validPlan().mappings().size());
			var compiled=new ConnectorConfigurationFactory().createDesiredBundles(desired,
					new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081")));
			assertEquals(desired.mappings().get(0).dependencies(),compiled.get(0).desired().dependencies());
			assertFalse(compiled.get(0).valid());
			// This fixture has no executable form, so capability-blocked plans must also retain dependencies.
			assertFalse(compiled.get(1).valid());assertFalse(compiled.get(1).issues().isEmpty());
			assertEquals(desired.mappings().get(1).dependencies(),compiled.get(1).desired().dependencies());
		}
	}

	@Test void projectionFailureBlocksOnlyItsMappingAndKeepsDiscoverableDependencies() throws Exception {
		var root = com.google.gson.JsonParser.parseString(aimc()).getAsJsonObject();
		var list = root.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value");
		var good = list.get(0).getAsJsonObject().deepCopy();
		var bad = good.deepCopy();
		bad.addProperty("idShort", "bad-wire");
		bad.getAsJsonArray("value").add(com.google.gson.JsonParser.parseString(
				"{\"modelType\":\"Property\",\"idShort\":\"apiToken\",\"valueType\":\"xs:string\",\"value\":\"private-token-one\"}"));
		var sourceList = bad.getAsJsonArray("value").get(0).getAsJsonObject().getAsJsonArray("value");
		var sourceEndpoint = sourceList.get(0).getAsJsonObject().getAsJsonArray("value");
		sourceEndpoint.get(1).getAsJsonObject().addProperty("idShort", "UnrecognizedSourceProperty");
		list.set(0, bad);
		list.add(good);
		var aimc = WireProjection.aimcIsolated(Aas.read(root.toString(), Submodel.class));
		assertEquals(1, aimc.mappingFailures().size());
		assertEquals(1, aimc.mappings().size());
		try (var repository = repository()) {
			var resolver = new AasReferenceResolver(repository.client());
			var desired = new MappingPlanCompiler(resolver).compileDesired(aimc);
			assertEquals(2, desired.mappings().size());
			assertFalse(desired.mappings().get(0).valid());
			assertTrue(desired.mappings().get(1).valid());
			assertEquals(Set.of(new MappingPlanCompiler.Dependency("submodel", AIMC_ID),
					new MappingPlanCompiler.Dependency("submodel", AID_ID),
					new MappingPlanCompiler.Dependency("submodel", DATA_ID)), desired.mappings().get(0).dependencies());
			assertEquals(1, desired.validPlan().mappings().size());
			var factory = new ConnectorConfigurationFactory();
			var settings = new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://aas.example:8081"));
			var blocked = factory.createDesiredBundles(desired, settings).get(0);
			assertFalse(blocked.valid());
			assertNotNull(blocked.fingerprint());
			assertFalse(blocked.fingerprint().exported().contains("private-token-one"));

			var changedRoot = com.google.gson.JsonParser.parseString(root.toString()).getAsJsonObject();
			var changedList = changedRoot.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value");
			var changedMapping = changedList.get(0).getAsJsonObject().getAsJsonArray("value");
			changedMapping.get(changedMapping.size() - 1).getAsJsonObject().addProperty("value", "private-token-two");
			var changedAimc = WireProjection.aimcIsolated(Aas.read(changedRoot.toString(), Submodel.class));
			var changedDesired = new MappingPlanCompiler(resolver).compileDesired(changedAimc);
			var changedBlocked = factory.createDesiredBundles(changedDesired, settings).get(0);
			assertEquals(blocked.fingerprint().exported(), changedBlocked.fingerprint().exported());
			assertFalse(blocked.fingerprint().sameEffectiveConfiguration(changedBlocked.fingerprint()));
		}
	}

	@Test void unresolvedAasIdTemplatesBlockTheMapping() throws Exception {
		var aimc=parse(aimc().replace(DATA_ID,"{aas_id}/submodels/State"));
		try (var repository = repository()) {
			var desired=new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compileDesired(aimc);
			assertFalse(desired.mappings().get(0).valid());
			assertTrue(desired.mappings().get(0).failures().get(0).detail().contains("{aas_id}"));
		}
	}

	@Test void aidsWithNonconventionalIdentifiersAreRecognisedBySemantics() throws Exception {
		String id="urn:odd:device-description/";
		var aimc=parse(aimc().replace(AID_ID,id));
		try (var repository = repository().put(aid().replace(AID_ID,id))) {
			var desired=new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compileDesired(aimc);
			assertTrue(desired.mappings().get(0).valid());
			assertTrue(desired.mappings().get(0).dependencies().contains(new MappingPlanCompiler.Dependency("submodel",id)));
		}
	}

	@Test void operationVariableMetadataReferencesJoinDependencySetWithoutDeviceReads() throws Exception {
		var aimc=parse(aimc().replace(ref(AID_ID,"interface_http","InteractionMetadata","properties","Temperature"),
				"{\"type\":\"ModelReference\",\"keys\":["+key("Submodel","cci")+","+key("Operation","Call")+"]}"));
		String cci="{\"modelType\":\"Submodel\",\"id\":\"cci\",\"idShort\":\"CCI\",\"submodelElements\":[{\"modelType\":\"Operation\",\"idShort\":\"Call\",\"inputVariables\":[{\"value\":{\"modelType\":\"ReferenceElement\",\"idShort\":\"args\",\"value\":"+refProperty("urn:missing:argument-contract","Value")+"}}]}]}";
		try (var repository = repository().put(cci)) {
			var desired=new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compileDesired(aimc);
			assertTrue(desired.mappings().get(0).valid());
			assertTrue(desired.mappings().get(0).dependencies().contains(new MappingPlanCompiler.Dependency("submodel","urn:missing:argument-contract")));
			assertEquals(0, reads(repository, "urn:missing:argument-contract"), "metadata references are dependencies, not reads");
		}
	}

	private static String aimc() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + OWNER + "/submodels/AIMC\",\"idShort\":\"AIMC\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"MappingConfigurations\",\"typeValueListElement\":\"SubmodelElementCollection\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations\"}]},"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ endpoint("Sources", "Source", "SourceId", "temperature", ref(AID_ID, "interface_http", "InteractionMetadata", "properties", "Temperature")) + ","
				+ endpoint("Sinks", "Sink", "SinkId", "Value", refProperty(DATA_ID, "Value"))
				+ "]}]}]}";
	}

	private static String endpoint(String listName, String referenceName, String idName, String id, String reference) {
		return "{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + listName
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"value\":["
				+ "{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + referenceName + "\",\"value\":" + reference + "},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"" + idName + "\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}]}]}";
	}

	private static String ref(String submodel, String... path) {
		java.util.List<String> keys = new java.util.ArrayList<>();
		keys.add(key("Submodel", submodel));
		for (String key : path) keys.add(key("SubmodelElementCollection", key));
		return "{\"type\":\"ModelReference\",\"keys\":[" + String.join(",", keys) + "]}";
	}

	private static String refProperty(String submodel, String property) {
		return "{\"type\":\"ModelReference\",\"keys\":[" + key("Submodel", submodel) + ","
				+ key("Property", property) + "]}";
	}

	private static String key(String type, String value) {
		return "{\"type\":\"" + type + "\",\"value\":\"" + value + "\"}";
	}

	private static String aid() {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + AID_ID + "\",\"idShort\":\"AID\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"interface_http\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/Interface\"}]},"
				+ "\"supplementalSemanticIds\":[{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"http://www.w3.org/2011/http\"}]}],"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"InteractionMetadata\",\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"properties\",\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Temperature\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://www.w3.org/2019/wot/td#PropertyAffordance\"}]},\"value\":[]}"
				+ "]}]}]}]}";
	}

	private static String data() {
		return data("ok");
	}

	private static String data(String value) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + DATA_ID + "\",\"idShort\":\"State\",\"submodelElements\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"Value\",\"valueType\":\"xs:string\",\"value\":\"" + value + "\"}]}";
	}
}
