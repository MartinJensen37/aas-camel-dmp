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
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.Executors;

import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import org.eclipse.digitaltwin.basyx.dmp.management.model.AasFixtures;
import org.junit.jupiter.api.Test;

class AasReferenceResolverTest {
	private static final String SUBMODEL_ID = "urn:remote:owner/submodels/AID";
	private static final String AID = "https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel";
	private static final String AID_INTERFACE = "https://admin-shell.io/idta/AssetInterfacesDescription/1/0/Interface";
	private static final String AID_ACTION = "https://www.w3.org/2019/wot/td#ActionAffordance";
	private static final String PROPERTIES = "https://www.w3.org/2019/wot/json-schema#properties";
	private static final String DATA = "urn:asset/submodels/OperationalData";

	@Test
	void resolvesAidAffordancesFromTheWholeAidRecognisedByItsSemanticId() throws Exception {
		try (var repository = new FakeAasRepository().put(aid(SUBMODEL_ID))) {
			var resolver = new AasReferenceResolver(repository.client());
			var result = resolver.resolve(aidReference(SUBMODEL_ID, "Property", "temperature-value"));
			assertEquals(AasRepositoryClient.Status.FOUND, result.status());
			var target = (AasReferenceResolver.AidTarget) result.value();
			assertEquals("Request", target.affordance().idShort().value());
			assertTrue(target.schema().present());
			assertEquals("temperature-value", target.schemaPath().get(0));
			assertEquals(List.of(path(SUBMODEL_ID) + "/$metadata", path(SUBMODEL_ID)), repository.requests(),
					"the semanticId in $metadata identifies the AID, which is then read whole once");
			resolver.resolve(aidReference(SUBMODEL_ID, "Property", "temperature-value"));
			assertEquals(2, repository.requests().size(), "a resolved submodel is served from the cache");
		}
	}

	@Test
	void readsAnAidNamedByTheFixedIdShortWholeWithoutAskingForMetadata() throws Exception {
		String id = "urn:asset/submodels/AssetInterfacesDescription";
		try (var repository = new FakeAasRepository().put(aid(id))) {
			var resolver = new AasReferenceResolver(repository.client());
			assertEquals(AasRepositoryClient.Status.FOUND, resolver.resolve(aidReference(id, "Property", "temperature-value")).status());
			assertEquals(List.of(path(id)), repository.requests());
		}
	}

	@Test
	void rejectsWrongKeyTypesMissingSubmodelsAndUnresolvedTemplates() throws Exception {
		try (var repository = new FakeAasRepository().put(aid(SUBMODEL_ID))) {
			var resolver = new AasReferenceResolver(repository.client());
			assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE,
					resolver.resolve(aidReference(SUBMODEL_ID, "SubmodelElementCollection", "temperature-value")).status());
			assertEquals(AasRepositoryClient.Status.NOT_FOUND,
					resolver.resolve(AasFixtures.reference("Submodel", "urn:other:owner/submodels/AID", "Property", "x")).status());
			var template = resolver.resolve(AasFixtures.reference("Submodel", "{aas_id}/submodels/OperationalData", "Property", "x"));
			assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE, template.status());
			assertTrue(template.detail().contains("{aas_id}"));
		}
	}

	@Test
	void readsOnlyMetadataAndTheReferencedSubtreesOfOrdinarySubmodels() throws Exception {
		try (var repository = new FakeAasRepository().put(operationalData())) {
			var resolver = new AasReferenceResolver(repository.client());
			var state = AasFixtures.reference("Submodel", DATA, "SubmodelElementCollection", "Status", "Property", "State");
			var mode = AasFixtures.reference("Submodel", DATA, "SubmodelElementCollection", "Status", "Property", "Mode");
			var status = AasFixtures.reference("Submodel", DATA, "SubmodelElementCollection", "Status");
			var speed = AasFixtures.reference("Submodel", DATA, "Property", "Speed");
			resolver.prefetch(List.of(state, mode, status, speed), Executors.newSingleThreadExecutor());
			assertEquals(List.of(path(DATA) + "/$metadata", path(DATA) + "/submodel-elements/Speed",
					path(DATA) + "/submodel-elements/Status"), repository.requests(),
					"Status covers State and Mode; Unrelated is never read");
			var resolved = (AasReferenceResolver.ResolvedElement) resolver.resolve(state).value();
			assertEquals("State", resolved.element().getIdShort());
			assertEquals(DATA, resolved.submodel().getId());
			assertEquals("xs:int", Aas.xsd(((org.eclipse.digitaltwin.aas4j.v3.model.Property) resolved.element()).getValueType()));
			assertEquals(AasRepositoryClient.Status.FOUND, resolver.resolve(speed).status());
			assertEquals(3, repository.requests().size());
			assertTrue(resolver.cachedElement(DATA, "Status.Mode").isPresent());
			assertFalse(resolver.cachedElement(DATA, "Unrelated").isPresent());
		}
	}

	@Test
	void operationVariableReferencesShareOneResolvedOperationAncestor() throws Exception {
		var position=AasFixtures.collection("Position",AasFixtures.property("x","xs:int","0"),
				AasFixtures.property("y","xs:int","0"));
		var operation=AasFixtures.operation("operation",List.of(AasFixtures.variable(position)),List.of(),List.of());
		String id="urn:asset/submodels/CCI";
		var submodel=AasFixtures.submodel(id,"CCI",AasFixtures.collection("Skills",
				AasFixtures.collection("Move",operation)));
		var x=AasFixtures.reference("Submodel",id,"SubmodelElementCollection","Skills",
				"SubmodelElementCollection","Move","Operation","operation",
				"SubmodelElementCollection","Position","Property","x");
		var y=AasFixtures.reference("Submodel",id,"SubmodelElementCollection","Skills",
				"SubmodelElementCollection","Move","Operation","operation",
				"SubmodelElementCollection","Position","Property","y");
		try (var repository=new FakeAasRepository().put(Aas.write(submodel))) {
			var resolver=new AasReferenceResolver(repository.client());
			resolver.prefetch(List.of(x,y),Runnable::run);
			var first=(AasReferenceResolver.ResolvedElement)resolver.resolve(x).value();
			var second=(AasReferenceResolver.ResolvedElement)resolver.resolve(y).value();
			assertEquals("x",first.element().getIdShort());
			assertEquals("y",second.element().getIdShort());
			assertEquals(operation,first.operation());
			assertEquals(first.operation(),second.operation());
			assertEquals("Skills.Move.operation",first.operationPath());
			assertEquals(List.of(path(id)+"/$metadata",path(id)+"/submodel-elements/Skills.Move.operation"),
					repository.requests(),"the Operation subtree should be fetched once for all member endpoints");
		}
	}

	@Test
	void retiredDeclarationsOutsideTheReferencedSubtreeDoNotBlockIt() throws Exception {
		try (var repository = new FakeAasRepository().put(operationalData())) {
			var resolver = new AasReferenceResolver(repository.client());
			assertEquals(AasRepositoryClient.Status.FOUND,
					resolver.resolve(AasFixtures.reference("Submodel", DATA, "Property", "Speed")).status());
			var retired = resolver.resolve(AasFixtures.reference("Submodel", DATA, "Property", "Unrelated"));
			assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE, retired.status());
			assertTrue(retired.detail().contains("writeDelegation"), retired.detail());
		}
	}

	@Test
	void readsWholeSubmodelsForManyPathsMissingPathsAndRepositoriesWithoutPartialReads() throws Exception {
		try (var repository = new FakeAasRepository().put(operationalData())) {
			var resolver = new AasReferenceResolver(repository.client());
			var missing = resolver.resolve(AasFixtures.reference("Submodel", DATA, "Property", "Absent"));
			assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE, missing.status());
			assertTrue(missing.detail().contains("reference element path is missing"), missing.detail());
			assertEquals(List.of(path(DATA) + "/$metadata", path(DATA) + "/submodel-elements/Absent", path(DATA)), repository.requests(),
					"a 404 element read is confirmed against the whole submodel");
		}
		try (var repository = new FakeAasRepository().put(operationalData())) {
			var resolver = new AasReferenceResolver(repository.client());
			List<org.eclipse.digitaltwin.aas4j.v3.model.Reference> many = new java.util.ArrayList<>();
			for (int i = 0; i <= AasReferenceResolver.MAX_ELEMENT_READS; i++)
				many.add(AasFixtures.reference("Submodel", DATA, "Property", "P" + i));
			resolver.prefetch(many, Executors.newSingleThreadExecutor());
			assertEquals(List.of(path(DATA) + "/$metadata", path(DATA)), repository.requests());
		}
		try (var repository = new FakeAasRepository().put(operationalData())) {
			repository.withoutMetadata();
			var resolver = new AasReferenceResolver(repository.client());
			assertEquals(AasRepositoryClient.Status.FOUND, resolver.resolve(AasFixtures.reference("Submodel", DATA, "Property", "Speed")).status());
			assertEquals(List.of(path(DATA) + "/$metadata", path(DATA)), repository.requests());
		}
		try (var repository = new FakeAasRepository().put(operationalData())) {
			repository.withoutElementPaths();
			var resolver = new AasReferenceResolver(repository.client());
			assertEquals(AasRepositoryClient.Status.FOUND, resolver.resolve(AasFixtures.reference("Submodel", DATA, "Property", "Speed")).status());
			assertEquals(List.of(path(DATA) + "/$metadata", path(DATA) + "/submodel-elements/Speed", path(DATA)), repository.requests());
		}
	}

	@Test
	void evictedSubmodelsAreReadAgainAndFailuresAreRetried() throws Exception {
		try (var repository = new FakeAasRepository().put(operationalData())) {
			var resolver = new AasReferenceResolver(repository.client());
			var speed = AasFixtures.reference("Submodel", DATA, "Property", "Speed");
			repository.override("busy", request -> new FakeAasRepository.Response(503, "{}"));
			assertEquals(AasRepositoryClient.Status.TRANSIENT_FAILURE, resolver.resolve(speed).status());
			repository.clearOverride("busy");
			assertEquals(AasRepositoryClient.Status.TRANSIENT_FAILURE, resolver.resolve(speed).status(), "failures stay until evicted");
			resolver.evictFailures();
			assertEquals(AasRepositoryClient.Status.FOUND, resolver.resolve(speed).status());
			repository.clearRequests();
			resolver.evict(List.of(DATA));
			assertEquals(AasRepositoryClient.Status.FOUND, resolver.resolve(speed).status());
			assertEquals(2, repository.requests().size());
		}
	}

	@Test
	void buildsRepositoryIdShortPathsWithListIndices() {
		var reference = AasFixtures.reference("Submodel", DATA, "SubmodelElementCollection", "a",
				"SubmodelElementList", "b", "Property", "0");
		assertEquals("a.b[0]", AasReferenceResolver.idShortPath(Aas.keys(reference)));
		assertTrue(AasReferenceResolver.within("a.b[0]", "a.b"));
		assertTrue(AasReferenceResolver.within("a.b.c", "a"));
		assertFalse(AasReferenceResolver.within("ab", "a"));
	}

	private static org.eclipse.digitaltwin.aas4j.v3.model.Reference aidReference(String id, String leafType, String leaf) {
		return AasFixtures.reference("Submodel", id, "SubmodelElementCollection", "interface_http",
				"SubmodelElementCollection", "InteractionMetadata", "SubmodelElementCollection", "actions",
				"SubmodelElementCollection", "Request", "SubmodelElementCollection", "input", leafType, leaf);
	}

	private static String path(String id) {
		return "/repo/submodels/" + AasRepositoryClient.base64UrlIdentifier(id);
	}

	private static String operationalData() {
		StringBuilder many = new StringBuilder();
		for (int i = 0; i <= AasReferenceResolver.MAX_ELEMENT_READS; i++)
			many.append(",{\"modelType\":\"Property\",\"idShort\":\"P").append(i).append("\",\"valueType\":\"xs:int\",\"value\":\"0\"}");
		return "{\"modelType\":\"Submodel\",\"id\":\"" + DATA + "\",\"idShort\":\"OperationalData\",\"submodelElements\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"1\"},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\",\"value\":\"auto\"}]},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"Speed\",\"valueType\":\"xs:double\",\"value\":\"2.5\"},"
				+ "{\"modelType\":\"Property\",\"idShort\":\"Unrelated\",\"valueType\":\"xs:string\",\"value\":\"x\","
				+ "\"qualifiers\":[{\"type\":\"writeDelegation\",\"valueType\":\"xs:string\",\"value\":\"http://old\"}]}"
				+ many + "]}";
	}

	private static String aid(String id) {
		return "{\"modelType\":\"Submodel\",\"id\":\"" + id + "\",\"idShort\":\"AID\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"" + AID + "\"}]},"
				+ "\"submodelElements\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"interface_http\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"" + AID_INTERFACE + "\"}]},"
				+ "\"supplementalSemanticIds\":[{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"http://www.w3.org/2011/http\"}]}],"
				+ "\"value\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"InteractionMetadata\",\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"actions\",\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Request\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"" + AID_ACTION + "\"}]},\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"input\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://www.w3.org/2019/wot/td#hasInputSchema\"}]},\"value\":["
				+ "{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"properties\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"" + PROPERTIES + "\"}]},\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"temperature-value\",\"valueType\":\"xs:double\",\"value\":\"0\"}]}]}]}]}]}]}]}";
	}
}
