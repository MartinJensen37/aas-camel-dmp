package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireModel;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireProjection;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasReferenceResolver;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasRepositoryClient;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.FakeAasRepository;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.PublishedModels;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Verifies authored wire models through the strict Java projection without Python. */
public class RawModelContractFixtureTest {
	private static final Set<String> CASES = Set.of(
			"stoppering", "opcua-method", "http-operation", "plc4x",
			"aas-property-event", "aas-property-poll", "aas-collection-poll",
			"opcua-read", "http-poll", "fine-path", "fine-protocols", "fan-in-out", "grouped-action",
			"projection-extensions", "stoppering-new-build");

	private byte[] resource(String path) throws Exception {
		assertFalse(path.startsWith("/") || path.contains(".."), "non-portable fixture path: " + path);
		try (var stream = getClass().getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(stream, "mandatory raw model fixture missing: " + path);
			return stream.readAllBytes();
		}
	}

	private JsonObject json(String path) throws Exception {
		return JsonParser.parseString(new String(resource(path), StandardCharsets.UTF_8)).getAsJsonObject();
	}

	private String text(String path) throws Exception {
		return new String(resource(path), StandardCharsets.UTF_8);
	}

	private static String sha256(byte[] bytes) throws Exception {
		return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
	}

	@Test
	public void mandatoryMatrixAndProvenanceNeedNoPython() throws Exception {
		JsonObject manifest = json("manifest.json");
		assertEquals(1, manifest.get("formatVersion").getAsInt());
		assertEquals(12, manifest.get("descriptorContractCaseCount").getAsInt());
		assertEquals(1, manifest.get("unsupportedContractCaseCount").getAsInt());
		assertEquals(1, manifest.get("cciUuidInoutCompatibilityCaseCount").getAsInt());
		assertEquals(1, manifest.get("projectionExtensionCaseCount").getAsInt());
		assertEquals("r1-relevant-asset-submodels-validated", manifest.get("scope").getAsString());
		assertTrue(manifest.get("fullR1Validation").getAsBoolean());
		assertTrue(manifest.get("javaConsumedProjectionValidated").getAsBoolean());
		JsonObject provenance = manifest.getAsJsonObject("provenance");
		assertEquals("aas-model", provenance.getAsJsonObject("authoringPackage").get("name").getAsString());
		assertEquals("0.2.0", provenance.getAsJsonObject("authoringPackage").get("version").getAsString());
		assertFalse(provenance.getAsJsonObject("modelSchemaSha256").entrySet().isEmpty());
		assertFalse(provenance.getAsJsonObject("sources").entrySet().isEmpty());
		assertTrue(manifest.getAsJsonArray("missingR1Coverage").isEmpty());
		JsonObject resources = manifest.getAsJsonObject("resources");
		Set<String> names = new HashSet<>();
		for (JsonElement item : manifest.getAsJsonArray("cases")) {
			JsonObject fixture = item.getAsJsonObject();
			String name = fixture.get("name").getAsString();
			assertTrue(names.add(name), "duplicate fixture " + name);
			assertEquals(fixture.getAsJsonArray("missingStructure").isEmpty(),
					fixture.get("runtimeComplete").getAsBoolean(), "dependency closure " + name);
			String expectedOutcome = name.equals("stoppering") ? "reject-retired-declaration"
					: fixture.get("runtimeComplete").getAsBoolean() ? "resolve"
					: fixture.getAsJsonArray("missingStructure").toString().contains("type mismatch")
							? "reject-invalid-reference-target-type" : "reject-incomplete-reference-target";
			assertEquals(expectedOutcome, fixture.get("expectedOutcome").getAsString(), "expected authoring outcome " + name);
			assertFalse(fixture.get("ownership").getAsString().isBlank());
			for (String role : List.of("aimc", "aid", "owner", "expected")) {
				String path = fixture.get(role).getAsString();
				assertEquals(name + "/" + role + ".json", path);
				assertTrue(resources.has(path), "untracked resource " + path);
				json(path);
			}
			for (JsonElement dependencyElement : fixture.getAsJsonArray("dependencies")) {
				String path = name + "/" + dependencyElement.getAsJsonObject().get("path").getAsString();
				assertTrue(resources.has(path), "untracked dependency " + path);
				json(path);
			}
		}
		assertEquals(CASES, names, "13 descriptor cases plus the mandatory projection-extension fixture");
		assertEquals(CASES.size(), manifest.getAsJsonArray("cases").size());
		assertEquals(12L, manifest.getAsJsonArray("cases").asList().stream()
				.filter(item -> item.getAsJsonObject().get("caseClass").getAsString().equals("descriptor-contract")).count());
		assertEquals(1L, manifest.getAsJsonArray("cases").asList().stream()
				.filter(item -> item.getAsJsonObject().get("caseClass").getAsString().equals("unsupported-contract")).count());
		assertEquals(1L, manifest.getAsJsonArray("cases").asList().stream()
				.filter(item -> item.getAsJsonObject().get("caseClass").getAsString()
						.equals("cci-uuid-inout-compatibility")).count());
		for (var entry : resources.entrySet()) {
			byte[] bytes = resource(entry.getKey());
			JsonObject integrity = entry.getValue().getAsJsonObject();
			assertEquals(integrity.get("byteLength").getAsInt(), bytes.length, entry.getKey());
			assertEquals(integrity.get("sha256").getAsString(), sha256(bytes), entry.getKey());
		}
	}

	@Test
	public void identitiesOrderedReferencesBlobsAndAuthoredKeysSurviveCapture() throws Exception {
		JsonObject manifest = json("manifest.json");
		Set<String> accepted = strings(manifest.getAsJsonArray("acceptedSemanticIds"));
		assertTrue(accepted.containsAll(Set.of(
				"https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel",
				"https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel")));
		for (JsonElement caseElement : manifest.getAsJsonArray("cases")) {
			JsonObject fixture = caseElement.getAsJsonObject();
			String name = fixture.get("name").getAsString();
			if (name.equals("stoppering")) {
				JsonObject owner = json(fixture.get("owner").getAsString());
				assertTrue(owner.getAsJsonArray("assetAdministrationShells").isEmpty());
				Set<String> captured = new HashSet<>();
				for (String role : List.of("aimc", "aid"))
					captured.add(json(fixture.get(role).getAsString()).get("id").getAsString());
				for (JsonElement dependency : fixture.getAsJsonArray("dependencies")) {
					JsonObject index = dependency.getAsJsonObject();
					String path = "stoppering/" + index.get("path").getAsString();
					JsonObject raw = json(path);
					assertEquals(index.get("id"), raw.get("id"));
					captured.add(raw.get("id").getAsString());
					if (raw.get("idShort").getAsString().equals("Variables")) {
						for (String id : List.of("PackMLState", "OccupationState"))
							assertEquals("Property", child(raw.getAsJsonArray("submodelElements"), id)
									.get("modelType").getAsString(), id);
						var failure = assertThrows(org.eclipse.digitaltwin.basyx.dmp.management.model.WireProjectionException.class,
								() -> WireProjection.validate(submodel(text(path))));
						assertTrue(failure.getMessage().contains("retired writeDelegation qualifier"));
					} else if (raw.get("idShort").getAsString().equals("ControlComponentInstance")) {
						assertEquals(raw.get("id").getAsString(), submodel(text(path)).getId());
					}
				}
			assertEquals(captured.size(), fixture.getAsJsonArray("dependencies").size() + 2);
			continue;
		}
			JsonObject expected = json(name + "/expected.json");
			JsonObject ownerDocument = json(name + "/owner.json");
			JsonArray ownerShells = ownerDocument.getAsJsonArray("assetAdministrationShells");
			JsonArray expectedOwners = fixture.getAsJsonArray("owners");
			assertEquals(expectedOwners.size(), ownerShells.size(), name + " owner shell count");
			Set<String> owned = new HashSet<>();
			for (int i = 0; i < ownerShells.size(); i++) {
				JsonObject shell = ownerShells.get(i).getAsJsonObject();
				JsonObject expectedOwner = expectedOwners.get(i).getAsJsonObject();
				assertEquals("AssetAdministrationShell", shell.get("modelType").getAsString());
				assertEquals(expectedOwner.get("id"), shell.get("id"));
				assertEquals(expectedOwner.get("idShort"), shell.get("idShort"));
				Set<String> expectedOwned = strings(expectedOwner.getAsJsonArray("ownedSubmodelIds"));
				Set<String> shellOwned = new HashSet<>();
				for (JsonElement ref : shell.getAsJsonArray("submodels")) {
					assertEquals("ModelReference", ref.getAsJsonObject().get("type").getAsString());
					JsonArray keys = ref.getAsJsonObject().getAsJsonArray("keys");
					assertEquals(1, keys.size());
					assertEquals("Submodel", keys.get(0).getAsJsonObject().get("type").getAsString());
					String id = keys.get(0).getAsJsonObject().get("value").getAsString();
					assertTrue(shellOwned.add(id));
					assertTrue(owned.add(id));
				}
				assertEquals(expectedOwned, shellOwned, name + " exact shell ownership");
			}
			Set<String> capturedSubmodels = new HashSet<>();
			for (String role : List.of("aimc", "aid")) {
				capturedSubmodels.add(json(name + "/" + role + ".json").get("id").getAsString());
			}
			for (JsonElement dependency : fixture.getAsJsonArray("dependencies")) {
				JsonObject index = dependency.getAsJsonObject();
				String path = name + "/" + index.get("path").getAsString();
				JsonObject raw = json(path);
				assertEquals(index.get("id"), raw.get("id"), name + " dependency identity");
				assertEquals("Submodel", raw.get("modelType").getAsString());
				assertEquals(index.get("id").getAsString(), submodel(text(path)).getId(),
						name + " dependency projection");
				capturedSubmodels.add(raw.get("id").getAsString());
			}
			if (expectedOwners.isEmpty()) owned.addAll(capturedSubmodels);
			assertEquals(owned, capturedSubmodels, name + " owner references resolve to captured submodels");
			for (String role : List.of("aimc", "aid")) {
				JsonObject raw = json(name + "/" + role + ".json");
				assertEquals("Submodel", raw.get("modelType").getAsString());
				assertTrue(owned.contains(raw.get("id").getAsString()));
				for (var identity : expected.getAsJsonObject("models").getAsJsonObject(role).entrySet())
					assertEquals(identity.getValue(), raw.get(identity.getKey()), name + ": " + identity.getKey());
				for (JsonElement item : expected.getAsJsonObject("elements").getAsJsonArray(role)) {
					JsonObject record = item.getAsJsonObject();
					JsonObject element = at(raw, record.get("path").getAsString());
					for (String key : List.of("modelType", "idShort", "semanticId", "supplementalSemanticIds", "qualifiers")) {
						assertEquals(record.has(key), element.has(key), name + ": presence of " + key);
						if (record.has(key)) assertEquals(record.get(key), element.get(key), name + ": " + key);
					}
					if (element.has("semanticId"))
						for (JsonElement key : element.getAsJsonObject("semanticId").getAsJsonArray("keys"))
							assertTrue(accepted.contains(key.getAsJsonObject().get("value").getAsString()));
					if (record.has("reference")) assertEquals(record.get("reference"), element.get("value"), name + ": ordered keys");
					if (record.has("authoredKey")) assertEquals(record.get("authoredKey"), element.get("value"));
					if (record.has("base64")) {
						assertEquals(record.get("contentType"), element.get("contentType"));
						byte[] blob = Base64.getDecoder().decode(element.get("value").getAsString());
						assertArrayEquals(Base64.getDecoder().decode(record.get("base64").getAsString()), blob);
						assertEquals(record.get("byteLength").getAsInt(), blob.length);
						assertEquals(record.get("sha256").getAsString(), sha256(blob));
					}
				}
			}
			JsonArray mappings = child(json(name + "/aimc.json").getAsJsonArray("submodelElements"), "MappingConfigurations")
					.getAsJsonArray("value");
			assertEquals(expected.getAsJsonArray("authoredMappings").size(), mappings.size());
			for (JsonElement item : expected.getAsJsonArray("authoredMappings")) {
				JsonObject authored = item.getAsJsonObject();
				assertFalse(authored.get("idShort").getAsString().isBlank());
				JsonArray members = mappings.get(authored.get("index").getAsInt()).getAsJsonObject().getAsJsonArray("value");
				for (String kind : List.of("source", "sink")) {
					String capitalized = kind.equals("source") ? "Source" : "Sink";
					JsonArray ids = new JsonArray();
					for (JsonElement value : child(members, capitalized + "s").getAsJsonArray("value"))
						ids.add(child(value.getAsJsonObject().getAsJsonArray("value"), capitalized + "Id").get("value"));
					assertEquals(authored.getAsJsonArray(kind + "Ids"), ids, name + ": authored keys");
				}
				if (!authored.get("transformationBase64").isJsonNull())
					assertArrayEquals(Base64.getDecoder().decode(authored.get("transformationBase64").getAsString()),
							Base64.getDecoder().decode(child(members, "Transformation").get("value").getAsString()));
			}
		}
	}

	@Test
	public void everyRawCaseParsesThroughTheJavaConsumedProjection() throws Exception {
		JsonObject manifest = json("manifest.json");
		for (JsonElement caseElement : manifest.getAsJsonArray("cases")) {
			JsonObject fixture = caseElement.getAsJsonObject();
			String name = fixture.get("name").getAsString();
			if (name.equals("stoppering")) {
				var aimcFailure = assertThrows(org.eclipse.digitaltwin.basyx.dmp.management.model.WireProjectionException.class,
						() -> WireProjection.aimc(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("aimc").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
				assertTrue(aimcFailure.getMessage().contains("retired writeDelegation qualifier"));
				assertEquals(json("stoppering/aid.json").get("id").getAsString(),
						WireProjection.aid(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("aid").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)).submodel().getId());
				JsonObject owner = json(fixture.get("owner").getAsString());
				assertTrue(owner.getAsJsonArray("assetAdministrationShells").isEmpty(),
						"Stoppering contract is submodels-only");
				JsonObject variables = null;
				for (JsonElement dependency : fixture.getAsJsonArray("dependencies")) {
					JsonObject index = dependency.getAsJsonObject();
					JsonObject candidate = json("stoppering/" + index.get("path").getAsString());
					if (candidate.get("idShort").getAsString().equals("Variables")) variables = candidate;
				}
			assertNotNull(variables);
				for (String id : List.of("PackMLState", "OccupationState"))
				assertEquals("Property", child(variables.getAsJsonArray("submodelElements"), id)
						.get("modelType").getAsString(), id);
				for (JsonElement mapping : json("stoppering/aimc.json")
						.getAsJsonArray("submodelElements").get(0).getAsJsonObject().getAsJsonArray("value")) {
					JsonObject sinks = child(mapping.getAsJsonObject().getAsJsonArray("value"), "Sinks");
					for (JsonElement sink : sinks.getAsJsonArray("value")) {
						JsonObject ref = child(sink.getAsJsonObject().getAsJsonArray("value"), "Sink")
								.getAsJsonObject().getAsJsonObject("value");
						if (ref.getAsJsonArray("keys").get(0).getAsJsonObject().get("value").getAsString()
								.endsWith("/Variables")) {
							JsonObject leaf = ref.getAsJsonArray("keys").get(ref.getAsJsonArray("keys").size() - 1).getAsJsonObject();
							assertEquals("Property", leaf.get("type").getAsString());
						}
					}
				}
				continue;
			}
			WireModel.Aimc aimc = WireProjection.aimc(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("aimc").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class));
			WireModel.Aid aid = WireProjection.aid(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("aid").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class));
			List<org.eclipse.digitaltwin.aas4j.v3.model.AssetAdministrationShell> owners = fixture.getAsJsonArray("owners").isEmpty() ? List.of()
					: org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("owner").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Environment.class)
							.getAssetAdministrationShells();
			JsonObject expected = json(fixture.get("expected").getAsString());

			assertEquals(expected.getAsJsonObject("models").getAsJsonObject("aimc")
					.get("idShort").getAsString(), aimc.submodel().getIdShort(), name);
			assertEquals("https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel",
					aimc.submodel().getSemanticId().getKeys().get(0).getValue(), name);
			assertEquals("https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel",
					aid.submodel().getSemanticId().getKeys().get(0).getValue(), name);
			assertEquals(expected.getAsJsonArray("authoredMappings").size(), aimc.mappings().size(), name);
			assertFalse(aid.interfaces().isEmpty(), name + " has no projected interfaces");
			JsonArray aidElements = json(fixture.get("aid").getAsString()).getAsJsonArray("submodelElements");
			JsonArray wireInterfaces = new JsonArray();
			for (JsonElement element : aidElements) {
				JsonObject node = element.getAsJsonObject();
				if (node.has("semanticId") && node.getAsJsonObject("semanticId").toString()
						.contains("AssetInterfacesDescription/1/0/Interface")) wireInterfaces.add(node);
			}
			assertEquals(wireInterfaces.size(), aid.interfaces().size(), name + " interface count");
			for (var projectedInterface : aid.interfaces()) {
				JsonObject rawInterface = null;
				for (JsonElement element : wireInterfaces) {
					if (projectedInterface.idShort().equals(element.getAsJsonObject().get("idShort").getAsString())) {
						rawInterface = element.getAsJsonObject();
						break;
					}
				}
				assertNotNull(rawInterface, name + " protocol interface " + projectedInterface.idShort());
				List<String> expectedSupplements = new java.util.ArrayList<>();
				if (rawInterface.has("supplementalSemanticIds")) {
					for (JsonElement ref : rawInterface.getAsJsonArray("supplementalSemanticIds"))
						for (JsonElement key : ref.getAsJsonObject().getAsJsonArray("keys"))
							expectedSupplements.add(key.getAsJsonObject().get("value").getAsString());
				}
				assertEquals(expectedSupplements, projectedInterface.supplementalSemanticIds(), name);
				assertEquals(expectedSupplements.stream().map(RawModelContractFixtureTest::protocolFamily)
						.filter(java.util.Objects::nonNull).distinct().toList(), projectedInterface.protocolFamilies(), name);
			}
			if (name.equals("projection-extensions")) {
				var dual = aid.interfaces().get(0);
				assertEquals(List.of(WireModel.ProtocolFamily.MQTT, WireModel.ProtocolFamily.HTTP),
						dual.protocolFamilies());
				assertTrue(dual.actionInputSchema().present());
				assertFalse(dual.actionInputSchema().value().type().present());
			}
			assertEquals(fixture.getAsJsonArray("owners").size(), owners.size(), name);
			for (int i = 0; i < owners.size(); i++) {
				JsonObject expectedOwner = fixture.getAsJsonArray("owners").get(i).getAsJsonObject();
				assertEquals(expectedOwner.get("id").getAsString(), owners.get(i).getId(), name);
				assertEquals(expectedOwner.get("idShort").getAsString(), owners.get(i).getIdShort(), name);
				assertEquals(strings(expectedOwner.getAsJsonArray("ownedSubmodelIds")),
						owners.get(i).getSubmodels().stream().map(reference -> reference.getKeys().get(0).getValue())
								.collect(java.util.stream.Collectors.toSet()), name);
			}
			assertEquals(name, fixture.get("name").getAsString());
			assertExpectedProjection(expected.getAsJsonObject("elements").getAsJsonArray("aimc"),
					aimc.submodel().getSubmodelElements(), "aimc", name);
			assertExpectedProjection(expected.getAsJsonObject("elements").getAsJsonArray("aid"),
					aid.submodel().getSubmodelElements(), "aid", name);

			JsonArray authoredMappings = expected.getAsJsonArray("authoredMappings");
			JsonArray rawMappings = child(json(fixture.get("aimc").getAsString())
					.getAsJsonArray("submodelElements"), "MappingConfigurations").getAsJsonArray("value");
			for (int i = 0; i < aimc.mappings().size(); i++) {
				WireModel.Mapping mapping = aimc.mappings().get(i);
				assertEquals(i, mapping.listIndex(), name);
				JsonObject authored = authoredMappings.get(i).getAsJsonObject();
				JsonObject rawMapping = rawMappings.get(i).getAsJsonObject();
				assertEquals(rawMapping.has("idShort") ? rawMapping.get("idShort").getAsString() : null,
						mapping.authoredId().orElse(null), name + " wire mapping identity");
				assertEquals(authored.getAsJsonArray("sourceIds").size(), mapping.sources().size(), name);
				assertEquals(authored.getAsJsonArray("sinkIds").size(), mapping.sinks().size(), name);
				assertEquals(stringList(authored.getAsJsonArray("sourceIds")),
						mapping.sources().stream().map(WireModel.Endpoint::id).toList(), name);
				assertEquals(stringList(authored.getAsJsonArray("sinkIds")),
						mapping.sinks().stream().map(WireModel.Endpoint::id).toList(), name);
				for (var source : mapping.sources()) assertEndpoint(source, name);
				for (var sink : mapping.sinks()) assertEndpoint(sink, name);
				assertEquals(hasMember(rawMapping, "DefaultPollingInterval"), mapping.defaultPollingInterval().present(), name);
				assertEquals(hasMember(rawMapping, "Transformation"), mapping.transformation().present(), name);
				assertEquals(hasMember(rawMapping, "ResponseSources"), mapping.responseSources().present(), name);
				assertEquals(hasMember(rawMapping, "ResponseSinks"), mapping.responseSinks().present(), name);
				assertEquals(hasMember(rawMapping, "ResponseTransformation"), mapping.responseTransformation().present(), name);
				assertEquals(hasMember(rawMapping, "ExecutionSemantics"), mapping.executionSemantics().present(), name);
				JsonObject execution = member(rawMapping, "ExecutionSemantics");
				assertEquals(execution != null && hasMember(execution, "TriggerSourceIds"),
						mapping.triggerSourceIds().present(), name);
				assertEquals(stringList(authored.getAsJsonArray("responseSourceIds")),
						mapping.responseSources().present() ? mapping.responseSources().value().stream().map(WireModel.Endpoint::id).toList() : List.of(), name);
				assertEquals(stringList(authored.getAsJsonArray("responseSinkIds")),
						mapping.responseSinks().present() ? mapping.responseSinks().value().stream().map(WireModel.Endpoint::id).toList() : List.of(), name);
				assertEquals(stringList(authored.getAsJsonArray("triggerSourceIds")),
						mapping.triggerSourceIds().present() ? mapping.triggerSourceIds().value() : List.of(), name);
				JsonArray qualifiers = authored.getAsJsonArray("qualifiers");
				assertEquals(qualifiers.size(), mapping.element().getQualifiers().size(), name);
				for (int q = 0; q < qualifiers.size(); q++) {
					JsonObject expectedQualifier = qualifiers.get(q).getAsJsonObject();
					var actualQualifier = mapping.element().getQualifiers().get(q);
					JsonElement qualifierType = expectedQualifier.has("type")
							? expectedQualifier.get("type") : expectedQualifier.get("type_");
					assertEquals(qualifierType.getAsString(), actualQualifier.getType(), name);
					if (expectedQualifier.has("value")) {
						assertEquals(expectedQualifier.get("value").getAsString(), actualQualifier.getValue(), name);
					}
				}
				JsonElement expectedResponseBlob = authored.get("responseTransformationBase64");
				assertEquals(expectedResponseBlob != null && !expectedResponseBlob.isJsonNull(),
						mapping.responseTransformation().present(), name);
				if (expectedResponseBlob != null && !expectedResponseBlob.isJsonNull()) {
					assertEquals(expectedResponseBlob.getAsString(), java.util.Base64.getEncoder()
							.encodeToString(mapping.responseTransformation().value().bytes()), name);
				}
			}
		}
	}

	@Test
	public void javaMappingPlansResolveEveryCompleteSubmodelCaseAndRejectRetiredStoppering() throws Exception {
		JsonObject manifest = json("manifest.json");
		try (var repository = new FakeAasRepository("")) {
			var client = new AasRepositoryClient(repository.uri(), Duration.ofSeconds(1), Duration.ofSeconds(1), 10, 2, null);
			for (JsonElement caseElement : manifest.getAsJsonArray("cases")) {
				JsonObject fixture = caseElement.getAsJsonObject();
				String name = fixture.get("name").getAsString();
				if (name.equals("stoppering")) {
					var aimcFailure = assertThrows(org.eclipse.digitaltwin.basyx.dmp.management.model.WireProjectionException.class,
							() -> WireProjection.aimc(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("aimc").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
					assertTrue(aimcFailure.getMessage().contains("retired writeDelegation qualifier"));
					assertEquals(json("stoppering/aid.json").get("id").getAsString(),
							WireProjection.aid(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(text(fixture.get("aid").getAsString()), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)).submodel().getId());
					continue;
				}
				// The repository holds what aas-model publishes: resolved templates and delegation URLs.
				JsonObject context = json(name + "/expected.json").getAsJsonObject("compilerContext");
				String aasId = context.get("aas_id").getAsString(), aasIdShort = context.get("aas_id_short").getAsString();
				repository.models().clear();
				for (String role : List.of("aimc", "aid"))
					repository.put(PublishedModels.publish(text(name + "/" + role + ".json"), aasId, aasIdShort));
				for (JsonElement dependency : fixture.getAsJsonArray("dependencies"))
					repository.put(PublishedModels.publish(text(name + "/" + dependency.getAsJsonObject().get("path").getAsString()),
							aasId, aasIdShort));
				WireModel.Aimc aimc = WireProjection.aimc(org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(
						PublishedModels.publish(text(name + "/aimc.json"), aasId, aasIdShort), org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class));
				var plan = new MappingPlanCompiler(new AasReferenceResolver(client)).compile(aimc);
				if (fixture.get("runtimeComplete").getAsBoolean()) {
					assertEquals(AasRepositoryClient.Status.FOUND, plan.status(),
							name + " must compile from exact wire references: " + plan.detail());
					JsonArray authoredMappings = json(name + "/expected.json").getAsJsonArray("authoredMappings");
					assertEquals(authoredMappings.size(), plan.value().mappings().size(), name);
					for (int i = 0; i < authoredMappings.size(); i++) {
						MappingPlanCompiler.MappingPlan mapping = plan.value().mappings().get(i);
						JsonObject rawAimc = json(name + "/aimc.json");
						JsonObject mappingList = child(rawAimc.getAsJsonArray("submodelElements"), "MappingConfigurations");
						JsonObject rawMapping = mappingList.getAsJsonArray("value").get(i).getAsJsonObject();
						String expectedIdentity = rawMapping.has("idShort")
								? rawMapping.get("idShort").getAsString() : "mapping_" + i;
						assertEquals(expectedIdentity, mapping.identity(), name);
						JsonObject authored = authoredMappings.get(i).getAsJsonObject();
						assertEquals(stringList(authored.getAsJsonArray("sourceIds")),
								mapping.sources().stream().map(MappingPlanCompiler.EndpointPlan::id).toList(), name);
						assertEquals(stringList(authored.getAsJsonArray("sinkIds")),
								mapping.sinks().stream().map(MappingPlanCompiler.EndpointPlan::id).toList(), name);
					}
					JsonObject connectorContext = json(name + "/expected.json").getAsJsonObject("compilerContext");
					var runtime = new ConnectorConfigurationFactory.RuntimeSettings(
							URI.create(connectorContext.get("aas_base_url").getAsString()));
					var connectorResult = new ConnectorConfigurationFactory().createBundles(plan.value(), runtime);
						switch (fixture.get("connectorCompileOutcome").getAsString()) {
						case "route-configured" -> {
							assertTrue(connectorResult.issues().isEmpty(), name + " connector compilation: " + connectorResult.issues());
							assertEquals(plan.value().mappings().size(), connectorResult.bundles().size(), name);
							if (name.equals("stoppering-new-build")) {
								var bridges = connectorResult.bundles().stream()
									.flatMap(bundle -> bundle.configuration().getRoutes().stream())
									.filter(route -> "bridge".equals(route.getRouteTrigger()))
									.map(org.eclipse.digitaltwin.basyx.dmp.aas.operation.ProtocolBridgeRouteConfiguration::new)
									.toList();
								assertEquals(3, bridges.size(), "all three captured caller operations compile as bridge routes");
								for (var bridge : bridges) {
									assertEquals(Map.of("Uuid", "xs:string", "State", "xs:string"),
											bridge.getTriggerData().get("callerResponseTypes"));
									assertEquals(List.of("Uuid", "State"), bridge.getCallerOutputOrder());
								}
							}
						}
						case "unsupported-response-extension-contract", "unsupported-Operation-bridge-ingress-contract",
								"reject-missing-opcua-node-information" ->
							assertFalse(connectorResult.issues().isEmpty(), name + " must report the unimplemented route form");
						default -> fail(name + " has unexpected connector outcome " + fixture.get("connectorCompileOutcome"));
					}
				} else {
					assertEquals("reject-invalid-reference-target-type", fixture.get("expectedOutcome").getAsString());
					assertEquals(AasRepositoryClient.Status.INVALID_RESPONSE, plan.status(), name);
					assertTrue(plan.detail().contains("does not match element type") || plan.detail().contains("type mismatch"),
							name + " must retain the invalid Property/SMC target type: " + plan.detail());
				}
			}
		}
	}

	private static void assertEndpoint(WireModel.Endpoint endpoint, String context) {
		assertFalse(endpoint.id().isBlank(), context + " authored key");
		assertEquals("SubmodelElementCollection", org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.modelType(endpoint.element()), context);
		assertFalse(endpoint.reference().getKeys().isEmpty(), context + " ordered reference keys are required");
	}

	private static boolean hasMember(JsonObject mapping, String idShort) {
		return member(mapping, idShort) != null;
	}

	private static JsonObject member(JsonObject mapping, String idShort) {
		for (JsonElement element : mapping.getAsJsonArray("value")) {
			if (element.getAsJsonObject().has("idShort")
					&& idShort.equals(element.getAsJsonObject().get("idShort").getAsString())) return element.getAsJsonObject();
		}
		return null;
	}

	private static org.eclipse.digitaltwin.aas4j.v3.model.Submodel submodel(String json) {
		return org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(json, org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class);
	}

	private static void assertExpectedProjection(JsonArray expected, List<SubmodelElement> roots,
			String role, String context) throws Exception {
		Map<String, SubmodelElement> actualByPath = new LinkedHashMap<>();
		for (int i = 0; i < roots.size(); i++) index(actualByPath, roots.get(i), "/submodelElements/" + i);
		List<String> expectedPaths = new java.util.ArrayList<>();
		for (JsonElement entry : expected) {
			String path = entry.getAsJsonObject().get("path").getAsString();
			if (!path.isEmpty()) expectedPaths.add(path);
		}
		java.util.Set<String> missingPaths = new java.util.LinkedHashSet<>(expectedPaths);
		missingPaths.removeAll(actualByPath.keySet());
		java.util.Set<String> extraPaths = new java.util.LinkedHashSet<>(actualByPath.keySet());
		extraPaths.removeAll(expectedPaths);
		assertTrue(missingPaths.isEmpty() && extraPaths.isEmpty(), context + ": " + role
				+ " missing=" + missingPaths + " extra=" + extraPaths);
		for (JsonElement entry : expected) {
			JsonObject source = entry.getAsJsonObject();
			String path = source.get("path").getAsString();
			if (path.isEmpty()) continue;
			SubmodelElement actual = actualByPath.get(path);
			assertNotNull(actual, context + ": missing projected path " + role + path);
			assertEquals(source.get("modelType").getAsString(), org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.modelType(actual), context + path);
			assertEquals(source.has("idShort"), actual.getIdShort() != null, context + path + " idShort presence");
			if (source.has("idShort")) assertEquals(source.get("idShort").getAsString(), actual.getIdShort(), context + path);
			if (source.has("semanticId") && !source.get("semanticId").isJsonNull())
				assertReference(source.getAsJsonObject("semanticId"), actual.getSemanticId(), context + path);
			else assertNull(actual.getSemanticId(), context + path + " semanticId absence");
			JsonArray qualifiers = source.has("qualifiers") ? source.getAsJsonArray("qualifiers") : new JsonArray();
			var actualQualifiers = org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.nonNull(actual.getQualifiers());
			assertEquals(qualifiers.size(), actualQualifiers.size(), context + path + " qualifier count");
			for (int i = 0; i < qualifiers.size(); i++) {
				JsonObject qualifier = qualifiers.get(i).getAsJsonObject();
				var actualQualifier = actualQualifiers.get(i);
				assertEquals(qualifier.get("type").getAsString(), actualQualifier.getType(), context + path);
				if (qualifier.has("valueType") && !qualifier.get("valueType").isJsonNull())
					assertEquals(qualifier.get("valueType").getAsString(), org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.xsd(actualQualifier.getValueType()), context + path);
				if (qualifier.has("value") && !qualifier.get("value").isJsonNull())
					assertEquals(qualifier.get("value").getAsString(), actualQualifier.getValue(), context + path);
			}
			if (source.has("supplementalSemanticIds")) {
				JsonArray supplements = source.getAsJsonArray("supplementalSemanticIds");
				assertEquals(supplements.size(), actual.getSupplementalSemanticIds().size(), context + path + " supplemental semantics retained");
				for (int i = 0; i < supplements.size(); i++)
					assertReference(supplements.get(i).getAsJsonObject(), actual.getSupplementalSemanticIds().get(i), context + path);
			}
			if (source.has("reference")) assertReference(source.getAsJsonObject("reference"),
					((org.eclipse.digitaltwin.aas4j.v3.model.ReferenceElement) actual).getValue(), context + path);
			if (source.has("base64")) {
				var blob = (org.eclipse.digitaltwin.aas4j.v3.model.Blob) actual;
				byte[] bytes = blob.getValue();
				assertEquals(source.get("base64").getAsString(), Base64.getEncoder().encodeToString(bytes), context + path + " Blob bytes");
				assertEquals(source.get("byteLength").getAsInt(), bytes.length, context + path);
				assertEquals(source.get("sha256").getAsString(),
						HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), context + path);
				assertEquals(source.get("contentType").getAsString(), blob.getContentType(), context + path);
			}
		}
	}

	private static void index(Map<String, SubmodelElement> paths, SubmodelElement element, String path) {
		paths.put(path, element);
		List<SubmodelElement> children = org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.children(element);
		for (int i = 0; i < children.size(); i++) index(paths, children.get(i), path + "/value/" + i);
	}

	private static void assertReference(JsonObject expected, org.eclipse.digitaltwin.aas4j.v3.model.Reference actual, String context) {
		assertNotNull(actual, context + " reference");
		assertEquals(expected.get("type").getAsString(),
				com.google.common.base.CaseFormat.UPPER_UNDERSCORE.to(com.google.common.base.CaseFormat.UPPER_CAMEL, actual.getType().name()),
				context + " reference type");
		JsonArray keys = expected.getAsJsonArray("keys");
		assertEquals(keys.size(), actual.getKeys().size(), context + " reference key count");
		for (int i = 0; i < keys.size(); i++) {
			JsonObject key = keys.get(i).getAsJsonObject();
			assertEquals(key.get("type").getAsString(), org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.keyType(actual.getKeys().get(i)), context + " reference key type " + i);
			assertEquals(key.get("value").getAsString(), actual.getKeys().get(i).getValue(), context + " reference key value " + i);
		}
	}

	private static Object javaValue(JsonElement json) {
		return JsonCodec.parseJson(json.toString());
	}

	private static WireModel.ProtocolFamily protocolFamily(String semanticId) {
		return switch (semanticId) {
			case "http://www.w3.org/2011/mqtt" -> WireModel.ProtocolFamily.MQTT;
			case "http://www.w3.org/2011/http" -> WireModel.ProtocolFamily.HTTP;
			case "http://www.w3.org/2011/modbus" -> WireModel.ProtocolFamily.MODBUS;
			case "http://opcfoundation.org/UA/WoT-Binding/" -> WireModel.ProtocolFamily.OPCUA;
			case "http://www.w3.org/2022/bacnet" -> WireModel.ProtocolFamily.BACNET;
			case "https://admin-shell.io/idta/AssetInterfacesDescription/1/1/IO-Link" -> WireModel.ProtocolFamily.IOLINK;
			default -> null;
		};
	}

	private static Set<String> strings(JsonArray values) {
		Set<String> result = new HashSet<>();
		for (JsonElement value : values) result.add(value.getAsString());
		return result;
	}

	private static List<String> stringList(JsonArray values) {
		List<String> result = new java.util.ArrayList<>();
		for (JsonElement value : values) result.add(value.getAsString());
		return result;
	}

	private static JsonObject at(JsonObject root, String path) {
		JsonElement value = root;
		if (!path.isEmpty()) for (String token : path.substring(1).split("/"))
			value = value.isJsonArray() ? value.getAsJsonArray().get(Integer.parseInt(token)) : value.getAsJsonObject().get(token);
		return value.getAsJsonObject();
	}

	private static JsonObject child(JsonArray values, String idShort) {
		for (JsonElement value : values) {
			JsonObject object = value.getAsJsonObject();
			if (object.has("idShort") && idShort.equals(object.get("idShort").getAsString())) return object;
		}
		throw new AssertionError("mandatory authored element missing: " + idShort);
	}
}
