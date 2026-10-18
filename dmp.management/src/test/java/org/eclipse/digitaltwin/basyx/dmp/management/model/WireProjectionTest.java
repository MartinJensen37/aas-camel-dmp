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
package org.eclipse.digitaltwin.basyx.dmp.management.model;

import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;
import static org.junit.jupiter.api.Assertions.*;

import org.eclipse.digitaltwin.aas4j.v3.model.Submodel;
import org.junit.jupiter.api.Test;

class WireProjectionTest {
	private static final String AIMC_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/2/0/Submodel";
	private static final String MAPPING_LIST_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesMappingConfiguration/1/0/MappingConfigurations";
	private static final String AID_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesDescription/1/1/Submodel";
	private static final String AID_INTERFACE_SEMANTIC = "https://admin-shell.io/idta/AssetInterfacesDescription/1/0/Interface";
	private static String submodel(String semantic, String elements) {
		return "{\"modelType\":\"Submodel\",\"id\":\"urn:test:sm\",\"idShort\":\"sm\",\"semanticId\":{"+
				"\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""+semantic+"\"}]},"+
				"\"submodelElements\":"+elements+"}";
	}
	private static Submodel sm(String json) { return Aas.read(json, Submodel.class); }
	private static String aimcWithItems(String items) {
		return submodel(AIMC_SEMANTIC,"[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"MappingConfigurations\",\"typeValueListElement\":\"SubmodelElementCollection\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""+MAPPING_LIST_SEMANTIC+"\"}]},\"value\":["+items+"]}]");
	}
	private static String mapping(String id) {
		String authored=id==null?"":"\"idShort\":\""+id+"\", ";
		return "{"+authored+"\"modelType\":\"SubmodelElementCollection\",\"value\":["+
				"{\"modelType\":\"SubmodelElementList\",\"idShort\":\"Sources\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":["+endpoint("Source")+"]},"+
				"{\"modelType\":\"SubmodelElementList\",\"idShort\":\"Sinks\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":["+endpoint("Sink")+"]}]}";
	}
	private static String endpoint(String role) {
		return "{\"modelType\":\"SubmodelElementCollection\",\"value\":["+
				"{\"modelType\":\"ReferenceElement\",\"idShort\":\""+role+"\",\"value\":{"+
				"\"type\":\"ModelReference\",\"keys\":[{\"type\":\"Submodel\",\"value\":\"urn:test:target\"}] }},"+
				"{\"modelType\":\"Property\",\"idShort\":\""+role+"Id\",\"valueType\":\"xs:string\",\"value\":\""+role+"-1\"}]}";
	}

	@Test void projectsOrderedReferencesAndMappingPositionFallback() {
		var parsed=WireProjection.aimc(sm(aimcWithItems(mapping(null))));
		assertEquals(1,parsed.mappings().size());
		assertEquals(0,parsed.mappings().get(0).listIndex());
		assertTrue(parsed.mappings().get(0).authoredId().isEmpty());
		assertEquals("urn:test:target",parsed.mappings().get(0).sources().get(0).reference().getKeys().get(0).getValue());
	}

	@Test void rejectsMissingMappingListWithPointer() {
		String json=submodel(AIMC_SEMANTIC,"[]");
		WireProjectionException e=assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(json)));
		assertEquals("/submodelElements",e.pointer());
	}

	@Test void rejectsUnsupportedSemanticVersion() {
		String json=submodel(AIMC_SEMANTIC.replace("/2/0/","/1/0/"),"[]");
		WireProjectionException e=assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(json)));
		assertEquals("/semanticId",e.pointer());
	}

	@Test void rejectsMalformedReferenceKeyAndDuplicateAuthoredIdentity() {
		String malformed=aimcWithItems(mapping(null).replace("\"type\":\"Submodel\",\"value\":\"urn:test:target\"","\"value\":\"urn:test:target\""));
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(malformed)));
		String duplicate=aimcWithItems(mapping("same")+","+mapping("same"));
		assertTrue(assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(duplicate))).getMessage().contains("duplicate authored"));
	}

	@Test void rejectsUntypedConsumedProperties() {
		String untyped=mapping(null).replace("\"valueType\":\"xs:string\",\"value\":\"Source-1\"","\"value\":\"Source-1\"");
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(aimcWithItems(untyped))));
	}

	@Test void projectsTypedEndpointsAndAcceptsOmittedEmptyListValues() {
		String item=mapping(null).replace(endpoint("Source"), "").replace(endpoint("Sink"), "");
		var parsed=WireProjection.aimc(sm(aimcWithItems(item)));
		assertTrue(parsed.mappings().get(0).sources().isEmpty());
		assertTrue(parsed.mappings().get(0).sinks().isEmpty());
		var withEndpoint=WireProjection.aimc(sm(aimcWithItems(mapping(null)))).mappings().get(0).sources().get(0);
		assertEquals("Source-1",withEndpoint.id());
		assertEquals("urn:test:target",withEndpoint.reference().getKeys().get(0).getValue());
	}

	@Test void rejectsDuplicateEndpointIdsWithinMapping() {
		String duplicate=endpoint("Source")+","+endpoint("Source");
		String item=mapping(null).replace(endpoint("Source"),duplicate);
		assertTrue(assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(aimcWithItems(item))))
				.getMessage().contains("duplicate authored SourceId"));
	}

	@Test void isolatedProjectionRetainsMalformedMappingAndContinuesValidSiblings() {
		String malformed = mapping("broken").replace("\"idShort\":\"SourceId\"", "\"idShort\":\"Unexpected\"");
		var projected = WireProjection.aimcIsolated(sm(aimcWithItems(malformed + "," + mapping("good"))));
		assertEquals(java.util.List.of("good"), projected.mappings().stream()
				.map(mapping -> mapping.authoredId().orElseThrow()).toList());
		assertEquals(1, projected.mappingFailures().size());
		var failure = projected.mappingFailures().get(0);
		assertEquals("broken", failure.identity());
		assertEquals(0, failure.listIndex());
		assertTrue(failure.pointer().contains("/value/0/"));
		assertTrue(failure.references().stream().anyMatch(reference -> "Source".equals(reference.role())
				&& "urn:test:target".equals(reference.reference().getKeys().get(0).getValue())));
		assertThrows(WireProjectionException.class, () -> WireProjection.aimc(sm(aimcWithItems(malformed + "," + mapping("good")))));
	}

	@Test void retainsOptionalMappingFieldsAndEmptyBlobBytes() {
		String item=mapping("mapped").replaceFirst(java.util.regex.Pattern.quote("\"modelType\":\"SubmodelElementCollection\",\"value\":["),
				"\"modelType\":\"SubmodelElementCollection\",\"value\":[{"+
				"\"modelType\":\"Property\",\"idShort\":\"DefaultPollingInterval\",\"valueType\":\"xs:double\",\"value\":2.5},"+
				"{\"modelType\":\"Blob\",\"idShort\":\"Transformation\",\"contentType\":\"text/plain\",\"value\":\"\"},");
		var projected=WireProjection.aimc(sm(aimcWithItems(item))).mappings().get(0);
		assertEquals("2.5",projected.defaultPollingInterval().value());
		assertEquals("text/plain",projected.transformation().value().contentType());
		assertArrayEquals(new byte[0],projected.transformation().value().bytes());
	}

	@Test void preservesOptionalResponseExecutionAndQualifierFields() {
		String item=mapping("extended").replaceFirst(java.util.regex.Pattern.quote("\"modelType\":\"SubmodelElementCollection\",\"value\":["),
				"\"modelType\":\"SubmodelElementCollection\",\"qualifiers\":[{\"type\":\"contract\",\"valueType\":\"xs:string\",\"value\":\"v1\"}],\"value\":["+
				"{\"modelType\":\"SubmodelElementList\",\"idShort\":\"ResponseSources\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":["+endpoint("Source")+"]},"+
				"{\"modelType\":\"SubmodelElementList\",\"idShort\":\"ResponseSinks\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":["+endpoint("Sink")+"]},"+
				"{\"modelType\":\"Blob\",\"idShort\":\"ResponseTransformation\",\"contentType\":\"application/json\",\"value\":\"e30=\"},");
		var mapping=WireProjection.aimc(sm(aimcWithItems(item))).mappings().get(0);
		assertEquals("Source-1",mapping.responseSources().value().get(0).id());
		assertEquals("Sink-1",mapping.responseSinks().value().get(0).id());
		assertArrayEquals(new byte[] {'{','}'},mapping.responseTransformation().value().bytes());
		assertFalse(mapping.executionSemantics().present());
		assertEquals("contract",mapping.element().getQualifiers().get(0).getType());
		assertEquals("v1",mapping.element().getQualifiers().get(0).getValue());
	}

	@Test void preservesUnusualReferenceKeys() {
		String unusual=aimcWithItems(mapping(null).replace("urn:test:target","urn:fixture:temperature-value"));
		assertEquals("urn:fixture:temperature-value",WireProjection.aimc(sm(unusual)).mappings().get(0)
				.sources().get(0).reference().getKeys().get(0).getValue());
	}

	@Test void projectsAidFormsEndpointAndRecursiveSchema() {
		String aid=submodel(AID_SEMANTIC,"""
				[{"modelType":"SubmodelElementCollection","idShort":"http","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"%s"}]},"supplementalSemanticIds":[{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"http://www.w3.org/2011/http"},{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/td"}]}],"value":[
				 {"modelType":"SubmodelElementCollection","idShort":"EndpointMetadata","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/EndpointMetadata"}]},"value":[{"modelType":"Property","idShort":"base","valueType":"xs:anyURI","value":"https://example.test"}]},
				 {"modelType":"SubmodelElementCollection","idShort":"InteractionMetadata","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/InteractionMetadata"}]},"value":[
				  {"modelType":"SubmodelElementCollection","idShort":"forms","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/td#hasForm"}]},"value":[{"modelType":"SubmodelElementCollection","value":[{"modelType":"Property","idShort":"href","valueType":"xs:string","value":"/status"},{"modelType":"Property","idShort":"htv_methodName","valueType":"xs:string","value":"GET"}]}]},
				  {"modelType":"SubmodelElementCollection","idShort":"DataSchema","value":[{"modelType":"Property","idShort":"type","valueType":"xs:string","value":"object"},{"modelType":"SubmodelElementCollection","idShort":"properties","value":[{"modelType":"SubmodelElementCollection","idShort":"state","value":[{"modelType":"Property","idShort":"type","valueType":"xs:string","value":"string"}]}]},{"modelType":"SubmodelElementList","idShort":"required","typeValueListElement":"Property","value":[{"modelType":"Property","valueType":"xs:string","value":"state"}]}]}
				 ]}]}]
				 """.formatted(AID_INTERFACE_SEMANTIC));
		var projected=WireProjection.aid(sm(aid)).interfaces().get(0);
		assertEquals(AID_INTERFACE_SEMANTIC,projected.familySemanticId());
		assertEquals(java.util.List.of(WireModel.ProtocolFamily.HTTP),projected.protocolFamilies());
		assertTrue(projected.endpointMetadata().present());
		assertTrue(projected.interactionMetadata().present());
		assertEquals("/status",projected.forms().get(0).href().value());
		assertEquals("GET",projected.forms().get(0).httpMethod().value());
		assertEquals("string",projected.schemas().get(0).properties().get("state").type().value());
		assertEquals(java.util.List.of("state"),projected.schemas().get(0).required().value());
	}

	@Test void projectsTheOpListAndRejectsASingleOpProperty() {
		String list="{\"modelType\":\"SubmodelElementList\",\"idShort\":\"op\",\"typeValueListElement\":\"Property\",\"value\":["
				+"{\"modelType\":\"Property\",\"valueType\":\"xs:string\",\"value\":\"readProperty\"},"
				+"{\"modelType\":\"Property\",\"valueType\":\"xs:string\",\"value\":\"observeproperty\"}]}";
		assertEquals(java.util.List.of("readproperty","observeproperty"),
				WireProjection.aid(sm(aidWithFormOp(list))).interfaces().get(0).forms().get(0).operations());
		String single="{\"modelType\":\"Property\",\"idShort\":\"op\",\"valueType\":\"xs:string\",\"value\":\"readproperty\"}";
		var failure=assertThrows(WireProjectionException.class,() -> WireProjection.aid(sm(aidWithFormOp(single))));
		assertTrue(failure.getMessage().contains("op must be a list"),failure.getMessage());
	}

	private static String aidWithFormOp(String op) {
		return submodel(AID_SEMANTIC,"""
				[{"modelType":"SubmodelElementCollection","idShort":"http","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"%s"}]},"supplementalSemanticIds":[{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"http://www.w3.org/2011/http"}]}],"value":[
				 {"modelType":"SubmodelElementCollection","idShort":"InteractionMetadata","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/InteractionMetadata"}]},"value":[
				  {"modelType":"SubmodelElementCollection","idShort":"forms","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/td#hasForm"}]},"value":[{"modelType":"Property","idShort":"href","valueType":"xs:string","value":"/status"},%s]}
				 ]}]}]
				 """.formatted(AID_INTERFACE_SEMANTIC,op));
	}

	@Test void acceptsAid12AndRejectsAid10() {
		String elements="""
				[{"modelType":"SubmodelElementCollection","idShort":"http","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"%s"}]},"supplementalSemanticIds":[{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"http://www.w3.org/2011/http"},{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/td"}]}],"value":[
				 {"modelType":"SubmodelElementCollection","idShort":"EndpointMetadata","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/EndpointMetadata"}]},"value":[{"modelType":"Property","idShort":"base","valueType":"xs:anyURI","value":"https://example.test"},{"modelType":"Property","idShort":"protocolVersion","valueType":"xs:string","value":"1.1"}]},
				 {"modelType":"SubmodelElementCollection","idShort":"InteractionMetadata","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://admin-shell.io/idta/AssetInterfacesDescription/1/0/InteractionMetadata"}]},"value":[
				  {"modelType":"SubmodelElementCollection","idShort":"forms","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/td#hasForm"}]},"value":[{"modelType":"SubmodelElementCollection","value":[{"modelType":"Property","idShort":"href","valueType":"xs:string","value":"/status"}]}]},
				  {"modelType":"SubmodelElementCollection","idShort":"DataSchema","value":[{"modelType":"Property","idShort":"type","valueType":"xs:string","value":"object"},{"modelType":"SubmodelElementCollection","idShort":"properties","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/json-schema#properties"}]},"value":[{"modelType":"SubmodelElementCollection","idShort":"state","value":[{"modelType":"Property","idShort":"type","valueType":"xs:string","value":"string"}]}]}]}
				 ]}]}]
				 """.formatted(AID_INTERFACE_SEMANTIC);
		Submodel aid12=sm(submodel("https://admin-shell.io/idta/AssetInterfacesDescription/1/2/Submodel",elements));
		assertTrue(WireProjection.isAid(aid12));
		var projected=WireProjection.aid(aid12).interfaces().get(0);
		assertEquals(java.util.List.of(WireModel.ProtocolFamily.HTTP),projected.protocolFamilies());
		assertEquals("/status",projected.forms().get(0).href().value());
		assertEquals("string",projected.schemas().get(0).properties().get("state").type().value());
		Submodel aid10=sm(submodel("https://admin-shell.io/idta/AssetInterfacesDescription/1/0/Submodel",elements));
		assertFalse(WireProjection.isAid(aid10));
		assertThrows(WireProjectionException.class,()->WireProjection.aid(aid10));
	}

	@Test void preservesDualBindingsAndRejectsUnknownInterfaceFamilies() {
		String valid=submodel(AID_SEMANTIC,"[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"dual\",\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""+AID_INTERFACE_SEMANTIC+"\"}]},\"supplementalSemanticIds\":[{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"http://www.w3.org/2011/http\"}]},{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"http://www.w3.org/2011/mqtt\"}]}],\"value\":[]}]");
		assertEquals(java.util.List.of(WireModel.ProtocolFamily.HTTP,WireModel.ProtocolFamily.MQTT),WireProjection.aid(sm(valid)).interfaces().get(0).protocolFamilies());
		String unknown=valid.replace("http://www.w3.org/2011/http","urn:unsupported:binding")
				.replace("http://www.w3.org/2011/mqtt","urn:another:binding");
		assertThrows(WireProjectionException.class,()->WireProjection.aid(sm(unknown)));
	}

	@Test void preservesTypeLessSchemasAndUnusualIdentifiers() {
		String aid=submodel(AID_SEMANTIC,"""
				[{"modelType":"SubmodelElementCollection","idShort":"interface_\\u6e29",
				  "semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"%s"}]},
				  "supplementalSemanticIds":[{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"http://www.w3.org/2011/http"}]}],
				  "value":[{"modelType":"SubmodelElementCollection","idShort":"InteractionMetadata","value":[
				    {"modelType":"SubmodelElementCollection","idShort":"properties","value":[
				      {"modelType":"SubmodelElementCollection","idShort":"temperature-value","semanticId":{"type":"ExternalReference","keys":[{"type":"GlobalReference","value":"https://www.w3.org/2019/wot/td#PropertyAffordance"}]},"value":[
				        {"modelType":"SubmodelElementCollection","idShort":"DataSchema","value":[
				          {"modelType":"SubmodelElementCollection","idShort":"properties","value":[]}
				        ]}
				      ]}
				    ]}
				  ]}]}]
				""".formatted(AID_INTERFACE_SEMANTIC));
		var projected=WireProjection.aid(sm(aid)).interfaces().get(0);
		assertEquals("interface_\u6e29",projected.idShort());
		assertEquals(java.util.List.of(WireModel.ProtocolFamily.HTTP),projected.protocolFamilies());
		assertFalse(projected.properties().isEmpty());
		assertFalse(projected.schemas().get(0).type().present());
		assertEquals("temperature-value",projected.properties().get(0).idShort().value());
	}

	@Test void rejectsMalformedConsumedAidFormField() {
		String aid=submodel(AID_SEMANTIC,"[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"http\",\"semanticId\":{"+
				"\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""+AID_INTERFACE_SEMANTIC+"\"}]},\"value\":["+
				"{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"forms\",\"semanticId\":{"+
				"\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://www.w3.org/2019/wot/td#hasForm\"}]},\"value\":["+
				"{\"modelType\":\"SubmodelElementCollection\",\"value\":[{\"modelType\":\"SubmodelElementList\",\"idShort\":\"href\",\"value\":[]}]}]}]}]");
		assertThrows(WireProjectionException.class,()->WireProjection.aid(sm(aid)));
	}

	@Test void rejectsRetiredDeclarationsInFieldsQualifiersAndBlobSemantics() {
		for(String retired:java.util.List.of("ExecutionPolicy","writeDelegation","LuaMode","transformEngine","queryLanguage")) {
			String field="{\"modelType\":\"Property\",\"idShort\":\""+retired+"\",\"valueType\":\"xs:string\",\"value\":\"lua\"}";
			assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(aimcWithItems(mapping(null)
					.replace("\"value\":[","\"value\":["+field+",")))),retired);
		}
		String qualifier=mapping(null).replace("\"modelType\":\"SubmodelElementCollection\",\"value\":[",
				"\"modelType\":\"SubmodelElementCollection\",\"qualifiers\":[{\"type\":\"writeDelegation\"}],\"value\":[");
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(aimcWithItems(qualifier))));
		String retired=submodel(AIMC_SEMANTIC,"[{\"modelType\":\"Blob\",\"idShort\":\"Transformation\",\"contentType\":\"text/plain\",\"value\":\"\",\"supplementalSemanticIds\":[{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\"https://example.org/jsonata/1/0\"}]}]}]");
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(retired)));
	}

	@Test void rejectsDuplicateConsumedChildrenAndWrongListTypes() {
		String duplicate=mapping(null).replace("\"idShort\":\"Sources\"","\"idShort\":\"Sinks\"");
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(aimcWithItems(duplicate))));
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(aimcWithItems(mapping(null))
				.replace("\"typeValueListElement\":\"SubmodelElementCollection\"","\"typeValueListElement\":\"Property\""))));
	}

	@Test void valuelessEndpointListsProjectAsEmpty() {
		String absent=aimcWithItems(mapping(null)).replace(",\"value\":["+endpoint("Source")+"]", "");
		assertTrue(WireProjection.aimc(sm(absent)).mappings().get(0).sources().isEmpty());
	}

	@Test void rejectsNullConsumedSemanticInsteadOfThrowingNullPointerException() {
		String root=submodel(AIMC_SEMANTIC,"[]");
		String semantic="{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\",\"value\":\""+AIMC_SEMANTIC+"\"}]}";
		assertThrows(WireProjectionException.class,()->WireProjection.aimc(sm(root.replace(semantic,"null"))));
	}
}
