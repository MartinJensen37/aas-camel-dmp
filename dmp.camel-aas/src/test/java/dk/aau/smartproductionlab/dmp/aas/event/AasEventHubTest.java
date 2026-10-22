/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.aas.configuration.AasEventDataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter;
import org.junit.jupiter.api.Test;

class AasEventHubTest extends CamelTestSupport {
	private static final String SUBMODEL = "https://example.org/aas/a/submodels/Variables";
	private final AasEventDataSourceConfiguration source =
			new AasEventDataSourceConfiguration("state", SUBMODEL, "Status.State", "http://127.0.0.1:1/unused");

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override public void configure() {
				from(source.getConnectionURI())
						.process(new AasPropertyEventAdapter().normalizer(source))
						.filter(exchange -> Boolean.TRUE.equals(exchange.getProperty(SourceEventAdapter.CONTRIBUTION_PROPERTY)))
						.to("mock:values");
			}
		};
	}

	private AasEventHub hub() { return AasEventComponent.hub(context); }

	@Test
	void deliversTheWatchedPropertyFromItsOwnAnAncestorsAndAWholeSubmodelEvent() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:values");
		values.expectedBodiesReceived("RUNNING", "STOPPED", "IDLE");
		assertTrue(hub().listensTo(SUBMODEL));
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status.State\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"RUNNING\"}}", Map.of());
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status\","
				+ "\"smElement\":{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"STOPPED\"}]}}", Map.of());
		hub().publish("{\"type\":\"SM_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"submodel\":{\"modelType\":\"Submodel\",\"id\":\"" + SUBMODEL
				+ "\",\"submodelElements\":[{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"Status\",\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"IDLE\"}]}]}}", Map.of());
		values.assertIsSatisfied(5000);
	}

	@Test
	void ignoresOtherElementsOtherSubmodelsAndDeletions() throws Exception {
		MockEndpoint values = getMockEndpoint("mock:values");
		values.expectedMessageCount(0);
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status.Mode\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"Mode\",\"valueType\":\"xs:string\",\"value\":\"x\"}}", Map.of());
		hub().publish("{\"type\":\"SME_UPDATED\",\"id\":\"urn:other\",\"smElementPath\":\"Status.State\","
				+ "\"smElement\":{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:string\",\"value\":\"x\"}}", Map.of());
		hub().publish("{\"type\":\"SME_DELETED\",\"id\":\"" + SUBMODEL + "\",\"smElementPath\":\"Status.State\"}", Map.of());
		hub().publish("not json", Map.of());
		values.assertIsSatisfied(500);
		assertFalse(hub().listensTo("urn:other"));
	}

	@Test
	void stoppingTheRouteUnregistersItsListener() throws Exception {
		context.getRouteController().stopRoute(context.getRoutes().get(0).getRouteId());
		assertFalse(hub().listensTo(SUBMODEL));
	}
}
