/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.execution.policy;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration;
import org.junit.jupiter.api.Test;

class AasPropertyEventProcessorTest extends CamelTestSupport {

	static final class EventSource extends DataSourceConfiguration {
		EventSource() {
			this("Property", "Runtime.State");
		}

		EventSource(String modelType, String elementPath) {
			super("event-source", "kafka", 9092);
			setEventFilter(Map.of(
					"submodelId", "urn:test:sm",
					"elementPath", elementPath,
					"modelType", modelType));
		}

		@Override
		public String getConnectionURI() {
			return "direct:event-source";
		}
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		EventSource source = new EventSource();
		return new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:event-source")
						.process(new AasPropertyEventProcessor(source))
						.filter(exchange -> Boolean.TRUE.equals(
							exchange.getProperty(AasPropertyEventProcessor.MATCH_PROPERTY)))
						.to("mock:matched");
				from("direct:smc-event-source")
						.process(new AasPropertyEventProcessor(
								new EventSource("SubmodelElementCollection", "Runtime.State")))
						.filter(exchange -> Boolean.TRUE.equals(
							exchange.getProperty(AasPropertyEventProcessor.MATCH_PROPERTY)))
						.to("mock:smc-matched");
			}
		};
	}

	@Test
	void matchingPropertyEventContributesItsValue() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:matched");
		matched.expectedMessageCount(1);
		matched.expectedBodiesReceived("RUNNING");

		template.sendBody("direct:event-source", """
				{"type":"SME_UPDATED","id":"urn:test:sm",
				 "smElementPath":"Runtime.State",
				 "smElement":{"modelType":"Property","value":"RUNNING"}}
				""");

		matched.assertIsSatisfied();
	}

	@Test
	void unrelatedPropertyEventDoesNotReachMapping() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:matched");
		matched.expectedMessageCount(0);

		template.sendBody("direct:event-source", """
				{"type":"SME_UPDATED","id":"urn:test:sm",
				 "smElementPath":"Runtime.Other",
				 "smElement":{"modelType":"Property","value":"STOPPED"}}
				""");

		matched.assertIsSatisfied();
	}

	@Test
	void fullSubmodelUpdateIsFilteredToTheTargetProperty() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:matched");
		matched.expectedMessageCount(1);
		matched.expectedBodiesReceived("READY");

		template.sendBody("direct:event-source", """
				{"type":"SM_UPDATED","id":"urn:test:sm",
				 "submodel":{"submodelElements":[
				   {"idShort":"Runtime","value":[
				     {"idShort":"State","modelType":"Property","value":"READY"}
				   ]}
				 ]}}
				""");

		matched.assertIsSatisfied();
	}

	@Test
	void collectionEventKeepsTheCompleteElementJson() throws Exception {
		MockEndpoint matched = getMockEndpoint("mock:smc-matched");
		matched.expectedMessageCount(1);

		template.sendBody("direct:smc-event-source", """
				{"type":"SME_UPDATED","id":"urn:test:sm",
				 "smElementPath":"Runtime.State",
				 "smElement":{"modelType":"SubmodelElementCollection",
				              "idShort":"State","value":[{"idShort":"Mode","value":"READY"}]}}
				""");

		matched.assertIsSatisfied();
		Map<?, ?> element = matched.getExchanges().get(0).getMessage().getBody(Map.class);
		assertEquals("SubmodelElementCollection", element.get("modelType"));
		assertEquals("State", element.get("idShort"));
		assertEquals(java.util.List.of(Map.of("idShort", "Mode", "value", "READY")), element.get("value"));
	}
}
