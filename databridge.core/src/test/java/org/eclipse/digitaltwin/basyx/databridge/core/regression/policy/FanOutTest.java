package org.eclipse.digitaltwin.basyx.databridge.core.regression.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.FanOut;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.junit.jupiter.api.Test;

class FanOutTest extends CamelTestSupport {
	private final AtomicInteger sharedTransforms = new AtomicInteger();

	static final class Sink extends DataSinkConfiguration {
		private final String uri;

		Sink(String uniqueId, String key, String uri) {
			super(uniqueId);
			setSinkId(key);
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	static final class Transformer extends DataTransformerConfiguration {
		private final String uri;

		Transformer(String uniqueId, String uri) {
			super(uniqueId);
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() {
				RoutesConfiguration configs = new RoutesConfiguration(List.of(),
						List.of(new Transformer("shared", "direct:shared"),
								new Transformer("leftMap", "direct:leftMap"),
								new Transformer("rightMap", "direct:rightMap")),
						List.of(new Sink("left", "Left", "mock:left"),
								new Sink("right", "Right", "mock:right"),
								new Sink("customLeft", "Left", "mock:customLeft"),
								new Sink("customRight", "Right", "mock:customRight")), List.of());

				RouteConfiguration shared = new RouteConfiguration("execution-policy", List.of(),
						List.of("shared"), List.of("left", "right"));
				FanOut.apply(this, from("direct:sharedInput").routeId("sharedRoute"), configs, shared,
						RouteCreatorHelper.getDataTransformerEndpoints(configs, shared.getTransformers(), "sharedRoute"),
						RouteCreatorHelper.getDataSinkEndpoints(configs, shared.getDatasinks(), "sharedRoute"),
						null, "sharedRoute");

				RouteConfiguration custom = new RouteConfiguration("execution-policy", List.of(),
						List.of("leftMap", "rightMap"), List.of("customLeft", "customRight"),
						Map.of("customLeft", new String[] {"leftMap"},
								"customRight", new String[] {"rightMap"}));
				FanOut.apply(this, from("direct:customInput").routeId("customRoute"), configs, custom,
						RouteCreatorHelper.getDataTransformerEndpoints(configs, custom.getTransformers(), "customRoute"),
						RouteCreatorHelper.getDataSinkEndpoints(configs, custom.getDatasinks(), "customRoute"),
						RouteCreatorHelper.getDataSinkMapping(configs,
								custom.getDatasinkMappingConfiguration(), "customRoute"), "customRoute");

				from("direct:shared").process(exchange -> sharedTransforms.incrementAndGet());
				from("direct:leftMap").setBody().constant("{\"Left\":\"mapped left\"}");
				from("direct:rightMap").setBody().constant("{\"Right\":\"mapped right\"}");
			}
		};
	}

	@Test
	void routeTransformRunsOnceAndEachSinkReadsOriginalResult() throws Exception {
		MockEndpoint left = getMockEndpoint("mock:left");
		MockEndpoint right = getMockEndpoint("mock:right");
		left.expectedMessageCount(1);
		right.expectedMessageCount(1);
		template.sendBody("direct:sharedInput", "{\"Left\":\"one\",\"Right\":\"two\"}");
		MockEndpoint.assertIsSatisfied(context);
		assertEquals("one", left.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals("two", right.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals(1, sharedTransforms.get());
	}

	@Test
	void heterogeneousSinkTransformersRunOnIndependentBranches() throws Exception {
		MockEndpoint left = getMockEndpoint("mock:customLeft");
		MockEndpoint right = getMockEndpoint("mock:customRight");
		left.expectedMessageCount(1);
		right.expectedMessageCount(1);
		template.sendBody("direct:customInput", "{\"Left\":\"raw\",\"Right\":\"raw\"}");
		MockEndpoint.assertIsSatisfied(context);
		assertEquals("mapped left", left.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals("mapped right", right.getExchanges().get(0).getMessage().getBody(String.class));
	}
}
