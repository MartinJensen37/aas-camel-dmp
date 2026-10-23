package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorHelper;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
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
						List.of(new Transformer("shared", "direct:shared")),
						List.of(new Sink("left", "Left", "mock:left"),
								new Sink("right", "Right", "mock:right"),
								new Sink("throwing", null, "mock:throwing"),
				new Sink("payloadSink", null, "mock:payloadSink"),
								new Sink("faultTolerant", null, "mock:faultTolerant")), List.of());

				RouteConfiguration shared = new RouteConfiguration("execution-policy", List.of(),
						List.of("shared"), List.of("left", "right"));
				FanOut.apply(from("direct:sharedInput").routeId("sharedRoute"), configs, shared,
						RouteCreatorHelper.getDataTransformerEndpoints(configs, shared.getTransformers(), "sharedRoute"),
						RouteCreatorHelper.getDataSinkEndpoints(configs, shared.getDatasinks(), "sharedRoute"),
						"sharedRoute");

				RouteConfiguration payload = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("payloadSink"));
				FanOut.apply(from("direct:payloadInput").routeId("payloadRoute"), configs, payload,
						new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, payload.getDatasinks(), "payloadRoute"),
						"payloadRoute");

				// a failing sink must not stop its sibling
				RouteConfiguration fault = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("throwing", "faultTolerant"));
				FanOut.apply(from("direct:faultInput").routeId("faultRoute"), configs, fault,
						new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, fault.getDatasinks(), "faultRoute"),
						"faultRoute");

				from("direct:shared").process(exchange -> sharedTransforms.incrementAndGet());
			}
		};
	}

	@Test
	void routeTransformRunsOnceAndEachSinkReadsOriginalResult() throws Exception {
		MockEndpoint left = getMockEndpoint("mock:left");
		MockEndpoint right = getMockEndpoint("mock:right");
		left.expectedMessageCount(1);
		right.expectedMessageCount(1);
		template.sendBody("direct:sharedInput", Map.of("Left", "one", "Right", "two"));
		MockEndpoint.assertIsSatisfied(context);
		assertEquals("one", left.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals("two", right.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals(1, sharedTransforms.get());
	}

	@Test
	void aFailingSinkDoesNotStopTheOthersButIsStillReported() throws Exception {
		MockEndpoint ok = getMockEndpoint("mock:faultTolerant");
		MockEndpoint broken = getMockEndpoint("mock:throwing");
		ok.expectedMessageCount(1);
		broken.expectedMessageCount(1);
		broken.whenExchangeReceived(1, (Processor) (Exchange ex) -> {
			throw new IllegalStateException("sink is down");
		});

		ProducerTemplate producer = context.createProducerTemplate();
		try {
			// the failure IS reported — a partial fan-out must not pass as success
			org.junit.jupiter.api.Assertions.assertThrows(
					org.apache.camel.CamelExecutionException.class,
					() -> producer.sendBody("direct:faultInput", Map.of("Value", "v")),
					"a sink failure must surface after all sinks were attempted");
		} finally {
			producer.stop();
		}

		// ...but the healthy sibling was still delivered
		MockEndpoint.assertIsSatisfied(context);
		assertEquals(Map.of("Value", "v"), ok.getExchanges().get(0).getMessage().getBody());
	}

	@Test
	void jsonLookingStringsRemainOpaqueAndDoNotGainQuotes() throws Exception {
		MockEndpoint sink = getMockEndpoint("mock:payloadSink");
		sink.expectedMessageCount(2);

		template.sendBody("direct:payloadInput", "103.55");
		template.sendBody("direct:payloadInput", "IDLE");

		MockEndpoint.assertIsSatisfied(context);
		assertEquals("103.55", sink.getExchanges().get(0).getMessage().getBody(String.class),
				"a JSON-looking String must not be reparsed or requoted");
		assertEquals("IDLE", sink.getExchanges().get(1).getMessage().getBody(String.class),
				"a plain device payload must pass through verbatim, not become \"IDLE\"");
	}
}
