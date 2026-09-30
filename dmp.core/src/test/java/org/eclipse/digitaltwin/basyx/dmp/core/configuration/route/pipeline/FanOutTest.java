package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

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
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
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
								new Sink("customRight", "Right", "mock:customRight"),
								new Sink("mixedMapped", null, "mock:mixedMapped"),
								new Sink("mixedRaw", null, "mock:mixedRaw"),
								new Sink("throwing", null, "mock:throwing"),
				new Sink("payloadSink", null, "mock:payloadSink"),
								new Sink("faultTolerant", null, "mock:faultTolerant")), List.of());

				RouteConfiguration shared = new RouteConfiguration("execution-policy", List.of(),
						List.of("shared"), List.of("left", "right"));
				FanOut.apply(from("direct:sharedInput").routeId("sharedRoute"), configs, shared,
						RouteCreatorHelper.getDataTransformerEndpoints(configs, shared.getTransformers(), "sharedRoute"),
						RouteCreatorHelper.getDataSinkEndpoints(configs, shared.getDatasinks(), "sharedRoute"),
						null, "sharedRoute");

				RouteConfiguration custom = new RouteConfiguration("execution-policy", List.of(),
						List.of("leftMap", "rightMap"), List.of("customLeft", "customRight"),
						Map.of("customLeft", new String[] {"leftMap"},
								"customRight", new String[] {"rightMap"}));
				FanOut.apply(from("direct:customInput").routeId("customRoute"), configs, custom,
						RouteCreatorHelper.getDataTransformerEndpoints(configs, custom.getTransformers(), "customRoute"),
						RouteCreatorHelper.getDataSinkEndpoints(configs, custom.getDatasinks(), "customRoute"),
						RouteCreatorHelper.getDataSinkMapping(configs,
								custom.getDatasinkMappingConfiguration(), "customRoute"), "customRoute");

				// one sink is mapped to a transformer, its sibling is not
				RouteConfiguration mixed = new RouteConfiguration("execution-policy", List.of(),
						List.of("leftMap"), List.of("mixedMapped", "mixedRaw"),
						Map.of("mixedMapped", new String[] {"leftMap"}));
				FanOut.apply(from("direct:mixedInput").routeId("mixedRoute"), configs, mixed,
						RouteCreatorHelper.getDataTransformerEndpoints(configs, mixed.getTransformers(), "mixedRoute"),
						RouteCreatorHelper.getDataSinkEndpoints(configs, mixed.getDatasinks(), "mixedRoute"),
						RouteCreatorHelper.getDataSinkMapping(configs,
								mixed.getDatasinkMappingConfiguration(), "mixedRoute"), "mixedRoute");

				RouteConfiguration payload = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("payloadSink"));
				FanOut.apply(from("direct:payloadInput").routeId("payloadRoute"), configs, payload,
						new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, payload.getDatasinks(), "payloadRoute"),
						null, "payloadRoute");

				// a failing sink must not stop its sibling
				RouteConfiguration fault = new RouteConfiguration("execution-policy", List.of(),
						List.of(), List.of("throwing", "faultTolerant"));
				FanOut.apply(from("direct:faultInput").routeId("faultRoute"), configs, fault,
						new String[0],
						RouteCreatorHelper.getDataSinkEndpoints(configs, fault.getDatasinks(), "faultRoute"),
						null, "faultRoute");

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

	/**
	 * A sink that maps to NO transformer must still receive the original
	 * transform result, not a body a sibling branch produced. The
	 * dot-aas-v3-multicast example relies on this: its raw sink sits beside four
	 * transformed sinks in one mapping, and none of the sinks declare a
	 * sinkId/valuePath (they are AAS sinks, which self-handle).
	 *
	 * <p>Two messages are sent on purpose: the example's failure only showed up
	 * on the SECOND fan-out, where each sink received its neighbour's body.
	 * That is a shared message across the multicast branches, not a
	 * configuration problem.
	 */
	@Test
	void unmappedSinkStillReceivesTheOriginalResult() throws Exception {
		MockEndpoint mapped = getMockEndpoint("mock:mixedMapped");
		MockEndpoint raw = getMockEndpoint("mock:mixedRaw");
		mapped.expectedMessageCount(2);
		raw.expectedMessageCount(2);
		template.sendBody("direct:mixedInput", "{\"Left\":\"raw\",\"Right\":\"raw\"}");
		template.sendBody("direct:mixedInput", "{\"Left\":\"raw2\",\"Right\":\"raw2\"}");
		MockEndpoint.assertIsSatisfied(context);
		assertEquals("{\"Left\":\"mapped left\"}",
				mapped.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals("{\"Left\":\"raw\",\"Right\":\"raw\"}",
				raw.getExchanges().get(0).getMessage().getBody(String.class));
		assertEquals("{\"Left\":\"raw2\",\"Right\":\"raw2\"}",
				raw.getExchanges().get(1).getMessage().getBody(String.class));
	}

	/**
	 * Every sink is attempted even when an earlier one fails, and the partial
	 * failure is then reported rather than passing as a successful write.
	 */
	/**
	 * A transform's JSON string output keeps its quoting on the way to the sink,
	 * while a plain device payload is passed through verbatim. Both arrive as a
	 * Java String, so only the payload's own shape can tell them apart - and
	 * re-serialising the plain one would turn IDLE into "IDLE".
	 */
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
					() -> producer.sendBody("direct:faultInput", "{\"Value\":\"v\"}"),
					"a sink failure must surface after all sinks were attempted");
		} finally {
			producer.stop();
		}

		// ...but the healthy sibling was still delivered
		MockEndpoint.assertIsSatisfied(context);
		assertEquals("{\"Value\":\"v\"}", ok.getExchanges().get(0).getMessage().getBody(String.class));
	}

	@Test
	void aJsonStringResultKeepsItsQuotesAndAPlainPayloadDoesNotGainThem() throws Exception {
		MockEndpoint sink = getMockEndpoint("mock:payloadSink");
		sink.expectedMessageCount(2);

		template.sendBody("direct:payloadInput", "\"103.55\"");
		template.sendBody("direct:payloadInput", "IDLE");

		MockEndpoint.assertIsSatisfied(context);
		assertEquals("\"103.55\"", sink.getExchanges().get(0).getMessage().getBody(String.class),
				"a JsonString result must keep the quoting the transform added");
		assertEquals("IDLE", sink.getExchanges().get(1).getMessage().getBody(String.class),
				"a plain device payload must pass through verbatim, not become \"IDLE\"");
	}
}
