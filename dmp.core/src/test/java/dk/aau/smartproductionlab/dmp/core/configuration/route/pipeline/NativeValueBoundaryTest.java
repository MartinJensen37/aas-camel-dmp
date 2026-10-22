package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter;
import dk.aau.smartproductionlab.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;

class NativeValueBoundaryTest {
	@Test
	void normalizedScalarsRemainNativeThroughFanIn() throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			DataSourceConfiguration source = new DataSourceConfiguration("endpoint", null, 0) {
				@Override
				public String getConnectionURI() {
					return "direct:source";
				}
			};
			source.setSourceId("AuthoredId");
			RouteConfiguration route = new RouteConfiguration("execution-policy", List.of("endpoint"),
					List.of(), List.of());
			try (FanIn fanIn = new FanIn(route, new RoutesConfiguration(List.of(source), List.of(), List.of(),
					List.of(route)), AffordanceValidation.Mode.ON)) {
				for (Object value : List.of(7L, 1.5, true, false, "103.5", "{\"value\":7}")) {
					var exchange = new DefaultExchange(context);
					exchange.setProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY, true);
					exchange.getMessage().setBody(value);
					fanIn.disassemble("endpoint").process(exchange);
					assertTrue(fanIn.contributed(exchange));
					assertTrue(fanIn.gatePredicate().matches(exchange));
					fanIn.merge().process(exchange);
					assertEquals(Map.of("AuthoredId", value), exchange.getMessage().getBody());
				}
			}
		}
	}

	@Test
	void controlledNullsAreValidatedAsNullAtEveryBoundary() {
		Map<String, Object> nullableSchema = Map.of("type", "object", "required", List.of("member"),
				"properties", Map.of("member", Map.of("type", "null")));
		Map<String, Object> payload = Map.of("member", JsonPointer.JSON_NULL);
		var validation = new AffordanceValidation(nullableSchema, "nullable", AffordanceValidation.Mode.ON);
		assertTrue(validation.incoming(payload));
		validation.outgoing(payload);
		validation.correlatedResponse(payload);
		assertFalse(validation.incoming(Map.of("member", "not-null")));

		var nonNull = new AffordanceValidation(Map.of("type", "object", "properties",
				Map.of("member", Map.of("type", "integer"))), "non-null", AffordanceValidation.Mode.ON);
		assertFalse(nonNull.incoming(payload));
		assertThrows(PayloadValidationException.class, () -> nonNull.outgoing(payload));
		assertThrows(PayloadValidationException.class, () -> nonNull.correlatedResponse(payload));
	}

	@Test
	void retiringAFanInBundleClearsItsReadinessSnapshot() throws Exception {
		DataSourceConfiguration first = source("first", "one");
		DataSourceConfiguration second = source("second", "two");
		RouteConfiguration route = new RouteConfiguration("execution-policy", List.of("first", "second"),
				List.of(), List.of());
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			try (FanIn fanIn = new FanIn(route, new RoutesConfiguration(List.of(first, second), List.of(), List.of(),
					List.of(route)))) {
				var firstExchange = new DefaultExchange(context);
				firstExchange.setProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY, true);
				firstExchange.getMessage().setBody(1L);
				fanIn.disassemble("first").process(firstExchange);
				assertFalse(fanIn.gatePredicate().matches(firstExchange));

				var secondExchange = new DefaultExchange(context);
				secondExchange.setProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY, true);
				secondExchange.getMessage().setBody(2L);
				fanIn.disassemble("second").process(secondExchange);
				assertTrue(fanIn.gatePredicate().matches(secondExchange));

				fanIn.close();
				var afterRetirement = new DefaultExchange(context);
				afterRetirement.setProperty(SourceEventAdapter.NORMALIZED_VALUE_PROPERTY, true);
				afterRetirement.getMessage().setBody(3L);
				fanIn.disassemble("second").process(afterRetirement);
				assertFalse(fanIn.gatePredicate().matches(afterRetirement));
			}
		}
	}

	private static DataSourceConfiguration source(String id, String sourceId) {
		DataSourceConfiguration source = new DataSourceConfiguration(id, null, 0) {
			@Override public String getConnectionURI() { return "direct:" + id; }
		};
		source.setSourceId(sourceId);
		return source;
	}
}
