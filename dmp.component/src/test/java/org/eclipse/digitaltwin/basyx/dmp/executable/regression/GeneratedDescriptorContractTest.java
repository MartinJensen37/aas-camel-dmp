package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASDatasinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.AASPollingDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.factory.AASPollingConsumerDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.aas.configuration.factory.AASDataSinkDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.aas.operation.ProtocolBridgeRouteCreatorFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.execution.policy.ExecutionPolicyRouteCreatorFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.routebuilder.DmpRouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.HttpPollingDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.factory.HttpDataSinkDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration.factory.HttpPollingDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.kafka.configuration.KafkaDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.kafka.configuration.factory.KafkaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.LuaTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.factory.LuaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.factory.OpcuaDataSinkDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.factory.OpcuaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.MqttDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.factory.MqttDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.paho.configuration.factory.MqttDataSinkDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Plc4xDataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.Plc4xDataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.factory.Plc4xDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration.factory.Plc4xDataSinkDefaultConfigurationFactory;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Run the full matrix with -Ddmp.contract.dir pointing to the Python-generated fixture root. */
public class GeneratedDescriptorContractTest {
	private static final List<String> FIXTURES = List.of(
			"stoppering", "opcua-method", "http-operation", "plc4x",
			"aas-property-event", "aas-property-poll",
			"aas-collection-poll", "opcua-read", "http-poll", "fine-path",
			"fine-protocols", "fan-in-out", "grouped-action");

	@Test
	public void generatedLuaRunsOnDecodedJavaValues() throws Exception {
		String directory = System.getProperty("dmp.contract.dir");
		assumeTrue(directory != null, "Generate the Python AIMC/AID contract matrix first");
		Path configFile = Path.of(directory, "grouped-action", "luatransformer.json");
		LuaTransformerConfiguration requestLua = new LuaDefaultConfigurationFactory(
				configFile.toString(), getClass().getClassLoader()).create().stream()
						.map(LuaTransformerConfiguration.class::cast)
						.filter(configuration -> configuration.getUniqueId().endsWith("request_transformer"))
						.findFirst().orElseThrow();

		var context = new DefaultCamelContext();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:generatedLua").to(requestLua.getConnectionURI()).to("mock:generatedLua");
			}
		});
		context.start();
		try {
			var template = context.createProducerTemplate();
			template.sendBody("direct:generatedLua", Map.of("Invoke", Map.of("x", 10, "y", 20)));
			MockEndpoint output = context.getEndpoint("mock:generatedLua", MockEndpoint.class);
			output.expectedMessageCount(1);
			output.assertIsSatisfied();
			Map<?, ?> value = output.getExchanges().get(0).getMessage().getBody(Map.class);
			assertEquals(1L, ((Number) value.get("X")).longValue());
			assertEquals(2L, ((Number) value.get("Y")).longValue());
		} finally {
			context.stop();
		}
	}

	@Test
	public void generatedProtocolMatrixLoadsAndBinds() throws Exception {
		String directory = System.getProperty("dmp.contract.dir");
		assumeTrue(directory != null, "Generate the full Python AIMC/AID contract matrix first");
		ClassLoader loader = getClass().getClassLoader();
		Gson gson = new Gson();
		for (String name : FIXTURES) {
			Path config = Path.of(directory, name);
			assertTrue("missing generated fixture " + name, Files.isRegularFile(config.resolve("routes.json")));
			RoutesConfiguration routes = new RoutesConfiguration();
			routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
			if (Files.exists(config.resolve("mqttdatasource.json")))
				routes.addDatasources(new MqttDefaultConfigurationFactory(config.resolve("mqttdatasource.json").toString(), loader).create());
			if (Files.exists(config.resolve("opcuadatasource.json")))
				routes.addDatasources(new OpcuaDefaultConfigurationFactory(config.resolve("opcuadatasource.json").toString(), loader).create());
			if (Files.exists(config.resolve("httpdatasource.json")))
				routes.addDatasources(new HttpPollingDefaultConfigurationFactory(config.resolve("httpdatasource.json").toString(), loader).create());
			if (Files.exists(config.resolve("plc4xdatasource.json")))
				routes.addDatasources(new Plc4xDefaultConfigurationFactory(config.resolve("plc4xdatasource.json").toString(), loader).create());
			if (Files.exists(config.resolve("aaspollingdatasource.json")))
				routes.addDatasources(new AASPollingConsumerDefaultConfigurationFactory(config.resolve("aaspollingdatasource.json").toString(), loader).create());
			if (Files.exists(config.resolve("kafkadatasource.json")))
				routes.addDatasources(new KafkaDefaultConfigurationFactory(config.resolve("kafkadatasource.json").toString(), loader).create());
			if (Files.exists(config.resolve("luatransformer.json")))
				routes.addTransformers(new LuaDefaultConfigurationFactory(config.resolve("luatransformer.json").toString(), loader).create());
			if (Files.exists(config.resolve("aasdatasink.json")))
				routes.addDatasinks(new AASDataSinkDefaultConfigurationFactory(config.resolve("aasdatasink.json").toString(), loader).create());
			if (Files.exists(config.resolve("mqttdatasink.json")))
				routes.addDatasinks(new MqttDataSinkDefaultConfigurationFactory(config.resolve("mqttdatasink.json").toString(), loader).create());
			if (Files.exists(config.resolve("opcuadatasink.json")))
				routes.addDatasinks(new OpcuaDataSinkDefaultConfigurationFactory(config.resolve("opcuadatasink.json").toString(), loader).create());
			if (Files.exists(config.resolve("httpdatasink.json")))
				routes.addDatasinks(new HttpDataSinkDefaultConfigurationFactory(config.resolve("httpdatasink.json").toString(), loader).create());
			if (Files.exists(config.resolve("plc4xdatasink.json")))
				routes.addDatasinks(new Plc4xDataSinkDefaultConfigurationFactory(config.resolve("plc4xdatasink.json").toString(), loader).create());

			assertFalse(name + " has no routes", routes.getRoutes().isEmpty());
			for (RouteConfiguration route : routes.getRoutes()) {
				for (String id : route.getDatasources())
					assertNotNull(name + ": missing source " + id, routes.getDatasources().get(id));
				for (String id : route.getTransformers())
					assertNotNull(name + ": missing transformer " + id, routes.getTransformers().get(id));
				for (String id : route.getDatasinks())
					assertNotNull(name + ": missing sink " + id, routes.getDatasinks().get(id));
				Object reply = route.getTriggerData().get("responseTransformer");
				if (reply != null)
					assertTrue(name + ": missing reply transformer " + reply,
							routes.getTransformers().containsKey(reply.toString()));
			}
			if (Files.exists(config.resolve("luatransformer.json"))) {
				for (var item : JsonParser.parseString(Files.readString(config.resolve("luatransformer.json"))).getAsJsonArray()) {
					JsonObject emitted = item.getAsJsonObject();
					LuaTransformerConfiguration transformer = (LuaTransformerConfiguration) routes.getTransformers()
							.get(emitted.get("uniqueId").getAsString());
					assertNotNull(name + ": dropped Lua transformer", transformer);
					assertFalse(name + ": Lua value modes are not configurable",
							emitted.has("inputType") || emitted.has("outputType") || emitted.has("queryLanguage"));
					assertEquals(name, config.resolve(emitted.get("queryPath").getAsString()).toString(),
							transformer.getQueryPath());
					assertTrue(name + ": missing Lua blob", Files.isRegularFile(Path.of(transformer.getQueryPath())));
				}
			}
			for (String filename : List.of("mqttdatasource.json", "opcuadatasource.json", "httpdatasource.json",
					"plc4xdatasource.json", "aaspollingdatasource.json", "kafkadatasource.json")) {
				if (!Files.exists(config.resolve(filename))) continue;
				for (var item : JsonParser.parseString(Files.readString(config.resolve(filename))).getAsJsonArray()) {
					JsonObject emitted = item.getAsJsonObject();
					DataSourceConfiguration source = routes.getDatasources().get(emitted.get("uniqueId").getAsString());
					assertNotNull(name + ": dropped " + filename + " source", source);
					assertEquals(name, emitted.get("sourceId").getAsString(), source.getSourceId());
					assertEquals(name, emitted.get("delivery").getAsString(), source.getDelivery());
					if (emitted.has("pollingIntervalMs"))
						assertEquals(name, emitted.get("pollingIntervalMs").getAsLong(), source.getPollingIntervalMs());
					if (emitted.has("valuePath"))
						assertEquals(name, emitted.get("valuePath").getAsString(), source.getValuePath());
					if (emitted.has("schema"))
						assertEquals(name, emitted.get("schema"), gson.toJsonTree(source.getSchema()));
					if (emitted.has("mode")) assertEquals(name, emitted.get("mode").getAsString(), source.getMode());
					if (emitted.has("elementType")) {
						String elementType = emitted.get("elementType").getAsString();
						if (source instanceof AASPollingDataSourceConfiguration aas)
							assertEquals(name, elementType, aas.getElementType());
						else assertEquals(name, elementType, source.getEventFilter().get("modelType"));
					}
					if (source instanceof KafkaDataSourceConfiguration kafka) {
						assertEquals(name, emitted.get("groupId").getAsString(), kafka.getGroupId());
						assertEquals(name, emitted.getAsJsonObject("eventFilter").get("elementPath").getAsString(),
								kafka.getEventFilter().get("elementPath"));
					}
					if (source instanceof AASPollingDataSourceConfiguration aas) {
						assertEquals(name, emitted.get("idShortPath").getAsString(), aas.getIdShortPath());
						assertEquals(name, emitted.get("submodelEndpoint").getAsString(), aas.getSubmodelEndpoint());
						assertTrue(name, aas.getConnectionURI().contains("elementType=" + aas.getElementType()));
					}
					if (source instanceof OpcuaDataSourceConfiguration opcua) {
						assertEquals(name, emitted.get("nodeInformation").getAsString(), opcua.getNodeInformation());
						assertEquals(name, emitted.get("pathToService").getAsString(), opcua.getPathToService());
						assertEquals(name, emitted.get("serverUrl").getAsString(), opcua.getServerUrl());
						assertTrue(name, opcua.getConnectionURI().contains("node=RAW(" + opcua.getNodeInformation() + ")"));
					}
					if (source instanceof HttpPollingDataSourceConfiguration http)
						assertEquals(name, emitted.get("serverUrl").getAsString(), http.getConnectionURI());
					if (source instanceof MqttDataSourceConfiguration mqtt) {
						assertEquals(name, emitted.get("topic").getAsString(), mqtt.getTopic());
						assertTrue(name, mqtt.getConnectionURI().contains("brokerUrl=tcp://" + mqtt.getServerUrl()));
					}
					if (source instanceof Plc4xDataSourceConfiguration plc) {
						assertEquals(name, emitted.get("driver").getAsString(), plc.getDriver());
						assertTrue(name, plc.getConnectionURI().contains("unit=2"));
						assertTrue(name, plc.getConnectionURI().contains("/service"));
						assertEquals(name, emitted.getAsJsonArray("tags").get(0).getAsJsonObject().get("value").getAsString(),
								plc.getTags().get(0).getValue());
					}
				}
			}
			for (String filename : List.of("aasdatasink.json", "mqttdatasink.json", "opcuadatasink.json", "httpdatasink.json", "plc4xdatasink.json")) {
				if (!Files.exists(config.resolve(filename))) continue;
				for (var item : JsonParser.parseString(Files.readString(config.resolve(filename))).getAsJsonArray()) {
					JsonObject emitted = item.getAsJsonObject();
					DataSinkConfiguration sink = routes.getDatasinks().get(emitted.get("uniqueId").getAsString());
					assertNotNull(name + ": dropped " + filename + " sink", sink);
					if (emitted.has("sinkId"))
						assertEquals(name, emitted.get("sinkId").getAsString(), sink.getSinkId());
					if (emitted.has("valuePath"))
						assertEquals(name, emitted.get("valuePath").getAsString(), sink.getValuePath());
					if (emitted.has("schema"))
						assertEquals(name, emitted.get("schema"), gson.toJsonTree(sink.getSchema()));
					if (emitted.has("payloadSchema"))
						assertEquals(name, emitted.get("payloadSchema"), gson.toJsonTree(sink.getPayloadSchema()));
					if (emitted.has("payloadGroup"))
						assertEquals(name, emitted.get("payloadGroup").getAsString(), sink.getPayloadGroup());
					if (sink instanceof Plc4xDataSinkConfiguration plc) {
						assertEquals(name, emitted.get("driver").getAsString(), plc.getDriver());
						assertEquals(name, emitted.get("servicePath").getAsString(), plc.getServicePath());
						assertTrue(name, plc.getConnectionURI().contains("unit=2"));
						assertEquals(name, emitted.getAsJsonArray("tags").get(0).getAsJsonObject().get("value").getAsString(),
								plc.getTags().get(0).getValue());
					}
					if (sink instanceof OpcuaDataSinkConfiguration opcua) {
						assertEquals(name, emitted.get("nodeInformation").getAsString(), opcua.getNodeInformation());
						assertEquals(name, emitted.get("pathToService").getAsString(), opcua.getPathToService());
					}
				}
			}
			if ("fan-in-out".equals(name)) {
				assertEquals(2, routes.getRoutes().get(0).getDatasources().size());
				assertEquals(2, routes.getRoutes().get(0).getDatasinks().size());
			}
			if ("fine-path".equals(name)) {
				assertEquals("$.mode", routes.getDatasources().values().iterator().next().getValuePath());
				assertEquals("$.mode", routes.getDatasinks().values().iterator().next().getValuePath());
			}
			if ("grouped-action".equals(name)) {
				assertEquals(2, routes.getRoutes().get(0).getDatasinks().size());
				List<String> sinkIds = routes.getRoutes().get(0).getDatasinks();
				DataSinkConfiguration first = routes.getDatasinks().get(sinkIds.get(0));
				DataSinkConfiguration second = routes.getDatasinks().get(sinkIds.get(1));
				assertEquals(first.getPayloadGroup(), second.getPayloadGroup());
				assertEquals(List.of("X", "Y"), ((Map<?, ?>) first.getPayloadSchema()).get("required"));
			}
			try (DefaultCamelContext context = new DefaultCamelContext()) {
				context.addRoutes(new DmpRouteBuilder(routes, Map.of(
						"execution-policy", new ExecutionPolicyRouteCreatorFactory(),
						"bridge", new ProtocolBridgeRouteCreatorFactory())));
				assertTrue(name + ": no bound routes", context.getRouteDefinitions().size() >= routes.getRoutes().size());
			}
		}
	}

	@Test
	public void generatedStopperingMappingLoadsAndBinds() throws Exception {
		String directory = System.getProperty("dmp.descriptor.dir");
		assumeTrue(directory != null, "Generate the AIMC/AID fixture before running this contract");
		Path config = Path.of(directory);
		ClassLoader loader = getClass().getClassLoader();
		RoutesConfiguration routes = new RoutesConfiguration();
		routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
		routes.addDatasources(new MqttDefaultConfigurationFactory(config.resolve("mqttdatasource.json").toString(), loader).create());
		routes.addTransformers(new LuaDefaultConfigurationFactory(config.resolve("luatransformer.json").toString(), loader).create());
		routes.addDatasinks(new AASDataSinkDefaultConfigurationFactory(config.resolve("aasdatasink.json").toString(), loader).create());

		assertFalse("no mapping routes compiled", routes.getRoutes().isEmpty());
		for (RouteConfiguration route : routes.getRoutes()) {
			assertEquals("execution-policy", route.getRouteTrigger());
			route.getDatasources().forEach(id -> assertNotNull("missing source " + id, routes.getDatasources().get(id)));
			route.getTransformers().forEach(id -> assertNotNull("missing transformer " + id, routes.getTransformers().get(id)));
			route.getDatasinks().forEach(id -> assertNotNull("missing sink " + id, routes.getDatasinks().get(id)));
		}

		JsonArray sources = JsonParser.parseString(Files.readString(config.resolve("mqttdatasource.json"))).getAsJsonArray();
		for (var item : sources) {
			JsonObject emitted = item.getAsJsonObject();
			MqttDataSourceConfiguration loaded = (MqttDataSourceConfiguration) routes.getDatasources()
					.get(emitted.get("uniqueId").getAsString());
			assertEquals(emitted.get("sourceId").getAsString(), loaded.getSourceId());
			assertEquals(emitted.get("topic").getAsString(), loaded.getTopic());
			assertEquals(emitted.get("serverUrl").getAsString(), loaded.getServerUrl());
			assertEquals(emitted.get("serverPort").getAsInt(), loaded.getServerPort());
			assertTrue(loaded.getConnectionURI().contains("brokerUrl=tcp://"));
		}
		JsonArray transforms = JsonParser.parseString(Files.readString(config.resolve("luatransformer.json"))).getAsJsonArray();
		for (var item : transforms) {
			JsonObject emitted = item.getAsJsonObject();
			LuaTransformerConfiguration loaded = (LuaTransformerConfiguration) routes.getTransformers()
					.get(emitted.get("uniqueId").getAsString());
			assertFalse(emitted.has("inputType") || emitted.has("outputType") || emitted.has("queryLanguage"));
			assertEquals(config.resolve(emitted.get("queryPath").getAsString()).toString(), loaded.getQueryPath());
			assertTrue(Files.isRegularFile(Path.of(loaded.getQueryPath())));
		}
		JsonArray sinks = JsonParser.parseString(Files.readString(config.resolve("aasdatasink.json"))).getAsJsonArray();
		for (var item : sinks) {
			JsonObject emitted = item.getAsJsonObject();
			AASDatasinkConfiguration loaded = (AASDatasinkConfiguration) routes.getDatasinks()
					.get(emitted.get("uniqueId").getAsString());
			assertEquals(emitted.get("idShortPath").getAsString(), loaded.getIdShortPath());
			assertEquals(emitted.get("submodelEndpoint").getAsString(), loaded.getSubmodelEndpoint());
			if (emitted.has("sinkId")) {
				assertEquals(emitted.get("sinkId").getAsString(), loaded.getSinkId());
			}
			if (emitted.has("valuePath")) {
				assertEquals(emitted.get("valuePath").getAsString(), loaded.getValuePath());
			}
		}

		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.addRoutes(new DmpRouteBuilder(routes,
					Map.of("execution-policy", new ExecutionPolicyRouteCreatorFactory())));
			assertTrue("route definitions were not created", context.getRouteDefinitions().size() >= routes.getRoutes().size());
		}
	}

	@Test
	public void generatedOpcuaMethodPreservesItsAddressAndSinkId() throws Exception {
		String directory = System.getProperty("dmp.opcua.descriptor.dir");
		assumeTrue(directory != null, "Generate the OPC UA AIMC/AID fixture before running this contract");
		Path config = Path.of(directory);
		ClassLoader loader = getClass().getClassLoader();
		RoutesConfiguration routes = new RoutesConfiguration();
		routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
		routes.addDatasinks(new OpcuaDataSinkDefaultConfigurationFactory(
				config.resolve("opcuadatasink.json").toString(), loader).create());
		assertEquals(1, routes.getRoutes().size());
		assertEquals("bridge", routes.getRoutes().get(0).getRouteTrigger());
		JsonObject emitted = JsonParser.parseString(Files.readString(config.resolve("opcuadatasink.json")))
				.getAsJsonArray().get(0).getAsJsonObject();
		OpcuaDataSinkConfiguration loaded = (OpcuaDataSinkConfiguration) routes.getDatasinks()
				.get(emitted.get("uniqueId").getAsString());
		assertNotNull(loaded);
		assertEquals(emitted.get("sinkId").getAsString(), loaded.getSinkId());
		assertEquals(emitted.get("nodeInformation").getAsString(), loaded.getNodeInformation());
		assertEquals(emitted.get("serverUrl").getAsString(), loaded.getServerUrl());
		assertEquals(emitted.get("serverPort").getAsInt(), loaded.getServerPort());
		assertEquals(emitted.getAsJsonObject("configuration").get("method").getAsString(),
				loaded.getConfiguration().get("method"));
		assertTrue(loaded.getConnectionURI().contains("node=RAW(" + loaded.getNodeInformation() + ")"));
	}

	@Test
	public void generatedHttpInteractionPreservesItsTarget() throws Exception {
		String directory = System.getProperty("dmp.http.descriptor.dir");
		assumeTrue(directory != null, "Generate the HTTP AIMC/AID fixture before running this contract");
		Path config = Path.of(directory);
		ClassLoader loader = getClass().getClassLoader();
		RoutesConfiguration routes = new RoutesConfiguration();
		routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
		routes.addDatasinks(new HttpDataSinkDefaultConfigurationFactory(
				config.resolve("httpdatasink.json").toString(), loader).create());
		assertEquals("bridge", routes.getRoutes().get(0).getRouteTrigger());
		JsonObject emitted = JsonParser.parseString(Files.readString(config.resolve("httpdatasink.json")))
				.getAsJsonArray().get(0).getAsJsonObject();
		HttpDataSinkConfiguration loaded = (HttpDataSinkConfiguration) routes.getDatasinks()
				.get(emitted.get("uniqueId").getAsString());
		assertNotNull(loaded);
		assertEquals(emitted.get("sinkId").getAsString(), loaded.getSinkId());
		assertEquals(emitted.get("scheme").getAsString(), loaded.getScheme());
		assertEquals(emitted.get("path").getAsString(), loaded.getPath());
		assertEquals(emitted.get("port").getAsInt(), loaded.getPort());
		assertEquals("http://192.168.0.60:8080/plc/api/actions/Restart?mode=quick", loaded.getConnectionURI());
	}
}
