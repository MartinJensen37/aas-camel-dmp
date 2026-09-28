package org.eclipse.digitaltwin.basyx.databridge.executable.regression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.databridge.aas.configuration.AASDatasinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.aas.configuration.AASPollingConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.aas.configuration.factory.AASPollingConsumerDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.aas.configuration.factory.AASProducerDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.bridge.ProtocolBridgeRouteCreatorFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.policy.ExecutionPolicyRouteCreatorFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.routebuilder.DataBridgeRouteBuilder;
import org.eclipse.digitaltwin.basyx.databridge.httppolling.configuration.HttpProducerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.httppolling.configuration.HttpPollingConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.httppolling.configuration.factory.HttpProducerDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.httppolling.configuration.factory.HttpPollingDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.kafka.configuration.KafkaConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.kafka.configuration.factory.KafkaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.lua.configuration.LuaTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.lua.configuration.factory.LuaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.opcua.configuration.OpcuaProducerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.opcua.configuration.OpcuaConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.opcua.configuration.factory.OpcuaDefaultSinkConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.opcua.configuration.factory.OpcuaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.paho.configuration.MqttConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.paho.configuration.factory.MqttDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.paho.configuration.factory.MqttDataSinkDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.plc4x.configuration.Plc4XConsumerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.plc4x.configuration.Plc4xProducerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.plc4x.configuration.factory.Plc4XDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.plc4x.configuration.factory.Plc4xDefaultSinkConfigurationFactory;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/** Run the full matrix with -Ddmp.contract.dir pointing to the Python-generated fixture root. */
public class GeneratedDescriptorContractTest {
	private static final List<String> FIXTURES = List.of(
			"stoppering", "opcua-method", "http-operation", "plc4x",
			"aas-property-event", "aas-property-poll", "aas-collection-event",
			"aas-collection-poll", "opcua-read", "http-poll", "fine-path",
			"fine-protocols", "fan-in-out");

	@Test
	public void generatedProtocolMatrixLoadsAndBinds() throws Exception {
		String directory = System.getProperty("dmp.contract.dir");
		assumeTrue("Generate the full Python AIMC/AID contract matrix first", directory != null);
		ClassLoader loader = getClass().getClassLoader();
		for (String name : FIXTURES) {
			Path config = Path.of(directory, name);
			assertTrue("missing generated fixture " + name, Files.isRegularFile(config.resolve("routes.json")));
			RoutesConfiguration routes = new RoutesConfiguration();
			routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
			if (Files.exists(config.resolve("mqttconsumer.json")))
				routes.addDatasources(new MqttDefaultConfigurationFactory(config.resolve("mqttconsumer.json").toString(), loader).create());
			if (Files.exists(config.resolve("opcuaconsumer.json")))
				routes.addDatasources(new OpcuaDefaultConfigurationFactory(config.resolve("opcuaconsumer.json").toString(), loader).create());
			if (Files.exists(config.resolve("httpconsumer.json")))
				routes.addDatasources(new HttpPollingDefaultConfigurationFactory(config.resolve("httpconsumer.json").toString(), loader).create());
			if (Files.exists(config.resolve("plc4xconsumer.json")))
				routes.addDatasources(new Plc4XDefaultConfigurationFactory(config.resolve("plc4xconsumer.json").toString(), loader).create());
			if (Files.exists(config.resolve("aaspollingconsumer.json")))
				routes.addDatasources(new AASPollingConsumerDefaultConfigurationFactory(config.resolve("aaspollingconsumer.json").toString(), loader).create());
			if (Files.exists(config.resolve("kafkaconsumer.json")))
				routes.addDatasources(new KafkaDefaultConfigurationFactory(config.resolve("kafkaconsumer.json").toString(), loader).create());
			if (Files.exists(config.resolve("luatransformers.json")))
				routes.addTransformers(new LuaDefaultConfigurationFactory(config.resolve("luatransformers.json").toString(), loader).create());
			if (Files.exists(config.resolve("aasserver.json")))
				routes.addDatasinks(new AASProducerDefaultConfigurationFactory(config.resolve("aasserver.json").toString(), loader).create());
			if (Files.exists(config.resolve("mqttdatasink.json")))
				routes.addDatasinks(new MqttDataSinkDefaultConfigurationFactory(config.resolve("mqttdatasink.json").toString(), loader).create());
			if (Files.exists(config.resolve("opcuadatasink.json")))
				routes.addDatasinks(new OpcuaDefaultSinkConfigurationFactory(config.resolve("opcuadatasink.json").toString(), loader).create());
			if (Files.exists(config.resolve("httpproducer.json")))
				routes.addDatasinks(new HttpProducerDefaultConfigurationFactory(config.resolve("httpproducer.json").toString(), loader).create());
			if (Files.exists(config.resolve("plc4xdatasink.json")))
				routes.addDatasinks(new Plc4xDefaultSinkConfigurationFactory(config.resolve("plc4xdatasink.json").toString(), loader).create());

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
			if (Files.exists(config.resolve("luatransformers.json"))) {
				for (var item : JsonParser.parseString(Files.readString(config.resolve("luatransformers.json"))).getAsJsonArray()) {
					JsonObject emitted = item.getAsJsonObject();
					LuaTransformerConfiguration transformer = (LuaTransformerConfiguration) routes.getTransformers()
							.get(emitted.get("uniqueId").getAsString());
					assertNotNull(name + ": dropped Lua transformer", transformer);
					assertEquals(name, emitted.get("inputType").getAsString(), transformer.getInputType());
					assertEquals(name, emitted.get("outputType").getAsString(), transformer.getOutputType());
					assertEquals(name, config.resolve(emitted.get("queryPath").getAsString()).toString(),
							transformer.getQueryPath());
					assertTrue(name + ": missing Lua blob", Files.isRegularFile(Path.of(transformer.getQueryPath())));
				}
			}
			for (String filename : List.of("mqttconsumer.json", "opcuaconsumer.json", "httpconsumer.json",
					"plc4xconsumer.json", "aaspollingconsumer.json", "kafkaconsumer.json")) {
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
					if (emitted.has("mode")) assertEquals(name, emitted.get("mode").getAsString(), source.getMode());
					if (emitted.has("elementType")) {
						String elementType = emitted.get("elementType").getAsString();
						if (source instanceof AASPollingConsumerConfiguration aas)
							assertEquals(name, elementType, aas.getElementType());
						else assertEquals(name, elementType, source.getEventFilter().get("modelType"));
					}
					if (source instanceof KafkaConsumerConfiguration kafka) {
						assertEquals(name, emitted.get("groupId").getAsString(), kafka.getGroupId());
						assertEquals(name, emitted.getAsJsonObject("eventFilter").get("elementPath").getAsString(),
								kafka.getEventFilter().get("elementPath"));
					}
					if (source instanceof AASPollingConsumerConfiguration aas) {
						assertEquals(name, emitted.get("idShortPath").getAsString(), aas.getIdShortPath());
						assertEquals(name, emitted.get("submodelEndpoint").getAsString(), aas.getSubmodelEndpoint());
						assertTrue(name, aas.getConnectionURI().contains("elementType=" + aas.getElementType()));
					}
					if (source instanceof OpcuaConsumerConfiguration opcua) {
						assertEquals(name, emitted.get("nodeInformation").getAsString(), opcua.getNodeInformation());
						assertEquals(name, emitted.get("pathToService").getAsString(), opcua.getPathToService());
						assertEquals(name, emitted.get("serverUrl").getAsString(), opcua.getServerUrl());
						assertTrue(name, opcua.getConnectionURI().contains("node=RAW(" + opcua.getNodeInformation() + ")"));
					}
					if (source instanceof HttpPollingConsumerConfiguration http)
						assertEquals(name, emitted.get("serverUrl").getAsString(), http.getConnectionURI());
					if (source instanceof MqttConsumerConfiguration mqtt) {
						assertEquals(name, emitted.get("topic").getAsString(), mqtt.getTopic());
						assertTrue(name, mqtt.getConnectionURI().contains("brokerUrl=tcp://" + mqtt.getServerUrl()));
					}
					if (source instanceof Plc4XConsumerConfiguration plc) {
						assertEquals(name, emitted.get("driver").getAsString(), plc.getDriver());
						assertTrue(name, plc.getConnectionURI().contains("unit=2"));
						assertTrue(name, plc.getConnectionURI().contains("/service"));
						assertEquals(name, emitted.getAsJsonArray("tags").get(0).getAsJsonObject().get("value").getAsString(),
								plc.getTags().get(0).getValue());
					}
				}
			}
			for (String filename : List.of("aasserver.json", "mqttdatasink.json", "opcuadatasink.json", "httpproducer.json", "plc4xdatasink.json")) {
				if (!Files.exists(config.resolve(filename))) continue;
				for (var item : JsonParser.parseString(Files.readString(config.resolve(filename))).getAsJsonArray()) {
					JsonObject emitted = item.getAsJsonObject();
					DataSinkConfiguration sink = routes.getDatasinks().get(emitted.get("uniqueId").getAsString());
					assertNotNull(name + ": dropped " + filename + " sink", sink);
					if (emitted.has("sinkId"))
						assertEquals(name, emitted.get("sinkId").getAsString(), sink.getSinkId());
					if (emitted.has("valuePath"))
						assertEquals(name, emitted.get("valuePath").getAsString(), sink.getValuePath());
					if (sink instanceof Plc4xProducerConfiguration plc) {
						assertEquals(name, emitted.get("driver").getAsString(), plc.getDriver());
						assertEquals(name, emitted.get("servicePath").getAsString(), plc.getServicePath());
						assertTrue(name, plc.getConnectionURI().contains("unit=2"));
						assertEquals(name, emitted.getAsJsonArray("tags").get(0).getAsJsonObject().get("value").getAsString(),
								plc.getTags().get(0).getValue());
					}
					if (sink instanceof OpcuaProducerConfiguration opcua) {
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
			try (DefaultCamelContext context = new DefaultCamelContext()) {
				context.addRoutes(new DataBridgeRouteBuilder(routes, Map.of(
						"execution-policy", new ExecutionPolicyRouteCreatorFactory(),
						"bridge", new ProtocolBridgeRouteCreatorFactory())));
				assertTrue(name + ": no bound routes", context.getRouteDefinitions().size() >= routes.getRoutes().size());
			}
		}
	}

	@Test
	public void generatedStopperingMappingLoadsAndBinds() throws Exception {
		String directory = System.getProperty("dmp.descriptor.dir");
		assumeTrue("Generate the AIMC/AID fixture before running this contract", directory != null);
		Path config = Path.of(directory);
		ClassLoader loader = getClass().getClassLoader();
		RoutesConfiguration routes = new RoutesConfiguration();
		routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
		routes.addDatasources(new MqttDefaultConfigurationFactory(config.resolve("mqttconsumer.json").toString(), loader).create());
		routes.addTransformers(new LuaDefaultConfigurationFactory(config.resolve("luatransformers.json").toString(), loader).create());
		routes.addDatasinks(new AASProducerDefaultConfigurationFactory(config.resolve("aasserver.json").toString(), loader).create());

		assertFalse("no mapping routes compiled", routes.getRoutes().isEmpty());
		for (RouteConfiguration route : routes.getRoutes()) {
			assertEquals("execution-policy", route.getRouteTrigger());
			route.getDatasources().forEach(id -> assertNotNull("missing source " + id, routes.getDatasources().get(id)));
			route.getTransformers().forEach(id -> assertNotNull("missing transformer " + id, routes.getTransformers().get(id)));
			route.getDatasinks().forEach(id -> assertNotNull("missing sink " + id, routes.getDatasinks().get(id)));
		}

		JsonArray sources = JsonParser.parseString(Files.readString(config.resolve("mqttconsumer.json"))).getAsJsonArray();
		for (var item : sources) {
			JsonObject emitted = item.getAsJsonObject();
			MqttConsumerConfiguration loaded = (MqttConsumerConfiguration) routes.getDatasources()
					.get(emitted.get("uniqueId").getAsString());
			assertEquals(emitted.get("sourceId").getAsString(), loaded.getSourceId());
			assertEquals(emitted.get("topic").getAsString(), loaded.getTopic());
			assertEquals(emitted.get("serverUrl").getAsString(), loaded.getServerUrl());
			assertEquals(emitted.get("serverPort").getAsInt(), loaded.getServerPort());
			assertTrue(loaded.getConnectionURI().contains("brokerUrl=tcp://"));
		}
		JsonArray transforms = JsonParser.parseString(Files.readString(config.resolve("luatransformers.json"))).getAsJsonArray();
		for (var item : transforms) {
			JsonObject emitted = item.getAsJsonObject();
			LuaTransformerConfiguration loaded = (LuaTransformerConfiguration) routes.getTransformers()
					.get(emitted.get("uniqueId").getAsString());
			assertEquals(emitted.get("inputType").getAsString(), loaded.getInputType());
			assertEquals(emitted.get("outputType").getAsString(), loaded.getOutputType());
			assertEquals(config.resolve(emitted.get("queryPath").getAsString()).toString(), loaded.getQueryPath());
			assertTrue(Files.isRegularFile(Path.of(loaded.getQueryPath())));
		}
		JsonArray sinks = JsonParser.parseString(Files.readString(config.resolve("aasserver.json"))).getAsJsonArray();
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
			context.addRoutes(new DataBridgeRouteBuilder(routes,
					Map.of("execution-policy", new ExecutionPolicyRouteCreatorFactory())));
			assertTrue("route definitions were not created", context.getRouteDefinitions().size() >= routes.getRoutes().size());
		}
	}

	@Test
	public void generatedOpcuaMethodPreservesItsAddressAndSinkId() throws Exception {
		String directory = System.getProperty("dmp.opcua.descriptor.dir");
		assumeTrue("Generate the OPC UA AIMC/AID fixture before running this contract", directory != null);
		Path config = Path.of(directory);
		ClassLoader loader = getClass().getClassLoader();
		RoutesConfiguration routes = new RoutesConfiguration();
		routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
		routes.addDatasinks(new OpcuaDefaultSinkConfigurationFactory(
				config.resolve("opcuadatasink.json").toString(), loader).create());
		assertEquals(1, routes.getRoutes().size());
		assertEquals("bridge", routes.getRoutes().get(0).getRouteTrigger());
		JsonObject emitted = JsonParser.parseString(Files.readString(config.resolve("opcuadatasink.json")))
				.getAsJsonArray().get(0).getAsJsonObject();
		OpcuaProducerConfiguration loaded = (OpcuaProducerConfiguration) routes.getDatasinks()
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
		assumeTrue("Generate the HTTP AIMC/AID fixture before running this contract", directory != null);
		Path config = Path.of(directory);
		ClassLoader loader = getClass().getClassLoader();
		RoutesConfiguration routes = new RoutesConfiguration();
		routes.addRoutes(new RoutesConfigurationFactory(config.resolve("routes.json").toString(), loader).create());
		routes.addDatasinks(new HttpProducerDefaultConfigurationFactory(
				config.resolve("httpproducer.json").toString(), loader).create());
		assertEquals("bridge", routes.getRoutes().get(0).getRouteTrigger());
		JsonObject emitted = JsonParser.parseString(Files.readString(config.resolve("httpproducer.json")))
				.getAsJsonArray().get(0).getAsJsonObject();
		HttpProducerConfiguration loaded = (HttpProducerConfiguration) routes.getDatasinks()
				.get(emitted.get("uniqueId").getAsString());
		assertNotNull(loaded);
		assertEquals(emitted.get("sinkId").getAsString(), loaded.getSinkId());
		assertEquals(emitted.get("scheme").getAsString(), loaded.getScheme());
		assertEquals(emitted.get("path").getAsString(), loaded.getPath());
		assertEquals(emitted.get("port").getAsInt(), loaded.getPort());
		assertEquals("http://192.168.0.60:8080/plc/api/actions/Restart?mode=quick", loaded.getConnectionURI());
	}
}
