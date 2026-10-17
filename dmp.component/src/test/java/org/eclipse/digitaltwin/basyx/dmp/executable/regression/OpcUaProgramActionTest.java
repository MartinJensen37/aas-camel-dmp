/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.*;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.builder.RouteBuilder;
import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpComponent;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler;
import org.eclipse.digitaltwin.basyx.dmp.management.model.WireProjection;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasReferenceResolver;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.AasRepositoryClient;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.FakeAasRepository;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.PublishedModels;
import org.eclipse.milo.opcua.sdk.core.AccessLevel;
import org.eclipse.milo.opcua.sdk.server.AddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.EndpointConfig;
import org.eclipse.milo.opcua.sdk.server.ManagedAddressSpaceFragmentWithLifecycle;
import org.eclipse.milo.opcua.sdk.server.OpcUaServer;
import org.eclipse.milo.opcua.sdk.server.OpcUaServerConfig;
import org.eclipse.milo.opcua.sdk.server.SimpleAddressSpaceFilter;
import org.eclipse.milo.opcua.sdk.server.items.DataItem;
import org.eclipse.milo.opcua.sdk.server.items.MonitoredItem;
import org.eclipse.milo.opcua.sdk.server.methods.AbstractMethodInvocationHandler;
import org.eclipse.milo.opcua.sdk.server.nodes.UaMethodNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaObjectNode;
import org.eclipse.milo.opcua.sdk.server.nodes.UaVariableNode;
import org.eclipse.milo.opcua.sdk.server.util.SubscriptionModel;
import org.eclipse.milo.opcua.stack.core.NodeIds;
import org.eclipse.milo.opcua.stack.core.security.DefaultCertificateManager;
import org.eclipse.milo.opcua.stack.core.security.MemoryCertificateQuarantine;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.DateTime;
import org.eclipse.milo.opcua.stack.core.types.builtin.LocalizedText;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.structured.Argument;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransport;
import org.eclipse.milo.opcua.stack.transport.server.tcp.OpcTcpServerTransportConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * An OPC UA program (Part 10 style) behind an AAS Operation: the Method only
 * starts it and returns at once; the status phase observes the program's state
 * variable (a monitored item, OPC 10101 observeproperty) and the response reads
 * its result variable once (DMP PLAN P3). The AID follows OPC 10101: the action
 * carries uav_componentOf, forms declare application/octet-stream.
 */
class OpcUaProgramActionTest {
	private static final String REQUEST_LUA = "function aimc_main(sources) return { Calibrate = {} } end";
	/** Done on a Halted state newer than the baseline: a Halted left over from the last run does not count. */
	private static final String STATUS_LUA = """
			function aimc_main(sources, invocation)
			  local state, before = sources.State, invocation.baseline.State
			  if state.value == "Faulted" then return { state = "failed", reason = "program faulted" } end
			  if state.value == "Halted" and state.timestamp ~= before.timestamp then return "done" end
			  return "running"
			end
			""";
	private static final String RESPONSE_LUA = """
			function aimc_main(sources, invocation)
			  if invocation.status == "failed" then return {} end
			  return { Calibrate = { Result = sources.Result.value } }
			end
			""";

	private static final class Runtime extends DmpComponent {
		Runtime() { super(new RoutesConfiguration(), Duration.ofSeconds(4)); }
		void listener(int port) throws Exception {
			camelContext.addRoutes(new RouteBuilder() {
				@Override public void configure() {
					restConfiguration().component("jetty").host("127.0.0.1").port(port).inlineRoutes(false);
				}
			});
		}
	}

	private OpcUaServer server;
	private ProgramDevice device;
	private Runtime runtime;
	private FakeAasRepository repository;
	private int devicePort;
	private String operationUri;

	@BeforeEach
	void start() throws Exception {
		devicePort = freePort();
		var endpoint = EndpointConfig.newBuilder().setBindAddress("127.0.0.1").setBindPort(devicePort)
				.setHostname("127.0.0.1").setPath("").setSecurityPolicy(SecurityPolicy.None)
				.setSecurityMode(MessageSecurityMode.None).addTokenPolicy(OpcUaServerConfig.USER_TOKEN_POLICY_ANONYMOUS).build();
		server = new OpcUaServer(OpcUaServerConfig.builder().setEndpoints(Set.of(endpoint))
				.setApplicationName(LocalizedText.english("program simulator"))
				.setApplicationUri("urn:dmp:program:server").setProductUri("urn:dmp:program")
				.setCertificateManager(new DefaultCertificateManager(new MemoryCertificateQuarantine())).build(),
				profile -> new OpcTcpServerTransport(OpcTcpServerTransportConfig.newBuilder().build()));
		device = new ProgramDevice(server);
		device.startup();
		server.startup().get(10, TimeUnit.SECONDS);
		runtime = new Runtime();
		runtime.startComponent();
		int port = freePort();
		runtime.listener(port);
		operationUri = "http://127.0.0.1:" + port + "/operations/contract/Calibrate";
	}

	@AfterEach
	void stop() throws Exception {
		try { runtime.stopComponent(); } finally {
			try { device.stopProgram(); device.shutdown(); } finally {
				server.shutdown().get(10, TimeUnit.SECONDS);
				if (repository != null) repository.close();
			}
		}
	}

	@Test
	void startMethodReturnsAtOnceAndTheCallEndsWhenTheProgramHalts() throws Exception {
		runtime.installBundle("program", compile());
		assertEquals(0, device.starts.get(), "install must not start the program");
		HttpClient caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

		long started = System.nanoTime();
		var reply = call(caller);
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":42}"), json(reply.body()), "the result variable, read once after Halted");
		assertTrue((System.nanoTime() - started) / 1_000_000 >= 300, "the reply waited for the program, not the Method");
		assertEquals(1, device.starts.get());

		// The previous run left the program Halted; the next call still waits for its own run.
		started = System.nanoTime();
		reply = call(caller);
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":43}"), json(reply.body()));
		assertTrue((System.nanoTime() - started) / 1_000_000 >= 300, "a leftover Halted did not end the second call");
		assertEquals(2, device.starts.get(), "exactly one Start per call");
	}

	@Test
	void aFaultedProgramFailsTheCall() throws Exception {
		runtime.installBundle("program", compile());
		device.faultNext = true;
		var reply = call(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build());
		assertEquals(502, reply.statusCode(), reply.body());
		assertEquals("DMP_DEVICE_ERROR", json(reply.body()).getAsJsonObject().get("code").getAsString());
		assertEquals(1, device.starts.get());
	}

	private HttpResponse<String> call(HttpClient caller) throws Exception {
		return caller.send(HttpRequest.newBuilder(URI.create(operationUri)).timeout(Duration.ofSeconds(10))
				.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{}")).build(),
				HttpResponse.BodyHandlers.ofString());
	}

	/** Calibrate starts a 300 ms program: State Running → Halted (or Faulted), then Result. */
	private static final class ProgramDevice extends ManagedAddressSpaceFragmentWithLifecycle {
		final AtomicInteger starts = new AtomicInteger();
		volatile boolean faultNext;
		private final ScheduledExecutorService program = Executors.newSingleThreadScheduledExecutor();
		private final UaVariableNode state;
		private final UaVariableNode result;
		/** Serves monitored items: samples variables and reports their changes. */
		private final SubscriptionModel subscriptions;

		ProgramDevice(OpcUaServer server) {
			super(server);
			subscriptions = new SubscriptionModel(server, this);
			getLifecycleManager().addLifecycle(subscriptions);
			server.getNamespaceTable().add("urn:dmp:program:object");
			server.getNamespaceTable().add("urn:dmp:program:method");
			UaObjectNode object = UaObjectNode.build(getNodeContext(), builder -> builder
					.setNodeId(new NodeId(1, "CalibrationObject"))
					.setBrowseName(new QualifiedName(1, "CalibrationObject"))
					.setDisplayName(LocalizedText.english("CalibrationObject"))
					.setTypeDefinition(NodeIds.BaseObjectType).buildAndAdd());
			state = variable("CalibrationState", NodeIds.String, new Variant("Ready"));
			result = variable("CalibrationResult", NodeIds.Int32, new Variant(41));
			UaMethodNode method = UaMethodNode.builder(getNodeContext()).setNodeId(new NodeId(2, "RunCalibration"))
					.setBrowseName(new QualifiedName(2, "RunCalibration")).setDisplayName(LocalizedText.english("RunCalibration"))
					.setExecutable(true).setUserExecutable(true).buildAndAdd();
			Argument[] none = new Argument[0];
			method.setInputArguments(none);
			method.setOutputArguments(none);
			method.setInvocationHandler(new AbstractMethodInvocationHandler(method) {
				@Override public Argument[] getInputArguments() { return none; }
				@Override public Argument[] getOutputArguments() { return none; }
				@Override protected Variant[] invoke(InvocationContext context, Variant[] input) {
					starts.incrementAndGet();
					boolean fault = faultNext;
					faultNext = false;
					set(state, new Variant("Running"));
					program.schedule(() -> {
						if (fault) { set(state, new Variant("Faulted")); return; }
						set(result, new Variant(((Integer) result.getValue().getValue().getValue()) + 1));
						set(state, new Variant("Halted"));
					}, 300, TimeUnit.MILLISECONDS);
					return new Variant[0];
				}
			});
			object.addComponent(method);
		}

		private UaVariableNode variable(String name, NodeId type, Variant initial) {
			UaVariableNode node = UaVariableNode.builder(getNodeContext()).setNodeId(new NodeId(1, name))
					.setBrowseName(new QualifiedName(1, name)).setDisplayName(LocalizedText.english(name))
					.setDataType(type).setTypeDefinition(NodeIds.BaseDataVariableType)
					.setAccessLevel(AccessLevel.CurrentRead).setUserAccessLevel(AccessLevel.CurrentRead).buildAndAdd();
			set(node, initial);
			return node;
		}

		private static void set(UaVariableNode node, Variant value) {
			node.setValue(new DataValue(value, StatusCode.GOOD, DateTime.now()));
		}

		void stopProgram() { program.shutdownNow(); }
		@Override public AddressSpaceFilter getFilter() {
			return SimpleAddressSpaceFilter.create(id -> id.getNamespaceIndex().intValue() == 1 || id.getNamespaceIndex().intValue() == 2);
		}
		@Override public void onDataItemsCreated(List<DataItem> items) { subscriptions.onDataItemsCreated(items); }
		@Override public void onDataItemsModified(List<DataItem> items) { subscriptions.onDataItemsModified(items); }
		@Override public void onDataItemsDeleted(List<DataItem> items) { subscriptions.onDataItemsDeleted(items); }
		@Override public void onMonitoringModeChanged(List<MonitoredItem> items) { subscriptions.onMonitoringModeChanged(items); }
	}

	// ---- models: the opcua-method fixture with a program state and result variable ----

	private RoutesConfiguration compile() throws Exception {
		Map<String, String> models = models();
		if (repository != null) repository.close();
		repository = new FakeAasRepository();
		models.values().forEach(model -> repository.put(PublishedModels.publish(model, "contractAAS", "contract")));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(models.get("aimc"),
						org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		var compiled = new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:1")));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		return compiled.bundles().get(0).configuration();
	}

	private Map<String, String> models() throws Exception {
		JsonObject aid = json(resource("opcua-method/aid.json")).getAsJsonObject();
		JsonObject opcua = child(aid, "interface_opcua");
		child(child(opcua, "EndpointMetadata"), "base").addProperty("value", "opc.tcp://127.0.0.1:" + devicePort);
		JsonObject pressure = child(child(child(child(json(resource("opcua-read/aid.json")).getAsJsonObject(), "interface_opcua"),
				"InteractionMetadata"), "properties"), "Pressure");
		JsonObject properties = child(child(opcua, "InteractionMetadata"), "properties");
		if (!properties.has("value")) properties.add("value", new JsonArray());
		properties.getAsJsonArray("value").add(variable(pressure, "CalibrationState", "observeproperty"));
		properties.getAsJsonArray("value").add(variable(pressure, "CalibrationResult", "readproperty"));

		JsonObject cci = json(resource("opcua-method/dependencies/0.json")).getAsJsonObject();
		child(child(child(cci, "Skills"), "Calibrate"), "operation").add("outputVariables",
				json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Result\",\"valueType\":\"xs:int\"}}]"));

		JsonObject aimc = json(resource("opcua-method/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		mapping.getAsJsonArray("value").forEach(field -> {
			JsonObject object = field.getAsJsonObject();
			if ("Transformation".equals(object.get("idShort").getAsString())) object.addProperty("value", base64(REQUEST_LUA));
		});
		JsonObject state = endpoint("Source", "State", "a/submodels/AID", "interface_opcua", "InteractionMetadata",
				"properties", "CalibrationState");
		state.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"PollingInterval\","
				+ "\"valueType\":\"xs:double\",\"value\":\"0.05\"}"));
		mapping.getAsJsonArray("value").add(list("StatusSources", state));
		mapping.getAsJsonArray("value").add(blob("StatusTransformation", STATUS_LUA));
		mapping.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"StatusTimeout\","
				+ "\"valueType\":\"xs:double\",\"value\":\"3\"}"));
		mapping.getAsJsonArray("value").add(list("ResponseSources", endpoint("Source", "Result", "a/submodels/AID",
				"interface_opcua", "InteractionMetadata", "properties", "CalibrationResult")));
		mapping.getAsJsonArray("value").add(blob("ResponseTransformation", RESPONSE_LUA));
		return Map.of("aimc", aimc.toString(), "aid", aid.toString(), "cci", cci.toString());
	}

	/** An OPC UA variable affordance (shaped like the opcua-read fixture's Pressure), as OPC 10101 writes it. */
	private static JsonObject variable(JsonObject template, String name, String op) {
		JsonObject variable = template.deepCopy();
		variable.addProperty("idShort", name);
		child(variable, "title").addProperty("value", name);
		JsonObject forms = child(variable, "forms");
		child(forms, "href").addProperty("value", "/?id=ns=1;s=" + name);
		child(forms, "op").addProperty("value", op);
		child(forms, "contentType").addProperty("value", "application/octet-stream");
		return variable;
	}

	private static JsonObject endpoint(String role, String id, String... keys) {
		JsonArray typed = new JsonArray();
		for (int i = 0; i < keys.length; i++) {
			JsonObject key = new JsonObject();
			key.addProperty("type", i == 0 ? "Submodel" : "SubmodelElementCollection");
			key.addProperty("value", keys[i]);
			typed.add(key);
		}
		JsonObject reference = new JsonObject();
		reference.addProperty("type", "ModelReference");
		reference.add("keys", typed);
		JsonObject element = json("{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + role + "\"}").getAsJsonObject();
		element.add("value", reference);
		JsonObject endpoint = json("{\"modelType\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		endpoint.getAsJsonArray("value").add(element);
		endpoint.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"" + role
				+ "Id\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}"));
		return endpoint;
	}

	private static JsonObject list(String idShort, JsonObject member) {
		JsonObject list = json("{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + idShort
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		list.getAsJsonArray("value").add(member);
		return list;
	}

	private static JsonObject blob(String idShort, String lua) {
		return json("{\"modelType\":\"Blob\",\"idShort\":\"" + idShort + "\",\"contentType\":\"text/plain\",\"value\":\""
				+ base64(lua) + "\"}").getAsJsonObject();
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}

	private static String base64(String text) {
		return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
	}

	private static String resource(String path) throws Exception {
		try (var input = OpcUaProgramActionTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static JsonElement json(String value) { return JsonParser.parseString(value); }

	/** A free port below the ephemeral range (client sockets take ephemeral ports). */
	private static int freePort() throws Exception {
		for (int attempt = 0; attempt < 200; attempt++) {
			int candidate = 20000 + java.util.concurrent.ThreadLocalRandom.current().nextInt(10000);
			try (ServerSocket socket = new ServerSocket(candidate, 1, java.net.InetAddress.getLoopbackAddress())) {
				return candidate;
			} catch (java.io.IOException taken) {
				// try another
			}
		}
		throw new IllegalStateException("no free port below the ephemeral range");
	}
}
