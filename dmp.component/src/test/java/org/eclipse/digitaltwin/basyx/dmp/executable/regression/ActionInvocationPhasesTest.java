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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
import org.eclipse.digitaltwin.basyx.dmp.paho.testing.Mqtt5TestBroker;
import org.eclipse.digitaltwin.basyx.dmp.plc4x.testing.ModbusTestServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Operation-mapping phases (DMP PLAN P3) against a Modbus TCP test server and
 * an MQTT 5 test broker: command (parameter registers, then the start
 * register), status (a polled register, a subscribed MQTT property or an MQTT
 * event, judged by the status Lua) and response (Lua over reused status
 * values, cleanup write to the device, caller reply).
 */
class ActionInvocationPhasesTest {
	/** Start: holding-register:5 (address 4). Status: holding-register:2 (1). Setpoint parameter: holding-register:7 (6). */
	private static final int TRIGGER = 4, STATUS = 1, SETPOINT = 6;
	private static final String REQUEST_LUA = "function aimc_main(sources) return { Restart = sources.Restart.Speed } end";
	private static final String PARAMETER_REQUEST_LUA =
			"function aimc_main(sources) return { Setpoint = sources.Restart.Speed, Restart = 1 } end";
	/** Done once the status register changed from its baseline; 99 is a device fault. */
	private static final String STATUS_LUA = """
			function aimc_main(sources, invocation)
			  if sources.Done == invocation.baseline.Done then return "running" end
			  if sources.Done == 99 then return { state = "failed", reason = "device fault" } end
			  return "done"
			end
			""";
	/** The reply from the final status value; a failed action answers with no caller value. */
	private static final String RESPONSE_LUA = """
			function aimc_main(sources, invocation)
			  if invocation.status == "failed" then return {} end
			  return { Restart = { Result = sources.Done * 100 + invocation.inputs.Speed } }
			end
			""";
	/** As RESPONSE_LUA, and resets the start register (PLCopen-style handshake). */
	private static final String RESET_RESPONSE_LUA = """
			function aimc_main(sources, invocation)
			  return { Restart = { Result = sources.Done * 100 + invocation.inputs.Speed }, Reset = 0 }
			end
			""";
	/** Subscribed state object {done = n}. */
	private static final String OBSERVE_STATUS_LUA = """
			function aimc_main(sources, invocation)
			  if sources.Done == nil or invocation.baseline.Done == nil then return "running" end
			  if sources.Done.done == invocation.baseline.Done.done then return "running" end
			  return "done"
			end
			""";
	private static final String OBSERVE_RESPONSE_LUA = """
			function aimc_main(sources, invocation)
			  return { Restart = { Result = sources.Done.done * 100 + invocation.inputs.Speed } }
			end
			""";
	/** An event: running until a Finished event arrives after the baseline. */
	private static final String EVENT_STATUS_LUA = """
			function aimc_main(sources, invocation)
			  if sources.Finished == nil then return "running" end
			  return "done"
			end
			""";
	private static final String EVENT_RESPONSE_LUA = """
			function aimc_main(sources, invocation)
			  return { Restart = { Result = sources.Finished.code } }
			end
			""";

	/** MQTT action: the correlated reply is only an ack; the Finished event reports completion. */
	private static final String MQTT_REQUEST_LUA = "function aimc_main(sources) return { Restart = { Speed = sources.Restart.Speed } } end";
	private static final String MQTT_STATUS_LUA = """
			function aimc_main(sources, invocation)
			  if invocation.reply == nil or invocation.reply.State ~= "RUNNING" then return "failed" end
			  if sources.Finished == nil then return "running" end
			  return "done"
			end
			""";

	private enum Status { READ, OBSERVE, EVENT, NONE }

	/** What one test's AIMC mapping declares. */
	private record Mapping(Status status, String timeoutSeconds, String pollingInterval, boolean parameter, boolean reset,
			boolean mqttAction) {
		Mapping(Status status, String timeoutSeconds, String pollingInterval, boolean parameter, boolean reset) {
			this(status, timeoutSeconds, pollingInterval, parameter, reset, false);
		}
		static Mapping of(Status status, String timeoutSeconds) {
			return new Mapping(status, timeoutSeconds, status == Status.READ ? "0.05" : null, false, false);
		}
	}

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

	private final ScheduledExecutorService plc = Executors.newSingleThreadScheduledExecutor();
	private ModbusTestServer device;
	private Mqtt5TestBroker broker;
	private Runtime runtime;
	private FakeAasRepository repository;
	private HttpClient caller;
	private String operationUri;

	@BeforeEach
	void start() throws Exception {
		device = new ModbusTestServer();
		device.register(STATUS, 5);
		broker = new Mqtt5TestBroker();
		// The simulated PLC answers a start after 300 ms: status = speed + 1, or the fault code 99 for speed 13.
		// The speed is the setpoint parameter when one was written, else the start value itself.
		device.onWrite((kind, address) -> {
			if (!"register".equals(kind) || address != TRIGGER) return;
			int start = device.register(TRIGGER);
			if (start == 0) return; // a reset, or "never finishes"
			int speed = device.register(SETPOINT) != 0 ? device.register(SETPOINT) : start;
			int status = speed == 13 ? 99 : speed + 1;
			plc.schedule(() -> {
				device.register(STATUS, status);
				broker.publish("base/state", "{\"done\":" + status + "}");
				broker.publish("base/events/finished", "{\"code\":" + status + "}");
				return null;
			}, 300, TimeUnit.MILLISECONDS);
		});
		runtime = new Runtime();
		runtime.startComponent();
		int port = freePort();
		runtime.listener(port);
		operationUri = "http://127.0.0.1:" + port + "/operations/contract/Restart";
		caller = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
	}

	@AfterEach
	void stop() throws Exception {
		try { runtime.stopComponent(); } finally {
			plc.shutdownNow();
			device.close();
			broker.close();
			if (repository != null) repository.close();
		}
	}

	@Test
	void polledStatusEndsTheCallAndTheResponseReusesItsFinalValue() throws Exception {
		var owner = runtime.installBundle("phases", compile(Mapping.of(Status.READ, "3")));
		assertEquals(List.of(), device.writes(), "install must not trigger the action");
		long started = System.nanoTime();
		var reply = call("{\"Speed\":7}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":807}"), json(reply.body()));
		assertTrue((System.nanoTime() - started) / 1_000_000 >= 300, "the reply waited for the device");
		assertEquals(List.of("register:4=7"), device.writes(), "exactly one start write");

		// A done value left over from the previous run does not end the next call: the baseline is 8 now.
		reply = call("{\"Speed\":8}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":908}"), json(reply.body()));
		runtime.removeBundle(owner);
	}

	@Test
	void parametersAreWrittenBeforeTheStartAndTheStartIsResetBeforeTheReply() throws Exception {
		runtime.installBundle("phases", compile(new Mapping(Status.READ, "3", "0.05", true, true)));
		var reply = call("{\"Speed\":7}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":807}"), json(reply.body()));
		assertEquals(List.of("register:6=7", "register:4=1", "register:4=0"), device.writes(),
				"setpoint, then start, then the cleanup reset, all before the reply");
	}

	@Test
	void failedStatusRunsTheResponseAndAnswersADeviceErrorWithoutCallerOutputs() throws Exception {
		runtime.installBundle("phases", compile(Mapping.of(Status.READ, "3")));
		var reply = call("{\"Speed\":13}");
		assertEquals(502, reply.statusCode(), reply.body());
		assertEquals("DMP_DEVICE_ERROR", code(reply));
	}

	@Test
	void anActionThatNeverFinishesTimesOutAndBlocksConcurrentCalls() throws Exception {
		runtime.installBundle("phases", compile(Mapping.of(Status.READ, "1")));
		CompletableFuture<HttpResponse<String>> first = CompletableFuture.supplyAsync(() -> {
			try { return call("{\"Speed\":0}"); } catch (Exception e) { throw new IllegalStateException(e); }
		});
		awaitWrite("register:4=0");
		var second = call("{\"Speed\":7}");
		assertEquals(409, second.statusCode(), second.body());
		assertEquals("DMP_ACTION_BUSY", code(second));

		var timedOut = first.get(5, TimeUnit.SECONDS);
		assertEquals(504, timedOut.statusCode(), timedOut.body());
		assertEquals("DMP_DEVICE_TIMEOUT", code(timedOut));
		assertEquals(List.of("register:4=0"), device.writes(), "the busy call never reached the device");

		var after = call("{\"Speed\":7}");
		assertEquals(200, after.statusCode(), "the action is free again after the timeout: " + after.body());
	}

	@Test
	void unreadableStatusFailsBeforeAnyWrite() throws Exception {
		runtime.installBundle("phases", compile(new Mapping(Status.READ, "3", "0.05", true, false)));
		device.failReads(true);
		var reply = call("{\"Speed\":7}");
		assertEquals(502, reply.statusCode(), reply.body());
		assertEquals("DMP_DEVICE_TRANSPORT_ERROR", code(reply));
		assertEquals(List.of(), device.writes(), "no parameter or start write without a baseline");
	}

	@Test
	void subscribedStatusPropertyEndsTheCallOnItsNextChange() throws Exception {
		runtime.installBundle("phases", compile(Mapping.of(Status.OBSERVE, "3")));
		awaitSubscription("base/state");
		broker.publish("base/state", "{\"done\":5}"); // the value before the call is the baseline
		Thread.sleep(200);
		var reply = call("{\"Speed\":7}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":807}"), json(reply.body()));
	}

	@Test
	void statusEventEndsTheCallAndEarlierEventsDoNot() throws Exception {
		runtime.installBundle("phases", compile(Mapping.of(Status.EVENT, "3")));
		awaitSubscription("base/events/finished");
		broker.publish("base/events/finished", "{\"code\":1}"); // left over from an earlier run
		Thread.sleep(200);
		long started = System.nanoTime();
		var reply = call("{\"Speed\":7}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":8}"), json(reply.body()), "only the event after the baseline counts");
		assertTrue((System.nanoTime() - started) / 1_000_000 >= 300, "the reply waited for the event");
	}

	@Test
	void mqttActionAcknowledgedRunningFinishesOnItsEvent() throws Exception {
		// The device acks the command on the reply topic, then reports completion as an event.
		broker.onPublish(publication -> {
			if (!"base/command".equals(publication.topic())) return;
			byte[] correlation = publication.correlationData();
			int speed = json(publication.text()).getAsJsonObject().get("Speed").getAsInt();
			plc.execute(() -> {
				try { broker.reply("base/command/reply", correlation, "{\"State\":\"RUNNING\"}"); }
				catch (java.io.IOException ignored) { }
			});
			plc.schedule(() -> broker.publish("base/events/finished", "{\"code\":" + (speed + 1) + "}"), 300, TimeUnit.MILLISECONDS);
		});
		runtime.installBundle("phases", compile(new Mapping(Status.EVENT, "3", null, false, false, true)));
		awaitSubscription("base/events/finished");
		long started = System.nanoTime();
		var reply = call("{\"Speed\":7}");
		assertEquals(200, reply.statusCode(), reply.body());
		assertEquals(json("{\"Result\":8}"), json(reply.body()));
		assertTrue((System.nanoTime() - started) / 1_000_000 >= 300, "the RUNNING ack did not end the call");
		assertEquals(List.of(), device.writes(), "no Modbus traffic for an MQTT action");
	}

	@Test
	void compilerChecksTheStatusPhase() throws Exception {
		assertTrue(issues(Mapping.of(Status.READ, null)).stream().anyMatch(issue -> issue.message().contains("require a StatusTimeout")));
		assertTrue(issues(Mapping.of(Status.NONE, null)).stream().anyMatch(issue -> issue.message().contains("requires StatusSources")),
				"a PLC4X tag write needs a status phase");
		assertTrue(issues(new Mapping(Status.EVENT, "3", "0.05", false, false)).stream()
				.anyMatch(issue -> issue.message().contains("can only be subscribed")), "an event cannot be polled");
	}

	// ---- models: the plc4x fixture AID plus a start action, status/setpoint registers, an MQTT state and event ----

	private RoutesConfiguration compile(Mapping mapping) throws Exception {
		var compiled = bundles(models(mapping));
		assertTrue(compiled.issues().isEmpty(), String.valueOf(compiled.issues()));
		assertEquals(1, compiled.bundles().size());
		return compiled.bundles().get(0).configuration();
	}

	private List<ConnectorConfigurationFactory.Issue> issues(Mapping mapping) throws Exception {
		return bundles(models(mapping)).issues();
	}

	private ConnectorConfigurationFactory.BundleResult bundles(Map<String, String> models) throws Exception {
		if (repository != null) repository.close();
		repository = new FakeAasRepository();
		models.values().forEach(model -> repository.put(PublishedModels.publish(model, "contractAAS", "contract")));
		var plan = new MappingPlanCompiler(new AasReferenceResolver(repository.client())).compile(WireProjection.aimc(
				org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas.read(models.get("aimc"),
						org.eclipse.digitaltwin.aas4j.v3.model.Submodel.class)));
		assertEquals(AasRepositoryClient.Status.FOUND, plan.status(), String.valueOf(plan));
		return new ConnectorConfigurationFactory().createBundles(plan.value(),
				new ConnectorConfigurationFactory.RuntimeSettings(URI.create("http://127.0.0.1:1")));
	}

	private Map<String, String> models(Mapping declared) throws Exception {
		JsonObject aid = json(resource("plc4x/aid.json")).getAsJsonObject();
		JsonObject modbus = child(aid, "interface_modbus");
		property(child(modbus, "EndpointMetadata"), "base").addProperty("value", "tcp://127.0.0.1:" + device.port());
		JsonObject interaction = child(modbus, "InteractionMetadata");
		JsonObject properties = child(interaction, "properties");
		JsonObject coil = child(properties, "Coil1");
		properties.getAsJsonArray("value").add(register(coil, "Done", "holding-register:2", "readproperty"));
		properties.getAsJsonArray("value").add(register(coil, "Setpoint", "holding-register:7", "writeproperty"));
		properties.getAsJsonArray("value").add(register(coil, "Start", "holding-register:5", "writeproperty"));
		JsonObject restart = child(child(child(child(json(resource("http-operation/aid.json")).getAsJsonObject(), "interface_http"),
				"InteractionMetadata"), "actions"), "Restart").deepCopy();
		JsonObject forms = child(restart, "forms");
		property(forms, "href").addProperty("value", "holding-register:5");
		removeChild(forms, "htv_methodName");
		JsonObject actions = child(interaction, "actions");
		if (!actions.has("value")) actions.add("value", new JsonArray());
		actions.getAsJsonArray("value").add(restart);
		JsonObject mqtt = mqttInterface();
		if (declared.mqttAction()) {
			JsonObject move = child(child(child(child(json(resource("grouped-action/aid.json")).getAsJsonObject(), "interface_mqtt"),
					"InteractionMetadata"), "actions"), "Move").deepCopy();
			move.addProperty("idShort", "Restart");
			removeChild(move, "input");
			JsonObject moveForms = child(move, "forms");
			property(moveForms, "href").addProperty("value", "/command");
			property(moveForms, "mqv_response_topic").addProperty("value", "/command/reply");
			property(child(moveForms, "response"), "href").addProperty("value", "/command/reply");
			JsonObject mqttActions = child(child(mqtt, "InteractionMetadata"), "actions");
			if (!mqttActions.has("value")) mqttActions.add("value", new JsonArray());
			mqttActions.getAsJsonArray("value").add(move);
		}
		replaceInterface(aid, mqtt);

		JsonObject cci = json(resource("http-operation/dependencies/0.json")).getAsJsonObject();
		JsonObject operation = child(child(child(cci, "Skills"), "Restart"), "operation");
		operation.add("inputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Speed\",\"valueType\":\"xs:int\"}}]"));
		operation.add("outputVariables", json("[{\"value\":{\"modelType\":\"Property\",\"idShort\":\"Result\",\"valueType\":\"xs:int\"}}]"));

		JsonObject aimc = json(resource("http-operation/aimc.json")).getAsJsonObject();
		JsonObject mapping = child(aimc, "MappingConfigurations").getAsJsonArray("value").get(0).getAsJsonObject();
		mapping.getAsJsonArray("value").forEach(field -> {
			JsonObject object = field.getAsJsonObject();
			if ("Transformation".equals(object.get("idShort").getAsString()))
				object.addProperty("value", base64(declared.mqttAction() ? MQTT_REQUEST_LUA
						: declared.parameter() ? PARAMETER_REQUEST_LUA : REQUEST_LUA));
		});
		JsonArray sinks = child(mapping, "Sinks").getAsJsonArray("value");
		reference(sinks.get(0).getAsJsonObject(), "Sink", "a/submodels/AID",
				declared.mqttAction() ? "interface_mqtt" : "interface_modbus", "InteractionMetadata", "actions", "Restart");
		if (declared.parameter()) sinks.add(endpoint("Sink", "Setpoint", modbusProperty("Setpoint")));
		String[] status = switch (declared.status()) {
			case READ -> new String[] { "Done", "interface_modbus", "properties", "Done" };
			case OBSERVE -> new String[] { "Done", "interface_mqtt", "properties", "State" };
			case EVENT -> new String[] { "Finished", "interface_mqtt", "events", "Finished" };
			case NONE -> null;
		};
		if (status != null) {
			JsonObject source = endpoint("Source", status[0], "a/submodels/AID", status[1], "InteractionMetadata", status[2], status[3]);
			if (declared.pollingInterval() != null) source.getAsJsonArray("value").add(json("{\"modelType\":\"Property\","
					+ "\"idShort\":\"PollingInterval\",\"valueType\":\"xs:double\",\"value\":\"" + declared.pollingInterval() + "\"}"));
			mapping.getAsJsonArray("value").add(list("StatusSources", source));
			mapping.getAsJsonArray("value").add(blob("StatusTransformation", declared.mqttAction() ? MQTT_STATUS_LUA
					: switch (declared.status()) { case OBSERVE -> OBSERVE_STATUS_LUA; case EVENT -> EVENT_STATUS_LUA; default -> STATUS_LUA; }));
			// The response reuses the value that ended the wait.
			mapping.getAsJsonArray("value").add(list("ResponseSources",
					endpoint("Source", status[0], "a/submodels/AID", status[1], "InteractionMetadata", status[2], status[3])));
		}
		if (declared.reset()) mapping.getAsJsonArray("value").add(list("ResponseSinks", endpoint("Sink", "Reset", modbusProperty("Start"))));
		mapping.getAsJsonArray("value").add(blob("ResponseTransformation", switch (declared.status()) {
			case OBSERVE -> OBSERVE_RESPONSE_LUA; case EVENT -> EVENT_RESPONSE_LUA;
			default -> declared.reset() ? RESET_RESPONSE_LUA : RESPONSE_LUA; }));
		if (declared.timeoutSeconds() != null) mapping.getAsJsonArray("value").add(json("{\"modelType\":\"Property\","
				+ "\"idShort\":\"StatusTimeout\",\"valueType\":\"xs:double\",\"value\":\"" + declared.timeoutSeconds() + "\"}"));
		return Map.of("aimc", aimc.toString(), "aid", aid.toString(), "cci", cci.toString());
	}

	private static String[] modbusProperty(String name) {
		return new String[] { "a/submodels/AID", "interface_modbus", "InteractionMetadata", "properties", name };
	}


	/** A holding-register property, shaped like the fixture's Coil1. */
	private static JsonObject register(JsonObject template, String name, String href, String op) {
		JsonObject register = template.deepCopy();
		register.addProperty("idShort", name);
		property(register, "title").addProperty("value", name);
		property(child(register, "forms"), "href").addProperty("value", href);
		property(child(register, "forms"), "op").addProperty("value", op);
		return register;
	}

	/** The device's MQTT interface: a State object property and a Finished event (with its data schema). */
	private JsonObject mqttInterface() throws Exception {
		JsonObject mqtt = child(json(resource("fan-in-out/aid.json")).getAsJsonObject(), "interface_mqtt").deepCopy();
		property(child(mqtt, "EndpointMetadata"), "base").addProperty("value", "mqtt://127.0.0.1:" + broker.port() + "/base");
		JsonObject interaction = child(mqtt, "InteractionMetadata");
		JsonObject state = child(child(interaction, "properties"), "State");
		state.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"type\",\"valueType\":\"xs:string\",\"value\":\"object\"}"));
		JsonObject finished = state.deepCopy();
		removeChild(finished, "type");
		finished.addProperty("idShort", "Finished");
		finished.remove("semanticId");
		property(child(finished, "forms"), "href").addProperty("value", "/events/finished");
		property(child(finished, "forms"), "op").addProperty("value", "subscribeevent");
		removeChild(child(finished, "forms"), "mqv_retain");
		// The event's data schema, as aas-model authors it (WoT td#hasNotificationSchema).
		finished.getAsJsonArray("value").add(json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"data\","
				+ "\"semanticId\":{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\","
				+ "\"value\":\"https://www.w3.org/2019/wot/td#hasNotificationSchema\"}]},\"value\":["
				+ "{\"modelType\":\"Property\",\"idShort\":\"type\",\"valueType\":\"xs:string\",\"value\":\"object\"}]}"));
		JsonObject events = json("{\"modelType\":\"SubmodelElementCollection\",\"idShort\":\"events\",\"semanticId\":"
				+ "{\"type\":\"ExternalReference\",\"keys\":[{\"type\":\"GlobalReference\","
				+ "\"value\":\"https://www.w3.org/2019/wot/td#EventAffordance\"}]},\"value\":[]}").getAsJsonObject();
		events.getAsJsonArray("value").add(finished);
		interaction.getAsJsonArray("value").add(events);
		return mqtt;
	}

	private static void replaceInterface(JsonObject aid, JsonObject replacement) {
		JsonArray interfaces = aid.getAsJsonArray("submodelElements");
		for (int i = 0; i < interfaces.size(); i++)
			if (replacement.get("idShort").getAsString().equals(interfaces.get(i).getAsJsonObject().get("idShort").getAsString()))
				interfaces.set(i, replacement);
	}

	private static JsonObject blob(String idShort, String lua) {
		return json("{\"modelType\":\"Blob\",\"idShort\":\"" + idShort + "\",\"contentType\":\"text/plain\",\"value\":\""
				+ base64(lua) + "\"}").getAsJsonObject();
	}

	private static String code(HttpResponse<String> reply) {
		return json(reply.body()).getAsJsonObject().get("code").getAsString();
	}

	private void awaitSubscription(String topic) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!broker.subscriptions.contains(topic)) {
			assertTrue(System.nanoTime() < deadline, "no subscription to " + topic + " in " + broker.subscriptions);
			Thread.sleep(20);
		}
	}

	private static JsonObject endpoint(String role, String id, String... keys) {
		JsonObject endpoint = json("{\"modelType\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		JsonObject reference = json("{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + role + "\"}").getAsJsonObject();
		endpoint.getAsJsonArray("value").add(reference);
		reference(endpoint, role, keys);
		endpoint.getAsJsonArray("value").add(json("{\"modelType\":\"Property\",\"idShort\":\"" + role
				+ "Id\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}"));
		return endpoint;
	}

	private static void reference(JsonObject endpoint, String role, String... keys) {
		JsonArray typed = new JsonArray();
		for (int i = 0; i < keys.length; i++) {
			JsonObject key = new JsonObject();
			key.addProperty("type", i == 0 ? "Submodel" : "SubmodelElementCollection");
			key.addProperty("value", keys[i]);
			typed.add(key);
		}
		JsonObject value = new JsonObject();
		value.addProperty("type", "ModelReference");
		value.add("keys", typed);
		child(endpoint, role).add("value", value);
	}

	private static JsonObject list(String idShort, JsonObject member) {
		JsonObject list = json("{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + idShort
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		list.getAsJsonArray("value").add(member);
		return list;
	}

	private HttpResponse<String> call(String body) throws Exception {
		return caller.send(HttpRequest.newBuilder(URI.create(operationUri)).timeout(Duration.ofSeconds(10))
				.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
				HttpResponse.BodyHandlers.ofString());
	}

	private void awaitWrite(String write) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!device.writes().contains(write)) {
			assertTrue(System.nanoTime() < deadline, "trigger write " + write + " did not arrive");
			Thread.sleep(20);
		}
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}

	private static JsonObject property(JsonObject parent, String name) { return child(parent, name); }

	private static void removeChild(JsonObject parent, String name) {
		JsonArray children = parent.getAsJsonArray("value");
		for (int i = 0; i < children.size(); i++)
			if (name.equals(children.get(i).getAsJsonObject().get("idShort").getAsString())) { children.remove(i); return; }
	}

	private static String base64(String text) {
		return Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8));
	}

	private static String resource(String path) throws Exception {
		try (var input = ActionInvocationPhasesTest.class.getClassLoader().getResourceAsStream("runtime-model-contracts/" + path)) {
			assertNotNull(input, path);
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static JsonElement json(String value) { return JsonParser.parseString(value); }

	/**
	 * A free port below the ephemeral range: Jetty binds it only when the bundle installs, and in
	 * between the test's own PLC4X and MQTT client sockets take ephemeral ports.
	 */
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
