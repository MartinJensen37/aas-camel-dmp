/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.databridge.examples.operationdelegation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.eclipse.digitaltwin.basyx.databridge.core.component.DataBridgeComponent;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.factory.RoutesConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.core.health.utility.HealthCheckUtils;
import org.eclipse.digitaltwin.basyx.databridge.lua.configuration.factory.LuaDefaultConfigurationFactory;
import org.eclipse.digitaltwin.basyx.databridge.paho.configuration.factory.MqttDataSinkDefaultConfigurationFactory;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Assume;
import org.junit.rules.TemporaryFolder;

/**
 * End-to-end declared MQTT 5 operation delegation against a broker provided
 * by DMP_MQTT5_URI. Moquette 0.16 cannot forward v5 publish properties.
 * Boots the REAL {@link DataBridgeComponent} and a v5 device-side actor, then
 * <ol>
 * <li>POSTs to the exposed operation path — the bridge transforms to the
 * native command (request Lua), publishes on the command topic, the actor
 * echoes the MQTT 5 CorrelationData on the DECLARED response
 * topic, and the caller receives 200 with the response-Lua-shaped body;</li>
 * <li>POSTs again with the actor absent — the bridge answers 504 after the
 * configured timeout.</li>
 * </ol>
 */
public class TestOperationDelegation {

	private static final int TIMEOUT_SECONDS = 2;

	private static String brokerUri;
	private static String baseTopic;
	private static int httpPort;
	private static DataBridgeComponent component;
	private static MqttAsyncClient actor;
	private static volatile boolean answerRequests = true;

	@ClassRule
	public static final TemporaryFolder TEMP = new TemporaryFolder();

	@BeforeClass
	public static void setUp() throws Exception {
		brokerUri = System.getenv("DMP_MQTT5_URI");
		Assume.assumeTrue("a MQTT 5 broker is required", brokerUri != null && !brokerUri.isBlank());
		baseTopic = "dmp-inout-example/" + UUID.randomUUID();

		// 2. config files (dynamic ports) + component boot
		httpPort = freePort();
		System.setProperty(HealthCheckUtils.HEALTH_PORT_SYSTEM_PROPERTY, String.valueOf(httpPort));
		Path configDir = TEMP.newFolder("dmp-config").toPath();
		writeConfigFiles(configDir);

		ClassLoader loader = TestOperationDelegation.class.getClassLoader();
		org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration configuration =
				new org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration();
		configuration.addRoutes(new RoutesConfigurationFactory(
				configDir.resolve("routes.json").toString(), loader).create());
		configuration.addDatasinks(new MqttDataSinkDefaultConfigurationFactory(
				configDir.resolve("mqttdatasink.json").toString(), loader).create());
		configuration.addTransformers(new LuaDefaultConfigurationFactory(
				configDir.resolve("luatransformers.json").toString(), loader).create());

		component = new DataBridgeComponent(configuration);
		component.startComponent();

		// The actor echoes the transport correlation; payload Uuid is not used
		// to match replies.
		actor = new MqttAsyncClient(brokerUri, "dmp-example-actor-" + UUID.randomUUID());
		actor.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {
			@Override public void disconnected(org.eclipse.paho.mqttv5.client.MqttDisconnectResponse response) { }
			@Override public void mqttErrorOccurred(org.eclipse.paho.mqttv5.common.MqttException error) { }
			@Override public void messageArrived(String topic, MqttMessage request) throws Exception {
				if (!answerRequests || request.getProperties() == null) return;
				String uuid = extractJsonField(new String(request.getPayload(), StandardCharsets.UTF_8), "Uuid");
				MqttProperties props = new MqttProperties();
				props.setCorrelationData(request.getProperties().getCorrelationData());
				MqttMessage reply = new MqttMessage(("{\"Uuid\":\"" + uuid + "\",\"State\":\"STOPPED\"}")
						.getBytes(StandardCharsets.UTF_8));
				reply.setProperties(props);
				actor.publish(request.getProperties().getResponseTopic(), reply);
			}
			@Override public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) { }
			@Override public void connectComplete(boolean reconnect, String uri) { }
			@Override public void authPacketArrived(int code, MqttProperties properties) { }
		});
		actor.connect().waitForCompletion(10_000);
		actor.subscribe(baseTopic + "/CMD/Halt", 2).waitForCompletion(10_000);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (actor != null && actor.isConnected()) {
			actor.disconnect().waitForCompletion(10_000);
		}
		if (component != null) {
			component.stopComponent();
		}
		System.clearProperty(HealthCheckUtils.HEALTH_PORT_SYSTEM_PROPERTY);
	}

	@Test
	public void restInvocationIsDelegatedThroughCommandAndReplyLegs() throws Exception {
		HttpResponse<String> response = post("{\"Command\":\"STOP\"}");
		assertEquals(200, response.statusCode());
		// the response-direction Lua shaped the reply; the bridge unwrapped
		// the action key for the caller
		assertTrue("body: " + response.body(), response.body().contains("STOPPED"));
	}

	@Test
	public void missingReplyTimesOutWithGatewayTimeout() throws Exception {
		answerRequests = false;
		try {
			assertEquals(504, post("{\"Command\":\"STOP\"}").statusCode());
		} finally {
			answerRequests = true;
		}
	}

	private static HttpResponse<String> post(String body) throws Exception {
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create("http://127.0.0.1:" + httpPort
						+ "/operations/syntegonStopperingSystemAAS/Halt"))
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(body))
				.build();
		return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
	}

	private static void writeConfigFiles(Path configDir) throws Exception {
		Path queries = configDir.resolve("queries");
		Files.createDirectories(queries);
		copy("queries/Halt_request.lua", queries.resolve("Halt_request.lua"));
		copy("queries/Halt_response.lua", queries.resolve("Halt_response.lua"));

		write(configDir, "routes.json", "[\n"
				+ "  {\n"
				+ "    \"routeId\": \"syntegonStopperingSystem_Halt_interaction\",\n"
				+ "    \"trigger\": \"bridge\",\n"
				+ "    \"datasources\": [],\n"
				+ "    \"transformers\": [\"syntegonStopperingSystem_Halt_request_transformer\"],\n"
				+ "    \"datasinks\": [\"syntegonStopperingSystem_Halt_command_sink\"],\n"
				+ "    \"triggerData\": {\n"
				+ "      \"restPath\": \"/operations/syntegonStopperingSystemAAS/Halt\",\n"
				+ "      \"restMethod\": \"POST\",\n"
				+ "      \"requestReply\": true,\n"
				+ "      \"responseTopic\": \"" + baseTopic + "/DATA/Halt\",\n"
				+ "      \"responseTransformer\": \"syntegonStopperingSystem_Halt_response_transformer\",\n"
				+ "      \"timeoutSeconds\": " + TIMEOUT_SECONDS + "\n"
				+ "    }\n"
				+ "  }\n"
				+ "]\n");
		write(configDir, "mqttdatasink.json", "[\n"
				+ "  {\n"
				+ "    \"uniqueId\": \"syntegonStopperingSystem_Halt_command_sink\",\n"
				+ "    \"serverUrl\": \"" + URI.create(brokerUri).getHost() + "\",\n"
				+ "    \"serverPort\": " + URI.create(brokerUri).getPort() + ",\n"
				+ "    \"topic\": \"" + baseTopic + "/CMD/Halt\",\n"
				+ "    \"clientId\": \"dmp-syntegonStopperingSystem_Halt-cmd\",\n"
				+ "    \"userName\": \"\",\n"
				+ "    \"password\": \"\"\n"
				+ "  }\n"
				+ "]\n");
		write(configDir, "luatransformers.json", "[\n"
				+ "  {\n"
				+ "    \"uniqueId\": \"syntegonStopperingSystem_Halt_request_transformer\",\n"
				+ "    \"queryLanguage\": \"LUA\",\n"
				+ "    \"queryPath\": \"queries/Halt_request.lua\",\n"
				+ "    \"inputType\": \"JsonString\",\n"
				+ "    \"outputType\": \"JsonString\",\n"
				+ "    \"sourceKeys\": \"Halt\"\n"
				+ "  },\n"
				+ "  {\n"
				+ "    \"uniqueId\": \"syntegonStopperingSystem_Halt_response_transformer\",\n"
				+ "    \"queryLanguage\": \"LUA\",\n"
				+ "    \"queryPath\": \"queries/Halt_response.lua\",\n"
				+ "    \"inputType\": \"JsonString\",\n"
				+ "    \"outputType\": \"JsonString\",\n"
				+ "    \"sourceKeys\": \"Halt\"\n"
				+ "  }\n"
				+ "]\n");
	}

	private static void copy(String resource, Path target) throws Exception {
		try (InputStream in = TestOperationDelegation.class.getClassLoader()
				.getResourceAsStream(resource)) {
			Files.copy(in, target);
		}
	}

	private static void write(Path configDir, String name, String content) throws Exception {
		Files.writeString(configDir.resolve(name), content, StandardCharsets.UTF_8);
	}

	private static String extractJsonField(String json, String field) {
		String needle = "\"" + field + "\":";
		int at = json.indexOf(needle);
		if (at < 0) {
			// tolerate whitespace after the colon
			at = json.indexOf("\"" + field + "\": ");
		}
		if (at < 0) {
			return null;
		}
		int valueStart = json.indexOf('"', at + needle.length());
		int valueEnd = json.indexOf('"', valueStart + 1);
		return valueStart < 0 || valueEnd < 0 ? null : json.substring(valueStart + 1, valueEnd);
	}

	private static int freePort() throws Exception {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}
}
