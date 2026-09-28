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
package org.eclipse.digitaltwin.basyx.databridge.core.regression.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.bridge.ProtocolBridgeRouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.bridge.ProtocolBridgeRouteCreator;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RoutesConfiguration;
import org.junit.jupiter.api.Test;

/**
 * ProtocolBridge flow (ADR-020): REST caller → request transform → native
 * command sink → response transform. The undeclared MQTT path must not
 * silently fall back to payload-Uuid correlation.
 */
class ProtocolBridgeRouteCreatorTest extends CamelTestSupport {

	private static final String HTTP_ROUTE_ID = "packingCellAAS_Restart_interaction";
	private static final ConcurrentMap<String, String> LAST_COMMAND = new ConcurrentHashMap<>();
	/** The consolidated REST surface port (bound once per class). */
	private static int restPort;

	static final class FixedSink extends DataSinkConfiguration {
		private final String uri;

		FixedSink(String uniqueId, String uri) {
			super(uniqueId);
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	static final class FixedTransformer extends DataTransformerConfiguration {
		private final String uri;

		FixedTransformer(String uniqueId, String uri) {
			super(uniqueId);
			this.uri = uri;
		}

		@Override
		public String getConnectionURI() {
			return uri;
		}
	}

	private static ProtocolBridgeRouteConfiguration undeclaredMqttBridge(String routeId, String restPath,
			String requestLuaId, String sinkId) {
		RouteConfiguration base = new RouteConfiguration(
				"bridge", List.of(), List.of(requestLuaId), List.of(sinkId));
		ProtocolBridgeRouteConfiguration config = new ProtocolBridgeRouteConfiguration(base);
		config.setRouteId(routeId);
		// the derived caller-facing operation path (DMP v3): the creator emits
		// the REST consumer from triggerData, no REST datasource exists
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.REST_PATH, restPath);
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.REST_METHOD, "POST");
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.PROTOCOL, "mqtt");
		config.getTriggerData().put("responseTransformer", "luaResp");
		return config;
	}

	/** A synchronous (http/opcua/plc4x) bridge route: no reply datasource. */
	private static ProtocolBridgeRouteConfiguration syncBridge(String routeId, String restPath,
			String requestLuaId, String sinkId, String protocol) {
		RouteConfiguration base = new RouteConfiguration(
				"bridge", List.of(), List.of(requestLuaId), List.of(sinkId));
		ProtocolBridgeRouteConfiguration config = new ProtocolBridgeRouteConfiguration(base);
		config.setRouteId(routeId);
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.REST_PATH, restPath);
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.REST_METHOD, "POST");
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.PROTOCOL, protocol);
		config.getTriggerData().put("responseTransformer", "luaResp");
		return config;
	}

	@Test
	void declaredPositionalReplyOrderParsesFromTriggerData() {
		ProtocolBridgeRouteConfiguration config = syncBridge(
				"syntegonStopperingSystem_Dispensing_interaction", "/operations/x/Dispensing",
				"luaReq", "httpCmd", "opcua");
		// no declared order → empty positional contract
		assertTrue(config.getResponseOrder().isEmpty());
		config.getTriggerData().put(ProtocolBridgeRouteConfiguration.RESPONSE_ORDER,
				List.of("State", "Outcome"));
		assertEquals(List.of("State", "Outcome"), config.getResponseOrder());
	}

	@Override
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				// bind the consolidated REST surface to a free ephemeral port
				try (java.net.ServerSocket probe = new java.net.ServerSocket(0)) {
					restPort = probe.getLocalPort();
				}
				restConfiguration().component("jetty").host("localhost").port(restPort);

				RoutesConfiguration routes = new RoutesConfiguration(
						List.of(),
						Arrays.asList(
								new FixedTransformer("luaReq", "direct:luaReq"),
								new FixedTransformer("luaReqRestart", "direct:luaReqRestart"),
								new FixedTransformer("luaResp", "direct:luaResp")),
						Arrays.asList(
								new FixedSink("haltCmd", "direct:answeringDevice"),
								new FixedSink("httpCmd", "direct:httpSink")),
						List.of());

				new ProtocolBridgeRouteCreator(this, routes)
						.addRouteToRouteBuilder(undeclaredMqttBridge("undeclared_mqtt",
								"/operations/stoppering/Halt", "luaReq", "haltCmd"));
				// synchronous (http) command leg: the HTTP response IS the reply
				new ProtocolBridgeRouteCreator(this, routes)
						.addRouteToRouteBuilder(syncBridge(HTTP_ROUTE_ID,
								"/operations/packingCellAAS/Restart", "luaReqRestart",
								"httpCmd", "http"));

				// test doubles for the Lua blob query endpoints
				from("direct:luaReq").process(e -> e.getMessage()
						.setBody("{\"Halt\":" + e.getMessage().getBody(String.class) + "}"));
				from("direct:luaReqRestart").process(e -> e.getMessage()
						.setBody("{\"Restart\":" + e.getMessage().getBody(String.class) + "}"));
				from("direct:luaResp").process(e -> e.getMessage()
						.setBody(e.getMessage().getBody(String.class)));


				// the synchronous HTTP device: records the command + method
				// header and answers inline
				from("direct:httpSink").process(e -> {
					LAST_COMMAND.put(HTTP_ROUTE_ID,
							e.getMessage().getHeader("CamelHttpMethod", String.class) + ":"
									+ e.getMessage().getBody(String.class));
					e.getMessage().setBody("{\"Restart\":{\"state\":\"restarting\"}}");
				});
			}
		};
	}

	/**
	 * Drives the bridge through its REST surface (the rest-DSL merge folds the
	 * direct assembly route into the jetty consumer, so the operation is
	 * invoked like a caller would).
	 */
	private static int post(String path, String body, StringBuilder out) throws Exception {
		java.net.HttpURLConnection connection = (java.net.HttpURLConnection)
				java.net.URI.create("http://localhost:" + restPort + path).toURL().openConnection();
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setDoOutput(true);
		connection.setConnectTimeout(2000);
		try (var os = connection.getOutputStream()) {
			os.write(body.getBytes(java.nio.charset.StandardCharsets.UTF_8));
		}
		int status = connection.getResponseCode();
		try (var is = status >= 400 ? connection.getErrorStream() : connection.getInputStream()) {
			out.append(is == null ? "" : new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
		}
		return status;
	}

	@Test
	void undeclaredMqttInteractionIsNotExposed() throws Exception {
		StringBuilder body = new StringBuilder();
		assertEquals(404, post("/operations/stoppering/Halt", "{}", body));
	}

	@Test
	void synchronousHttpOperationAnswersInline() throws Exception {
		StringBuilder body = new StringBuilder();
		int status = post("/operations/packingCellAAS/Restart",
				"{\"mode\":\"warm\"}", body);

		assertEquals(200, status);
		String command = LAST_COMMAND.get(HTTP_ROUTE_ID);
		assertTrue(command.startsWith("POST:"),
				"the declared method rides the CamelHttpMethod header: " + command);
		assertTrue(command.contains("warm"),
				"the request transform shaped the command: " + command);
		assertEquals("{\"state\":\"restarting\"}", body.toString(),
				"the inline HTTP reply is unwrapped for the caller");
	}
}
