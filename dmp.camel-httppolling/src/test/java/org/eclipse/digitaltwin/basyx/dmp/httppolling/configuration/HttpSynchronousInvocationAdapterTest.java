package org.eclipse.digitaltwin.basyx.dmp.httppolling.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.junit.jupiter.api.Test;

public class HttpSynchronousInvocationAdapterTest {
	@Test
	public void usesAIDFormMethodAndReturnsNamedJsonReply() throws Exception {
		AtomicReference<String> method = new AtomicReference<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/invoke", request -> {
			method.set(request.getRequestMethod());
			byte[] body = "{\"result\":\"accepted\"}".getBytes(StandardCharsets.UTF_8);
			request.getResponseHeaders().add("Content-Type", "application/json");
			request.sendResponseHeaders(200, body.length);
			request.getResponseBody().write(body);
			request.close();
		});
		server.start();
		var camel = new DefaultCamelContext();
		camel.start();
		try {
			var adapter = new HttpSynchronousInvocationAdapter();
			String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/invoke";
			int baseline = camel.getEndpoints().size();
			var inventory = adapter.ownedEndpointUris(uri, schema("PUT"));
			assertEquals(1, inventory.size());
			assertEquals(baseline, camel.getEndpoints().size(), "preflight inventory must not allocate endpoints");
			Map<String, Object> result = adapter.invoke(camel,
					uri,
					Map.of("input", "value"), schema("PUT"));
			org.junit.jupiter.api.Assertions.assertNotNull(camel.hasEndpoint(inventory.iterator().next()), "inventory must identify the actual lazy invocation endpoint");
			assertEquals("PUT", method.get());
			assertEquals("accepted", result.get("result"));
		} finally {
			camel.stop();
			server.stop(0);
		}
	}

	@Test
	public void mapsDeviceHttpErrorsToAConnectorFailureWithoutAssigningAasStatus() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/invoke", request -> {
			byte[] body = "{\"code\":\"BUSY\",\"message\":\"try later\"}"
					.getBytes(StandardCharsets.UTF_8);
			request.sendResponseHeaders(409, body.length);
			request.getResponseBody().write(body);
			request.close();
		});
		server.start();
		var camel = new DefaultCamelContext();
		camel.start();
		try {
			var adapter = new HttpSynchronousInvocationAdapter();
			ConnectorInvocationException failure;
			try {
				adapter.invoke(camel,
						"http://127.0.0.1:" + server.getAddress().getPort() + "/invoke",
						Map.of("input", "value"), schema("POST"));
				fail("HTTP error should be propagated as a connector outcome");
				return;
			} catch (ConnectorInvocationException expected) {
				failure = expected;
			}
			assertEquals(ConnectorInvocationException.Category.REMOTE, failure.getCategory());
			assertEquals("HTTP_409", failure.getConnectorCode());
		} finally {
			camel.stop();
			server.stop(0);
		}
	}

	@Test
	public void retryableResponsesCauseExactlyOneWireCommand() throws Exception {
		AtomicInteger commands = new AtomicInteger();
		AtomicInteger status = new AtomicInteger();
		AtomicReference<String> query = new AtomicReference<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/invoke", request -> {
			commands.incrementAndGet();
			query.set(request.getRequestURI().getRawQuery());
			request.getRequestBody().readAllBytes();
			request.getResponseHeaders().add("Retry-After", "1");
			request.sendResponseHeaders(status.get(), -1);
			request.close();
		});
		server.start();
		var camel = new DefaultCamelContext();
		camel.start();
		try {
			var adapter = new HttpSynchronousInvocationAdapter();
			String uri = "http://127.0.0.1:" + server.getAddress().getPort() + "/invoke?mode=quick";
			for (int retryableStatus : new int[] {429, 503}) {
				for (String method : new String[] {"POST", "PUT"}) {
					status.set(retryableStatus);
					commands.set(0);
					ConnectorInvocationException failure = assertThrows(ConnectorInvocationException.class,
							() -> adapter.invoke(camel, uri, Map.of("input", "value"), schema(method)));
					assertEquals(ConnectorInvocationException.Category.REMOTE, failure.getCategory());
					assertEquals("HTTP_" + retryableStatus, failure.getConnectorCode());
					assertEquals(1, commands.get(), method + " must not retry HTTP " + retryableStatus);
					assertEquals("mode=quick", query.get());
				}
			}
		} finally {
			camel.stop();
			server.stop(0);
		}
	}

	private static SynchronousInvocationAdapter.InvocationSchema schema(String method) {
		return new SynchronousInvocationAdapter.InvocationSchema(Map.of(), java.util.Set.of(),
				Map.of(), java.util.Set.of(), Map.of("httpMethod", method));
	}
}
