package org.eclipse.digitaltwin.basyx.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditLog;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditPoint;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

/** AAS reads and writes through the endpoint are logged, successful or not. */
class AasEndpointAuditTest {
	private static final String SUBMODEL = "urn:example:sm:Variables";

	@Test
	void readsAndWritesAreLogged() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			byte[] body = "PATCH".equals(exchange.getRequestMethod())
					? new byte[0]
					: "{\"modelType\":\"Property\",\"idShort\":\"State\",\"valueType\":\"xs:int\",\"value\":\"7\"}"
							.getBytes(StandardCharsets.UTF_8);
			exchange.getRequestBody().readAllBytes();
			int status = exchange.getRequestURI().getPath().contains("Broken") ? 500 : "PATCH".equals(exchange.getRequestMethod()) ? 204 : 200;
			exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
			if (body.length > 0) exchange.getResponseBody().write(body);
			exchange.close();
		});
		server.start();
		List<AuditPoint> points = new CopyOnWriteArrayList<>();
		AuditLog capture = points::add;
		AuditLog.install(capture);
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.addComponent("aas", new AASComponent());
			context.start();
			String base = "aas:http://127.0.0.1:" + server.getAddress().getPort() + "/submodels/"
					+ Base64.getUrlEncoder().withoutPadding().encodeToString(SUBMODEL.getBytes(StandardCharsets.UTF_8));
			AASEndpoint state = context.getEndpoint(base + "?propertyPath=State", AASEndpoint.class);
			state.setPropertyValue(5L);
			AASEndpoint broken = context.getEndpoint(base + "?propertyPath=Broken", AASEndpoint.class);
			assertThrows(java.io.IOException.class, () -> broken.setPropertyValue("x"));
			try (var poller = state.createPollingConsumer()) {
				poller.start();
				poller.receive(5_000);
			}
		} finally {
			AuditLog.uninstall(capture);
			server.stop(0);
		}
		assertEquals(List.of("aas_write", "aas_write", "aas_read"), points.stream().map(AuditPoint::measurement).toList());
		AuditPoint write = points.get(0), failed = points.get(1), read = points.get(2);
		assertEquals(SUBMODEL, write.tags().get("submodel"));
		assertEquals("State", write.tags().get("path"));
		assertEquals("ok", write.tags().get("outcome"));
		assertEquals(5.0, write.fields().get("value_num"));
		assertEquals("failed", failed.tags().get("outcome"));
		assertEquals("IOException", failed.fields().get("error"));
		assertEquals(7.0, read.fields().get("value_num"), "a read logs the Property value, typed");
	}
}
