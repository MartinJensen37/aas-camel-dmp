package org.eclipse.digitaltwin.basyx.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

/** A Property's $value goes on the wire as its lexical form in a JSON string, as BaSyx requires. */
class AasPropertyValueWireTest {
	private static final String SUBMODEL = "urn:example:sm:Variables";

	@Test
	void scalarsAreWrittenAsJsonStrings() throws Exception {
		List<String> bodies = new CopyOnWriteArrayList<>();
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/", exchange -> {
			bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			exchange.sendResponseHeaders(204, -1);
			exchange.close();
		});
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.addComponent("aas", new AASComponent());
			context.start();
			String base = "aas:http://127.0.0.1:" + server.getAddress().getPort() + "/submodels/"
					+ Base64.getUrlEncoder().withoutPadding().encodeToString(SUBMODEL.getBytes(StandardCharsets.UTF_8));
			AASEndpoint weight = context.getEndpoint(base + "?propertyPath=Weight", AASEndpoint.class);
			for (Object value : new Object[] { 2.9842, 3.0, 7L, 0.0001, true, "IDLE" }) weight.setPropertyValue(value);
			AASEndpoint whole = context.getEndpoint(base + "?propertyPath=Weight&valuePath=Weight", AASEndpoint.class);
			whole.setPropertyValue(Map.of("Weight", 1.5));
			AASEndpoint group = context.getEndpoint(base + "?propertyPath=Location&elementType=SubmodelElementCollection",
					AASEndpoint.class);
			group.setPropertyValue(Map.of("x", 1.5));
		} finally {
			server.stop(0);
		}
		assertEquals(List.of("\"2.9842\"", "\"3\"", "\"7\"", "\"0.0001\"", "\"true\"", "\"IDLE\"", "\"1.5\"",
				"{\"x\":1.5}"), bodies);
	}
}
