package org.eclipse.digitaltwin.basyx.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.aas.api.ApiType;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class AASPollingConsumerTest {
	@Test
	void timedOutReadReturnsNoContributionWithinMilliseconds() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/submodels/test", request -> {
			try {
				Thread.sleep(2000);
				request.sendResponseHeaders(200, 0);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (IOException ignored) {
				// The client cancelled the read on timeout.
			} finally {
				request.close();
			}
		});
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			AASPollingConsumer consumer = consumer(context, server);
			consumer.start();
			try {
				long start = System.nanoTime();
				assertNull(consumer.receive(100));
				assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1500,
						"Camel's timeout must be interpreted as milliseconds, not seconds");
			} finally {
				consumer.stop();
			}
		} finally {
			server.stop(0);
		}
	}

	@Test
	void failedHttpReadDoesNotBecomeAnEmptyPayload() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/submodels/test", request -> {
			byte[] error = "unavailable".getBytes(StandardCharsets.UTF_8);
			request.sendResponseHeaders(503, error.length);
			try (var body = request.getResponseBody()) {
				body.write(error);
			}
		});
		server.start();
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			AASPollingConsumer consumer = consumer(context, server);
			consumer.start();
			try {
				assertNull(consumer.receive(1000));
			} finally {
				consumer.stop();
			}
		} finally {
			server.stop(0);
		}
	}

	private static AASPollingConsumer consumer(DefaultCamelContext context, HttpServer server) throws Exception {
		context.addComponent("aas", new AASComponent());
		AASComponent component = context.getComponent("aas", AASComponent.class);
		AASEndpoint endpoint = (AASEndpoint) component.createEndpoint(
				"aas:http://127.0.0.1:" + server.getAddress().getPort() + "/submodels/test");
		endpoint.setApi(ApiType.DOT_AAS_V3);
		endpoint.setPropertyPath("");
		return new AASPollingConsumer(endpoint);
	}
}
