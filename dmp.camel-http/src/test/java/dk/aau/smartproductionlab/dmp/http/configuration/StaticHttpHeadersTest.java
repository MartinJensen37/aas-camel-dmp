/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.http.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;

/** A form's static htv_headers reach the device on writes, polls and observations, and never the URI. */
class StaticHttpHeadersTest {
	private static final Map<String, String> HEADERS = Map.of("X-Api-Key", "s3cret", "Authorization", "Bearer token-1");
	private HttpServer device;
	private CamelContext camel;
	private final BlockingQueue<Headers> received = new LinkedBlockingQueue<>();

	@BeforeEach void start() throws Exception {
		device = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		device.createContext("/", exchange -> {
			received.add(exchange.getRequestHeaders());
			exchange.getRequestBody().readAllBytes();
			byte[] body = "{\"v\":1}".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Type", "application/json");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream out = exchange.getResponseBody()) { out.write(body); }
		});
		device.start();
		camel = new DefaultCamelContext();
		camel.start();
	}

	@AfterEach void stop() throws Exception {
		camel.stop();
		device.stop(0);
	}

	private int port() { return device.getAddress().getPort(); }

	private void assertSent() throws InterruptedException {
		Headers headers = received.poll(5, TimeUnit.SECONDS);
		assertEquals("s3cret", headers.getFirst("X-api-key"));
		assertEquals("Bearer token-1", headers.getFirst("Authorization"));
	}

	@Test void aWriteSendsTheHeaders() throws Exception {
		HttpDataSinkConfiguration sink = new HttpDataSinkConfiguration("sink", "http", "127.0.0.1", "/value", port());
		sink.setMethod("PUT");
		sink.setHeaders(HEADERS);
		sink.bind(camel);
		assertFalse(sink.getConnectionURI().contains("s3cret"), sink.getConnectionURI());
		camel.createProducerTemplate().sendBody(sink.getConnectionURI(), "{\"v\":2}");
		assertSent();
		sink.close();
		assertNull(camel.getRegistry().lookupByName(sink.getConnectionURI().replaceAll(".*#bean:", "")));
	}

	@Test void aPollSendsTheHeaders() throws Exception {
		HttpPollingDataSourceConfiguration source = new HttpPollingDataSourceConfiguration("source",
				"http://127.0.0.1:" + port() + "/value", port(), null, null);
		source.setHeaders(HEADERS);
		source.bind(camel);
		assertFalse(source.getConnectionURI().contains("token-1"), source.getConnectionURI());
		camel.createConsumerTemplate().receiveBody(source.getConnectionURI(), 5000);
		assertSent();
		source.close();
	}

	@Test void anObservationSendsTheHeaders() throws Exception {
		HttpObserveDataSourceConfiguration source = new HttpObserveDataSourceConfiguration("observe",
				"http://127.0.0.1:" + port() + "/value", port(), "longpoll", "application/json", null, null);
		source.setHeaders(HEADERS);
		source.bind(camel);
		assertFalse(source.getConnectionURI().contains("s3cret"), source.getConnectionURI());
		camel.addRoutes(new org.apache.camel.builder.RouteBuilder() {
			@Override public void configure() { from(source.getConnectionURI()).to("mock:sink"); }
		});
		assertSent();
		camel.getRouteController().stopAllRoutes();
		source.close();
	}
}
