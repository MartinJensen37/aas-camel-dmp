/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditPoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

class InfluxAuditLogTest {
	private final List<String> bodies = new CopyOnWriteArrayList<>();
	private final List<String> requests = new CopyOnWriteArrayList<>();
	private final AtomicInteger calls = new AtomicInteger();
	private volatile int[] statuses = { 204 };
	private final CountDownLatch release = new CountDownLatch(1);
	private volatile boolean hang;
	private HttpServer server;

	private InfluxAuditLog start(int queueSize, int batchSize) throws Exception {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.createContext("/api/v2/write", exchange -> {
			int call = calls.getAndIncrement();
			requests.add(exchange.getRequestURI() + " " + exchange.getRequestHeaders().getFirst("Authorization"));
			bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
			if (hang) {
				try { release.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			}
			int status = statuses[Math.min(call, statuses.length - 1)];
			exchange.sendResponseHeaders(status, -1);
			exchange.close();
		});
		server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
		server.start();
		return new InfluxAuditLog(new InfluxAuditLog.Settings(URI.create("http://127.0.0.1:" + server.getAddress().getPort()),
				"secret-token", "lab org", "dmp", queueSize, batchSize, Duration.ofMillis(20)));
	}

	@AfterEach
	void stopServer() {
		release.countDown();
		if (server != null) server.stop(0);
	}

	@Test
	void writesBatchesAsLineProtocolWithTokenAndBucket() throws Exception {
		try (var log = start(100, 100)) {
			log.record(AuditPoint.of("aas_write").tag("submodel", "urn:x, y").tag("path", "Variables.State")
					.tag("outcome", "ok").field("durationMs", 1.5).value("IDLE"));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> log.status().get("written").equals(1L));
			assertTrue(requests.get(0).startsWith("/api/v2/write?org=lab+org&bucket=dmp&precision=ns Token secret-token"),
					requests.get(0));
			assertTrue(bodies.get(0).startsWith(
					"aas_write,outcome=ok,path=Variables.State,submodel=urn:x\\,\\ y durationMs=1.5,value_str=\"IDLE\" "), bodies.get(0));
			assertFalse(String.valueOf(log.status()).contains("secret-token"));
		}
	}

	@Test
	void recordNeverWaitsForInfluxAndCountsWhatItDrops() throws Exception {
		hang = true;
		try (var log = start(10, 5)) {
			long started = System.nanoTime();
			for (int i = 0; i < 10_000; i++) log.record(AuditPoint.of("aas_read").value(i));
			long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
			assertTrue(elapsedMillis < 2_000, "recording took " + elapsedMillis + " ms while InfluxDB hung");
			long dropped = (Long) log.status().get("dropped");
			assertTrue(dropped >= 10_000 - 10 - 5, "dropped " + dropped);
			release.countDown();
		}
	}

	@Test
	void retriesTransientFailuresButNotRejectedBatches() throws Exception {
		statuses = new int[] { 503, 204 };
		try (var log = start(100, 100)) {
			log.record(AuditPoint.of("aas_event").value(true));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> log.status().get("written").equals(1L));
			assertEquals(2, calls.get(), "a 503 batch is sent again");
		}
		calls.set(0);
		statuses = new int[] { 422 };
		server.stop(0);
		try (var log = start(100, 100)) {
			log.record(AuditPoint.of("aas_event").value(true));
			Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> log.status().get("rejected").equals(1L));
			Thread.sleep(200);
			assertEquals(1, calls.get(), "a rejected batch is not retried");
			assertEquals("influx-http-422", log.status().get("lastError"));
		}
	}

	@Test
	void closeFlushesQueuedPoints() throws Exception {
		var log = start(100, 1);
		for (int i = 0; i < 20; i++) log.record(AuditPoint.of("aas_read").value(i));
		log.close(Duration.ofSeconds(5));
		assertEquals(20L, log.status().get("written"));
		log.record(AuditPoint.of("aas_read").value(1));
		assertEquals(1L, log.status().get("dropped"), "a closed log accepts nothing");
	}

	@Test
	void settingsComeFromTheEnvironmentAndRequireCredentials() {
		assertTrue(InfluxAuditLog.Settings.fromEnvironment(Map.of()).isEmpty(), "no URL means no log");
		var settings = InfluxAuditLog.Settings.fromEnvironment(Map.of("DMP_AUDIT_INFLUX_URL", "http://influx:8086/",
				"DMP_AUDIT_INFLUX_TOKEN", "t", "DMP_AUDIT_INFLUX_ORG", "o", "DMP_AUDIT_INFLUX_BUCKET", "b")).orElseThrow();
		assertEquals(URI.create("http://influx:8086"), settings.url());
		assertEquals(10_000, settings.queueSize());
		assertFalse(settings.toString().contains("token=t"));
		assertThrows(IllegalArgumentException.class, () -> InfluxAuditLog.Settings.fromEnvironment(
				Map.of("DMP_AUDIT_INFLUX_URL", "http://influx:8086", "DMP_AUDIT_INFLUX_ORG", "o", "DMP_AUDIT_INFLUX_BUCKET", "b")));
	}

	@Test
	void lineProtocolEncodesTypesAndEscapes() {
		StringBuilder out = new StringBuilder();
		AuditPoint point = AuditPoint.of("aas reply").tag("aas", "a=b").field("httpStatus", 502)
				.field("errorCode", "say \"hi\"\nnext").value(Map.of("ok", true));
		LineProtocol.append(out, point);
		Instant time = point.time();
		assertEquals("aas\\ reply,aas=a\\=b errorCode=\"say \\\"hi\\\"\\nnext\",httpStatus=502i,value_json=\"{\\\"ok\\\":true}\" "
				+ (time.getEpochSecond() * 1_000_000_000L + time.getNano()) + "\n", out.toString());
		StringBuilder empty = new StringBuilder();
		LineProtocol.append(empty, AuditPoint.of("aas_read").tag("path", "x"));
		assertEquals("", empty.toString(), "a point without fields is skipped");
	}

	@Test
	void valuesUseOneFieldPerType() {
		assertEquals(Map.of("value_num", 3.0), AuditPoint.of("m").value(3L).fields());
		assertEquals(Map.of("value_num", 2.5), AuditPoint.of("m").value(new java.math.BigDecimal("2.5")).fields());
		assertEquals(Map.of("value_bool", false), AuditPoint.of("m").value(false).fields());
		assertEquals(Map.of("value_str", "x"), AuditPoint.of("m").value("x").fields());
		assertEquals(Map.of("value_json", "[1,2]"), AuditPoint.of("m").value(List.of(1L, 2L)).fields());
		assertEquals(Map.of("value_null", true), AuditPoint.of("m").value(null).fields());
	}
}
