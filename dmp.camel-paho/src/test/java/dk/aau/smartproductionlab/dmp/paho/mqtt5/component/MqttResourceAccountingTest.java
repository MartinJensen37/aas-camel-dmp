/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import org.junit.jupiter.api.Test;

class MqttResourceAccountingTest {
	private static Map<String, Long> counters(MqttV5Endpoint endpoint) {
		var counters = new java.util.HashMap<String, Long>();
		var identities = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Object, Boolean>());
		for (ManagedRouteResource.AccountingEntry entry : endpoint.accounting()) {
			assertTrue(identities.add(entry.identity()), "each actual client must occur once");
			entry.counters().forEach((key, value) -> counters.merge(key, value, Long::sum));
		}
		return counters;
	}

	@Test
	void socketsAreObservedAfterConnectAndDisappearAfterLocalClose() throws Exception {
		var factory = new MqttV5Endpoint.ObservedSocketFactory();
		try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress()); var socket = factory.createSocket()) {
			assertEquals(0, factory.openSocketCount(), "an unconnected Socket object is not an established socket");
			socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.getLocalPort()), 1_000);
			assertEquals(1, factory.openSocketCount());
			socket.close();
			assertEquals(0, factory.openSocketCount());
		}
	}

	@Test
	void lazySharedTransportsCountActualClientsAndSocketsUntilLastOwnerCloses() throws Exception {
		try (var fixture = new MqttV5TransportDeadlineTest.Fixture(1)) {
			assertTrue(counters(fixture.endpoint).isEmpty(), "creating an endpoint/producer opens no transport");
			fixture.producer.process(fixture.request("accounting-first"));
			assertEquals(Map.of("mqttClients", 2L, "mqttConnectedClients", 2L,
					"mqttObservedOpenTcpSockets", 2L, "mqttSocketUnobservableClients", 0L), counters(fixture.endpoint));
			var sibling = fixture.newProducer();
			sibling.start();
			try {
				sibling.process(fixture.request("accounting-sibling"));
				var baseline = counters(fixture.endpoint);
				fixture.producer.stop();
				assertEquals(baseline, counters(fixture.endpoint), "shared clients survive the first owner's cleanup");
				assertEquals(2, fixture.broker.publications.get(), "accounting never sends a command");
			} finally { sibling.stop(); }
			assertTrue(counters(fixture.endpoint).isEmpty());
			fixture.assertRetired();
		}
	}

	@Test
	void reconnectReplacesRatherThanAccumulatesObservedSockets() throws Exception {
		try (var fixture = new MqttV5TransportDeadlineTest.Fixture(1)) {
			fixture.producer.process(fixture.request("accounting-reconnect"));
			var baseline = counters(fixture.endpoint);
			fixture.broker.subscriber.close();
			assertTrue(fixture.broker.thirdSubscription.await(4, TimeUnit.SECONDS));
			assertEquals(baseline, counters(fixture.endpoint));
			assertEquals(1, fixture.broker.publications.get(), "reconnect never replays a command");
			fixture.producer.stop();
			assertTrue(counters(fixture.endpoint).isEmpty());
		}
	}

	@Test
	void readsDoNotWaitForTransportOrEndpointMonitorDuringBlockedSubscription() throws Exception {
		try (var fixture = new MqttV5TransportDeadlineTest.Fixture(1)) {
			fixture.broker.withholdSubscription = 1;
			var request = fixture.executor.submit(() -> assertThrows(Exception.class,
					() -> fixture.producer.process(fixture.request("accounting-blocked"))));
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			var snapshot = fixture.executor.submit(() -> counters(fixture.endpoint)).get(500, TimeUnit.MILLISECONDS);
			assertEquals(1L, snapshot.get("mqttClients"));
			assertEquals(1L, snapshot.get("mqttObservedOpenTcpSockets"));
			request.get(4, TimeUnit.SECONDS);
			fixture.producer.stop();
			assertTrue(counters(fixture.endpoint).isEmpty());
		}
	}

	@Test
	void establishedTcpSocketIsNotReportedAsAMqttConnectionBeforeConnack() throws Exception {
		try (var fixture = new MqttV5TransportDeadlineTest.Fixture(1)) {
			fixture.broker.withholdConnectAck = true;
			var request = fixture.executor.submit(() -> assertThrows(Exception.class,
					() -> fixture.producer.process(fixture.request("accounting-connack"))));
			assertTrue(fixture.broker.withheld.await(3, TimeUnit.SECONDS));
			var snapshot = fixture.executor.submit(() -> counters(fixture.endpoint)).get(500, TimeUnit.MILLISECONDS);
			assertEquals(1L, snapshot.get("mqttClients"));
			assertEquals(0L, snapshot.get("mqttConnectedClients"));
			assertEquals(1L, snapshot.get("mqttObservedOpenTcpSockets"));
			request.get(4, TimeUnit.SECONDS);
			fixture.producer.stop();
			assertTrue(counters(fixture.endpoint).isEmpty());
			assertEquals(0, fixture.broker.publications.get());
		}
	}

	@Test
	void emptyObservationDoesNotAcquireEndpointMonitor() throws Exception {
		try (var fixture = new MqttV5TransportDeadlineTest.Fixture(1)) {
			var entered = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var locked = fixture.executor.submit(() -> {
				synchronized (fixture.endpoint) { entered.countDown(); release.await(3, TimeUnit.SECONDS); }
				return null;
			});
			try {
				assertTrue(entered.await(1, TimeUnit.SECONDS));
				assertTrue(fixture.executor.submit(() -> counters(fixture.endpoint)).get(500, TimeUnit.MILLISECONDS).isEmpty());
			} finally { release.countDown(); locked.get(2, TimeUnit.SECONDS); }
		}
	}
}
