/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.component;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.server.common.MetadataVersion;

import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;

/**
 * A single-node KRaft broker (combined broker and controller) for tests, on fixed loopback ports so it can be
 * stopped and restarted like a real broker outage.
 *
 * <p>The listener under test is named {@code EXTERNAL} and speaks {@code protocol}; listener-scoped settings use
 * the prefix {@code listener.name.external.}. Controller and inter-broker traffic use separate plaintext
 * listeners ({@code CONTROLLER}, {@code INTERNAL}); their principal is {@code User:ANONYMOUS}, which an
 * authorizing test has to treat as a super user.
 */
final class KraftTestBroker implements AutoCloseable {
	final int port = freePort(), internalPort = freePort(), controllerPort = freePort();
	final Path directory;
	private final Properties config = new Properties();
	private KafkaRaftServer server;

	KraftTestBroker(String protocol, Map<String, Object> settings) throws Exception {
		directory = Files.createTempDirectory("dmp-kraft-");
		String logs = directory.resolve("logs").toString();
		config.put("process.roles", "broker,controller");
		config.put("node.id", "1");
		config.put("controller.quorum.voters", "1@127.0.0.1:" + controllerPort);
		config.put("controller.listener.names", "CONTROLLER");
		config.put("listeners", "EXTERNAL://127.0.0.1:" + port + ",INTERNAL://127.0.0.1:" + internalPort
				+ ",CONTROLLER://127.0.0.1:" + controllerPort);
		config.put("advertised.listeners", "EXTERNAL://127.0.0.1:" + port + ",INTERNAL://127.0.0.1:" + internalPort);
		config.put("listener.security.protocol.map", "EXTERNAL:" + protocol + ",INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
		config.put("inter.broker.listener.name", "INTERNAL");
		config.put("log.dirs", logs);
		config.put("offsets.topic.replication.factor", "1");
		config.put("offsets.topic.num.partitions", "1");
		config.put("transaction.state.log.replication.factor", "1");
		config.put("transaction.state.log.min.isr", "1");
		config.put("share.coordinator.state.topic.replication.factor", "1");
		config.put("share.coordinator.state.topic.min.isr", "1");
		config.put("group.initial.rebalance.delay.ms", "0");
		config.put("auto.create.topics.enable", "false");
		config.putAll(settings);
		try {
			new Formatter().setPrintStream(new PrintStream(OutputStream.nullOutputStream()))
					.setNodeId(1).setClusterId(Uuid.randomUuid().toString())
					.setDirectories(List.of(logs)).setMetadataLogDirectory(logs)
					.setControllerListenerName("CONTROLLER")
					.setReleaseVersion(MetadataVersion.LATEST_PRODUCTION)
					.run();
			start();
		} catch (Exception | Error failure) {
			close();
			throw failure;
		}
	}

	/** A plaintext broker. */
	KraftTestBroker(boolean autoCreateTopics) throws Exception {
		this("PLAINTEXT", Map.of("auto.create.topics.enable", Boolean.toString(autoCreateTopics)));
	}

	String bootstrap() { return "127.0.0.1:" + port; }
	/** The internal plaintext listener, for provisioning that must bypass the listener under test. */
	String internalBootstrap() { return "127.0.0.1:" + internalPort; }

	AdminClient admin() { return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap())); }
	AdminClient internalAdmin() { return AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, internalBootstrap())); }

	synchronized void start() {
		if (server != null) return;
		server = new KafkaRaftServer(new KafkaConfig(config, false), Time.SYSTEM);
		server.startup();
	}

	/** Stops the broker, keeping its data, as an outage would. */
	synchronized void shutdown() {
		if (server == null) return;
		server.shutdown();
		server.awaitShutdown();
		server = null;
	}

	void restart() { start(); }

	@Override public void close() throws Exception {
		try { shutdown(); }
		finally {
			if (Files.exists(directory)) try (var paths = Files.walk(directory)) {
				for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
			}
		}
	}

	static int freePort() {
		try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
		catch (java.io.IOException failure) { throw new IllegalStateException(failure); }
	}
}
