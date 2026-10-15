/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.KafkaControlEventRouteBuilder;

/** Read-only bounded background observation. Health/status reads only consult this cache. */
final class ManagedControlReadiness implements AutoCloseable {
	static final int PROBE_TIMEOUT_MILLIS = 2000;
	static final int POLL_MILLIS = 1000;
	static final int STALE_MILLIS = 5000;
	enum Outcome {
		ASSIGNED(""), PENDING("control-assignment-pending"), TOPIC_MISSING("control-topic-missing"),
		UNAVAILABLE("control-broker-unavailable"), UNAUTHORIZED("control-unauthorized"),
		CONSUMER_UNAVAILABLE("control-consumer-unavailable"),
		FAILED("control-probe-failed"), STALE("control-probe-stale"), CLOSED("control-observer-closed");
		final String category;
		Outcome(String category) { this.category = category; }
	}
	record Snapshot(boolean ready, String category) { }
	interface Probe extends AutoCloseable {
		Outcome check() throws Exception;
		@Override void close();
	}
	private record Observation(Outcome outcome, long checkedAt) { }
	private final Probe probe;
	private final LongSupplier clock;
	private final long staleNanos;
	private final ScheduledExecutorService executor;
	private final AtomicReference<Observation> cached = new AtomicReference<>();
	private final AtomicBoolean closed = new AtomicBoolean();

	ManagedControlReadiness(KafkaControlEventRouteBuilder.Settings settings, String clientId) {
		this(settings, clientId, () -> true);
	}

	ManagedControlReadiness(KafkaControlEventRouteBuilder.Settings settings, String clientId, BooleanSupplier consumerReady) {
		this(settings, clientId, consumerReady, ManagedRuntimeSettings.KafkaSecurity.plaintext());
	}

	ManagedControlReadiness(KafkaControlEventRouteBuilder.Settings settings, String clientId, BooleanSupplier consumerReady,
			ManagedRuntimeSettings.KafkaSecurity security) {
		this(new KafkaProbe(settings, clientId, consumerReady, security), System::nanoTime, STALE_MILLIS);
	}

	/** Readiness of a transport that only reports whether it is connected (MQTT). */
	ManagedControlReadiness(BooleanSupplier connected) {
		this(new Probe() {
			@Override public Outcome check() { return connected.getAsBoolean() ? Outcome.ASSIGNED : Outcome.UNAVAILABLE; }
			@Override public void close() { }
		}, System::nanoTime, STALE_MILLIS);
	}

	ManagedControlReadiness(Probe probe, LongSupplier clock, long staleMillis) {
		this.probe = java.util.Objects.requireNonNull(probe);
		this.clock = clock;
		this.staleNanos = TimeUnit.MILLISECONDS.toNanos(staleMillis);
		try {
			executor = Executors.newSingleThreadScheduledExecutor(task -> {
				Thread thread = new Thread(task, "dmp-control-readiness");
				thread.setDaemon(true);
				return thread;
			});
		} catch (RuntimeException | LinkageError failure) {
			probe.close();
			throw failure;
		}
	}

	void start() { executor.scheduleWithFixedDelay(this::poll, 0, POLL_MILLIS, TimeUnit.MILLISECONDS); }

	Snapshot snapshot() {
		Observation observation = cached.get();
		Outcome outcome = closed.get() ? Outcome.CLOSED : observation == null ? Outcome.PENDING
				: clock.getAsLong() - observation.checkedAt() >= staleNanos ? Outcome.STALE : observation.outcome();
		return new Snapshot(outcome == Outcome.ASSIGNED, outcome.category);
	}

	void poll() {
		if (closed.get()) return;
		// Age starts before I/O so a delayed completion cannot extend stale evidence.
		long checkedAt = clock.getAsLong();
		Outcome outcome;
		try { outcome = probe.check(); }
		catch (Exception | LinkageError failure) { outcome = classify(failure); }
		Observation previous = cached.get();
		if (!closed.get()) cached.compareAndSet(previous, new Observation(outcome, checkedAt));
	}

	@Override public void close() {
		if (!closed.compareAndSet(false, true)) return;
		executor.shutdownNow();
		try { probe.close(); }
		finally {
			cached.set(new Observation(Outcome.CLOSED, clock.getAsLong()));
			try { executor.awaitTermination(PROBE_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS); }
			catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
		}
	}

	static Outcome classify(Throwable failure) {
		while (failure instanceof java.util.concurrent.ExecutionException && failure.getCause() != null)
			failure = failure.getCause();
		if (failure instanceof AuthenticationException || failure instanceof AuthorizationException) return Outcome.UNAUTHORIZED;
		if (failure instanceof UnknownTopicOrPartitionException) return Outcome.TOPIC_MISSING;
		if (failure instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) return Outcome.PENDING;
		if (failure instanceof java.util.concurrent.TimeoutException || failure instanceof org.apache.kafka.common.errors.TimeoutException
				|| failure instanceof org.apache.kafka.common.errors.DisconnectException) return Outcome.UNAVAILABLE;
		return Outcome.FAILED;
	}

	static Outcome assignment(TopicDescription topic, ConsumerGroupDescription group, String ownClientId) {
		if (topic == null || topic.partitions().isEmpty()) return Outcome.TOPIC_MISSING;
		if (group == null || group.state() != ConsumerGroupState.STABLE) return Outcome.PENDING;
		return group.members().stream().filter(member -> member.clientId().equals(ownClientId)
				|| member.clientId().startsWith(ownClientId + "-"))
				.anyMatch(member -> member.assignment().topicPartitions().stream().anyMatch(partition ->
						partition.topic().equals(topic.name()) && topic.partitions().stream().anyMatch(info ->
								info.partition() == partition.partition() && info.leader() != null && info.leader().id() >= 0)))
				? Outcome.ASSIGNED : Outcome.PENDING;
	}

	static Outcome assignments(List<TopicDescription> topics, ConsumerGroupDescription group, String ownClientId) {
		for (TopicDescription topic : topics) {
			Outcome outcome = assignment(topic, group, ownClientId);
			if (outcome != Outcome.ASSIGNED) return outcome;
		}
		return topics.isEmpty() ? Outcome.TOPIC_MISSING : Outcome.ASSIGNED;
	}

	private static final class KafkaProbe implements Probe {
		private final KafkaControlEventRouteBuilder.Settings settings;
		private final String clientId;
		private final AdminClient admin;
		private final BooleanSupplier consumerReady;
		KafkaProbe(KafkaControlEventRouteBuilder.Settings settings, String clientId, BooleanSupplier consumerReady,
				ManagedRuntimeSettings.KafkaSecurity security) {
			this.settings = settings;
			this.clientId = clientId;
			this.consumerReady = consumerReady;
			Map<String, Object> properties = new java.util.HashMap<>(security.clientProperties());
			properties.putAll(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, settings.brokers(),
					AdminClientConfig.CLIENT_ID_CONFIG, "dmp-control-readiness-" + java.util.UUID.randomUUID(),
					AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, PROBE_TIMEOUT_MILLIS,
					AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, PROBE_TIMEOUT_MILLIS));
			admin = AdminClient.create(properties);
		}
		@Override public Outcome check() throws Exception {
			long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(PROBE_TIMEOUT_MILLIS);
			var topics = admin.describeTopics(Set.copyOf(settings.topics()), new DescribeTopicsOptions().timeoutMs(PROBE_TIMEOUT_MILLIS))
					.topicNameValues();
			var groups = admin.describeConsumerGroups(Set.of(settings.groupId()),
					new DescribeConsumerGroupsOptions().timeoutMs(PROBE_TIMEOUT_MILLIS)).describedGroups().get(settings.groupId());
			// Topics first: an unauthorized or missing topic explains why the group never forms, and Kafka 4
			// reports a group without members as GroupIdNotFound, which alone only means "pending".
			var descriptions = new java.util.ArrayList<TopicDescription>(settings.topics().size());
			for (String topic : settings.topics()) {
				descriptions.add(topics.get(topic).get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS));
			}
			var group = groups.get(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
			Outcome observed = assignments(descriptions, group, clientId);
			return observed == Outcome.ASSIGNED && !consumerReady.getAsBoolean() ? Outcome.CONSUMER_UNAVAILABLE : observed;
		}
		@Override public void close() { admin.close(Duration.ofSeconds(2)); }
	}
}
