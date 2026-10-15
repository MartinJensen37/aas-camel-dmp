/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.component;

import static org.junit.jupiter.api.Assertions.*;
import static org.eclipse.digitaltwin.basyx.dmp.component.ManagedControlReadiness.Outcome.*;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.MappingReconciliationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ManagedControlReadinessTest {
	@AfterEach void reset() { ManagedRuntimeStatus.reset(); }

	@Test void cachedReadinessRequiresOwnAssignmentAndInitialScanAndExpiresWithoutIo() {
		var clock = new AtomicLong();
		var probe = new StubProbe();
		try (var observer = new ManagedControlReadiness(probe, clock::get, 5000)) {
			ManagedRuntimeStatus.beginManaged();
			ManagedRuntimeStatus.trackControlReadiness(observer);
			ManagedRuntimeStatus.controlRouteStarted();
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
			observer.poll();
			assertFalse(ManagedRuntimeStatus.snapshot().ready(), "assigned control alone cannot complete the initial scan");
			ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0, List.of(), List.of()));
			assertTrue(ManagedRuntimeStatus.snapshot().ready());
			int calls = probe.calls.get();
			assertTimeoutPreemptively(Duration.ofMillis(500), () -> {
				for (int i = 0; i < 1000; i++) { ManagedRuntimeStatus.snapshot(); ManagedRuntimeStatus.asMap(); }
			});
			assertEquals(calls, probe.calls.get(), "GET/status must not trigger probes");
			clock.set(TimeUnit.SECONDS.toNanos(5));
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
			assertEquals("control-probe-stale", ManagedRuntimeStatus.snapshot().category());
			observer.poll();
			assertTrue(ManagedRuntimeStatus.snapshot().ready());
			probe.outcome = UNAVAILABLE;
			observer.poll();
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
			assertEquals("control-broker-unavailable", ManagedRuntimeStatus.asMap().get("controlCategory"));
			probe.outcome = ASSIGNED;
			observer.poll();
			assertTrue(ManagedRuntimeStatus.snapshot().ready());
		}
	}

	@Test void assignmentOfOtherOrOldClientCannotQualifyNewInstance() {
		Node node = new Node(1, "sensitive-host", 9092);
		var topic = new TopicDescription("private-topic", false, List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
		var old = member("old-instance", "private-topic");
		var group = group(node, ConsumerGroupState.STABLE, old);
		assertEquals(PENDING, ManagedControlReadiness.assignment(topic, group, "new-instance"));
		assertEquals(ASSIGNED, ManagedControlReadiness.assignment(topic, group, "old-instance"));
		assertEquals(PENDING, ManagedControlReadiness.assignment(topic, group(node, ConsumerGroupState.PREPARING_REBALANCE, old), "old-instance"));
		assertEquals(PENDING, ManagedControlReadiness.assignment(topic, group(node, ConsumerGroupState.STABLE, member("new-instance", "other-topic")), "new-instance"));
		assertEquals(ASSIGNED, ManagedControlReadiness.assignment(topic, group(node, ConsumerGroupState.STABLE, member("new-instance-0", "private-topic")), "new-instance"));
		assertEquals(PENDING, ManagedControlReadiness.assignment(topic, group(node, ConsumerGroupState.STABLE, member("new-instanceExtra", "private-topic")), "new-instance"));
		assertEquals(TOPIC_MISSING, ManagedControlReadiness.assignment(null, group, "new-instance"));
		var assigned = new TopicDescription("first", false, List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
		var unassigned = new TopicDescription("second", false, List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node))));
		assertEquals(ASSIGNED, ManagedControlReadiness.assignments(List.of(assigned),
				group(node, ConsumerGroupState.STABLE, member("new-instance", "first")), "new-instance"));
		assertEquals(PENDING, ManagedControlReadiness.assignments(List.of(assigned, unassigned),
				group(node, ConsumerGroupState.STABLE, member("new-instance", "first")), "new-instance"),
				"assignment to only one configured topic must remain unready");
		assertEquals(TOPIC_MISSING, ManagedControlReadiness.assignments(List.of(assigned,
				new TopicDescription("second", false, List.of())),
				group(node, ConsumerGroupState.STABLE, member("new-instance", "first")), "new-instance"));
	}

	@Test void delayedSuccessfulProbeCannotRefreshOldAssignmentEvidence() {
		var clock = new AtomicLong();
		try (var observer = new ManagedControlReadiness(new ManagedControlReadiness.Probe() {
			@Override public ManagedControlReadiness.Outcome check() {
				clock.addAndGet(TimeUnit.SECONDS.toNanos(5)); return ASSIGNED;
			}
			@Override public void close() { }
		}, clock::get, 5000)) {
			observer.poll();
			assertFalse(observer.snapshot().ready());
			assertEquals("control-probe-stale", observer.snapshot().category());
		}
	}

	@Test void blockedDaemonDoesNotBlockCachedStatusAndLateCompletionAfterCloseIsRejected() throws Exception {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var worker = new AtomicReference<Thread>();
		var closes = new AtomicInteger();
		var observer = new ManagedControlReadiness(new ManagedControlReadiness.Probe() {
			@Override public ManagedControlReadiness.Outcome check() throws Exception {
				worker.set(Thread.currentThread()); entered.countDown();
				while (release.getCount() != 0) {
					try { release.await(10, TimeUnit.MILLISECONDS); }
					catch (InterruptedException ignored) { /* Simulate a probe finishing after cancellation. */ }
				}
				return ASSIGNED;
			}
			@Override public void close() { closes.incrementAndGet(); release.countDown(); }
		}, System::nanoTime, 5000);
		ManagedRuntimeStatus.beginManaged();
		ManagedRuntimeStatus.trackControlReadiness(observer);
		ManagedRuntimeStatus.controlRouteStarted();
		ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0, List.of(), List.of()));
		try {
			observer.start();
			assertTrue(entered.await(2, TimeUnit.SECONDS));
			assertTrue(worker.get().isDaemon());
			assertTimeoutPreemptively(Duration.ofMillis(500), () -> assertFalse(ManagedRuntimeStatus.snapshot().ready()));
			ManagedRuntimeStatus.stopping();
			observer.close();
			worker.get().join(1000);
			assertFalse(worker.get().isAlive());
			assertEquals("control-observer-closed", observer.snapshot().category());
			assertEquals(ManagedRuntimeStatus.State.STOPPING, ManagedRuntimeStatus.snapshot().state());
			ManagedRuntimeStatus.stopped();
			observer.poll();
			assertFalse(ManagedRuntimeStatus.snapshot().ready());
		} finally { release.countDown(); observer.close(); }
		assertEquals(1, closes.get());
	}

	@Test void errorsExportOnlyFixedSafeCategoriesAndRecover() {
		var probe = new StubProbe();
		try (var observer = new ManagedControlReadiness(probe, System::nanoTime, 5000)) {
			for (Throwable error : List.of(new org.apache.kafka.common.errors.TopicAuthorizationException(Set.of("secret-topic")),
					new org.apache.kafka.common.errors.AuthenticationException("secret URI password"),
					new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("secret-topic"),
					new java.util.concurrent.TimeoutException("secret-host"), new NoClassDefFoundError("secret-client"))) {
				probe.failure = error; observer.poll();
				assertFalse(observer.snapshot().ready());
				assertFalse(observer.snapshot().toString().contains("secret"));
			}
			assertEquals(UNAUTHORIZED, ManagedControlReadiness.classify(new java.util.concurrent.ExecutionException(
					new org.apache.kafka.common.errors.AuthenticationException("private"))));
			probe.failure = null; observer.poll();
			assertTrue(observer.snapshot().ready());
		}
	}

	@Test void readinessIsNotPerMappingParity() {
		var probe = new StubProbe();
		try (var observer = new ManagedControlReadiness(probe, System::nanoTime, 5000)) {
			ManagedRuntimeStatus.beginManaged(); ManagedRuntimeStatus.trackControlReadiness(observer);
			ManagedRuntimeStatus.controlRouteStarted(); observer.poll();
			var key = new org.eclipse.digitaltwin.basyx.dmp.management.repository.RepositoryDiscovery.MappingKey("aimc", "mapping", 0);
			ManagedRuntimeStatus.publish(new MappingReconciliationService.Report(true, false, 0, 0, 0,
					List.of(new MappingReconciliationService.MappingStatus(key, MappingReconciliationService.State.BLOCKED,
							"desired", "", 1, "unsupported-capability")), List.of("unsupported-capability")));
			assertTrue(ManagedRuntimeStatus.snapshot().ready());
			assertEquals(false, ManagedRuntimeStatus.asMap().get("converged"));
		}
	}

	private static MemberDescription member(String clientId, String topic) {
		return new MemberDescription("member-sensitive", clientId, "host-sensitive", new MemberAssignment(Set.of(new TopicPartition(topic, 0))));
	}
	private static ConsumerGroupDescription group(Node node, ConsumerGroupState state, MemberDescription member) {
		return new ConsumerGroupDescription("private-group", false, List.of(member), "range", state, node);
	}
	private static final class StubProbe implements ManagedControlReadiness.Probe {
		final AtomicInteger calls = new AtomicInteger();
		ManagedControlReadiness.Outcome outcome = ASSIGNED;
		Throwable failure;
		@Override public ManagedControlReadiness.Outcome check() throws Exception {
			calls.incrementAndGet();
			if (failure instanceof Exception exception) throw exception;
			if (failure instanceof LinkageError error) throw error;
			return outcome;
		}
		@Override public void close() { }
	}
}
