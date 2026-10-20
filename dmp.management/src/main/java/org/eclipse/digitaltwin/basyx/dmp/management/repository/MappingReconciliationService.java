/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.Consumer;

import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpComponent;
import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpComponent.ManagedBundle;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory.DesiredBundle;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.ConnectorConfigurationFactory.RuntimeSettings;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.EffectiveFingerprint;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.MappingPlan;
import org.eclipse.digitaltwin.basyx.dmp.management.compiler.MappingPlanCompiler.Plan;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.RepositoryDiscovery.MappingCandidate;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.RepositoryDiscovery.MappingKey;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.RepositoryDiscovery.Scan;

/**
 * Mapping-level bundle convergence. One full scan at startup; afterwards only relevant repository events
 * trigger targeted refreshes. A pass with transient failures is retried with backoff.
 */
public final class MappingReconciliationService implements AutoCloseable {
	public enum State { INSTALLED, BLOCKED, STALE, FAILED, REMOVED }
	/** Supplies complete authoritative desired state; repository scans also revalidate installed membership. */
	@FunctionalInterface public interface ScanProvider { Scan scan(); }
	@FunctionalInterface public interface DefinitionFactory { Supplier<RoutesConfiguration> freshDefinition(MappingCandidate candidate); }
	/**
	 * One mapping's reconciliation state. {@code generation} is the installed bundle's fence, drawn from a sequence
	 * shared by all bundles; {@code revision} counts this mapping's installations since the DMP started (1 for the
	 * first, +1 per replacement) and is for display only.
	 */
	public record MappingStatus(MappingKey key, State state, String desiredFingerprint,
			String installedFingerprint, long generation, String category, long revision) { }
	public record Report(boolean complete, boolean superseded, int installedOrUpdated,
			int unchanged, int removed, List<MappingStatus> mappings, List<String> categories) {
		public Report { mappings = List.copyOf(mappings); categories = List.copyOf(categories); }
	}

	private record Active(MappingKey key, String bundleId, ManagedBundle handle,
			EffectiveFingerprint fingerprint, Set<String> dependencies) { }
	private record MutableStatus(State state, String desired, String installed, long generation, String category) { }

	private final DmpComponent component;
	private final ScanProvider scanProvider;
	private final java.util.function.Function<Set<String>, Scan> trackedScanProvider;
	private final java.util.function.Predicate<AasControlEvent> relevance;
	private RepositoryDiscovery discovery;
	private final DefinitionFactory definitions;
	private final int maxMappings;
	private final int maxQueuedInvalidations;
	private final long retryMillis;
	private final Map<MappingKey, Active> active = new LinkedHashMap<>();
	private final Map<MappingKey, MutableStatus> statuses = new LinkedHashMap<>();
	/** Installations per mapping since start; dropped with the mapping's status. Not a fence. */
	private final Map<MappingKey, Long> revisions = new LinkedHashMap<>();
	private volatile Map<String, Set<String>> activeBundleDependencies = Map.of();
	private final Map<String, Long> dependencyEpochs = new LinkedHashMap<>();
	private long fullRescanEpoch;
	private final AtomicLong invalidationEpoch = new AtomicLong();
	private final AtomicLong invalidationOverflows = new AtomicLong();
	private final AtomicLong ignoredEvents = new AtomicLong();
	private final AtomicBoolean scanQueued = new AtomicBoolean();
	private final AtomicInteger consecutiveFullScanFailures = new AtomicInteger();
	private final Object lock = new Object();
	private final Object scheduleLock = new Object();
	private volatile ScheduledExecutorService executor;
	private volatile ScheduledFuture<?> scheduledRetry;
	private volatile Consumer<Report> reportListener = ignored -> { };
	private volatile boolean closed;
	private volatile long appliedEpoch;
	private volatile long retryNotBeforeNanos;
	private volatile int overflowMappings;
	private volatile boolean scanned;

	public MappingReconciliationService(DmpComponent component, RepositoryDiscovery discovery, RuntimeSettings runtime,
			int maxMappings, int maxQueuedInvalidations, long retryMillis) {
		this(component, discovery::scan, defaultDefinitions(runtime), maxMappings, maxQueuedInvalidations, retryMillis,
				changed -> changed == null ? discovery.scan() : discovery.refresh(changed), discovery::relevant);
		this.discovery = discovery;
	}

	/** Every scan is a full scan and every event is relevant; for tests and simple providers. */
	public MappingReconciliationService(DmpComponent component, ScanProvider scanProvider,
			DefinitionFactory definitions, int maxMappings, int maxQueuedInvalidations, long retryMillis) {
		this(component, scanProvider, definitions, maxMappings, maxQueuedInvalidations, retryMillis, null, event -> true);
	}

	private MappingReconciliationService(DmpComponent component, ScanProvider scanProvider,
			DefinitionFactory definitions, int maxMappings, int maxQueuedInvalidations, long retryMillis,
			java.util.function.Function<Set<String>, Scan> trackedScanProvider,
			java.util.function.Predicate<AasControlEvent> relevance) {
		this.component = Objects.requireNonNull(component);
		this.scanProvider = Objects.requireNonNull(scanProvider);
		this.trackedScanProvider = trackedScanProvider;
		this.relevance = Objects.requireNonNull(relevance);
		this.definitions = Objects.requireNonNull(definitions);
		if (maxMappings < 1 || maxMappings > 8192) throw new IllegalArgumentException("maxMappings must be 1..8192");
		if (maxQueuedInvalidations < 1 || maxQueuedInvalidations > 65_536)
			throw new IllegalArgumentException("maxQueuedInvalidations must be 1..65536");
		if (retryMillis < 100 || retryMillis > TimeUnit.DAYS.toMillis(1))
			throw new IllegalArgumentException("retryMillis must be 100..86400000");
		this.maxMappings = maxMappings;
		this.maxQueuedInvalidations = maxQueuedInvalidations;
		this.retryMillis = retryMillis;
	}

	private static DefinitionFactory defaultDefinitions(RuntimeSettings runtime) {
		ConnectorConfigurationFactory factory = new ConnectorConfigurationFactory();
		return candidate -> {
			MappingPlan mapping = candidate.desired().desired().plan();
			Plan plan = new Plan(candidate.key().aimcId(), mapping == null ? List.of() : List.of(mapping));
			return () -> {
				var result = factory.createBundles(plan, runtime);
				if (!result.issues().isEmpty() || result.bundles().size() != 1)
					throw new IllegalStateException("validated mapping definition could not be reconstructed");
				return result.bundles().get(0).configuration();
			};
		};
	}

	/** Starts processing invalidations; a scan runs now only when one is pending or the last pass must be retried. */
	public synchronized void start() {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		if (executor != null) return;
		start(Executors.newSingleThreadScheduledExecutor(r -> {
			Thread thread = new Thread(r, "dmp-reconciliation");
			thread.setDaemon(true);
			return thread;
		}));
	}

	synchronized void start(ScheduledExecutorService worker) {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		if (executor != null) return;
		executor = Objects.requireNonNull(worker);
		if (!scanned || invalidationEpoch.get() != appliedEpoch) queueScan();
		else if (consecutiveFullScanFailures.get() > 0) scheduleRetry(nextRetryDelay());
	}

	public void setReportListener(Consumer<Report> listener) {
		reportListener = listener == null ? ignored -> { } : listener;
	}

	/** Invalidates what a relevant repository event concerns; irrelevant events are dropped. */
	public void acceptEvent(AasControlEvent event) {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		if (!relevance.test(event)) { ignoredEvents.incrementAndGet(); return; }
		invalidate(event.fullRescan() ? null : event.resourceId());
	}

	/** Accepts an event invalidation into bounded tracking; overflow schedules a full repair. */
	public void invalidate(String dependencyId) {
		if (closed) throw new IllegalStateException("reconciliation service is closed");
		synchronized (dependencyEpochs) {
			long epoch = invalidationEpoch.incrementAndGet();
			boolean unidentified = dependencyId == null || dependencyId.isBlank();
			boolean overflow = !unidentified && !dependencyEpochs.containsKey(dependencyId)
					&& dependencyEpochs.size() >= maxQueuedInvalidations;
			boolean full = unidentified || overflow;
			if (overflow) invalidationOverflows.incrementAndGet();
			if (full) {
				fullRescanEpoch = epoch;
				dependencyEpochs.clear();
			} else dependencyEpochs.put(dependencyId, epoch);
			Set<String> affected = full ? activeBundleDependencies.values().stream().flatMap(Set::stream)
					.collect(java.util.stream.Collectors.toSet()) : activeBundleDependencies.getOrDefault(dependencyId, Set.of());
			for (String bundleId : affected) component.fenceBundleGeneration(bundleId);
		}
		queueScan();
	}

	/** A full scan now, on the caller's thread. */
	public Report reconcileNow() {
		return reconcile(true);
	}

	private Report reconcile(boolean fullRepair) {
		synchronized (lock) {
			if (closed) return report(false, false, 0, 0, 0, List.of("service-closed"));
			long epoch;
			Set<String> changedIds;
			synchronized (dependencyEpochs) {
				epoch = invalidationEpoch.get();
				boolean full = fullRepair || fullRescanEpoch > appliedEpoch || !scanned;
				changedIds = full ? null : Set.copyOf(dependencyEpochs.keySet());
			}
			Scan scan;
			try {
				scan = trackedScanProvider == null ? scanProvider.scan() : trackedScanProvider.apply(changedIds);
			}
			catch (RuntimeException failure) { return failWholeScan("repository-scan-failed"); }
			if (!scan.complete()) return failWholeScan(scan.failureCategory());
			scanned = true;
			int changed = 0, unchanged = 0, removed = 0;
			boolean superseded = false;
			List<String> categories = new ArrayList<>();
			scan.failures().forEach(failure -> categories.add(failure.category()));
			Set<MappingKey> restConflicts = restConflicts(scan.mappings().values());
			for (var entry : new ArrayList<>(active.entrySet())) {
				MappingKey key = entry.getKey();
				if (superseded(epoch, key, entry.getValue().dependencies())) { superseded = true; continue; }
				if (scan.failedAimcIds().contains(key.aimcId())) {
					String category = scan.failures().stream().filter(failure -> failure.aimcId().equals(key.aimcId()))
							.map(RepositoryDiscovery.AimcFailure::category).findFirst().orElse("aimc-fetch-failed");
					mark(key, State.STALE, null, entry.getValue().fingerprint().exported(),
							entry.getValue().handle().generation(), category);
					continue;
				}
				if (!scan.mappings().containsKey(key)) {
					if (retire(entry.getValue())) { active.remove(key); statuses.remove(key); revisions.remove(key); refreshReverseDependencies(); removed++; }
					else {
						mark(key, State.STALE, null, entry.getValue().fingerprint().exported(),
								entry.getValue().handle().generation(), "bundle-removal-failed");
						categories.add("bundle-removal-failed");
					}
				}
			}
			for (MappingKey key : new ArrayList<>(statuses.keySet())) {
				if (!active.containsKey(key) && !superseded(epoch, key, Set.of())
						&& !scan.failedAimcIds().contains(key.aimcId()) && !scan.mappings().containsKey(key)) {
					statuses.remove(key);
					revisions.remove(key);
				}
			}
			overflowMappings = 0;
			for (MappingCandidate candidate : scan.mappings().values()) {
				Set<String> dependencies = new LinkedHashSet<>(dependencyIds(candidate));
				Active prior = active.get(candidate.key());
				if (prior != null) dependencies.addAll(prior.dependencies());
				if (superseded(epoch, candidate.key(), dependencies)) { superseded = true; continue; }
				if (statuses.size() >= maxMappings && !statuses.containsKey(candidate.key())) {
					if (overflowMappings++ == 0) categories.add("mapping-capacity-exceeded");
					continue;
				}
				Active previous = active.get(candidate.key());
				if (restConflicts.contains(candidate.key())) {
					mark(candidate.key(), previous == null ? State.BLOCKED : State.STALE,
							candidate.desired().fingerprint().exported(),
							previous == null ? null : previous.fingerprint().exported(),
							previous == null ? 0 : previous.handle().generation(), "rest-path-conflict");
					categories.add("rest-path-conflict");
					continue;
				}
				if (!candidate.desired().valid()) {
					if (previous != null && confirmedMissingDependency(candidate.desired())) {
						if (retire(previous)) { active.remove(candidate.key()); refreshReverseDependencies(); removed++; previous = null; }
						else {
						mark(candidate.key(), State.FAILED, candidate.desired().fingerprint().exported(),
								previous.fingerprint().exported(), previous.handle().generation(), "bundle-removal-failed");
						categories.add("bundle-removal-failed");
						continue;
						}
					}
					mark(candidate.key(), previous == null ? State.BLOCKED : State.STALE,
							candidate.desired().fingerprint().exported(),
							previous == null ? null : previous.fingerprint().exported(),
							previous == null ? 0 : previous.handle().generation(), failureCategory(candidate.desired()));
					categories.add(failureCategory(candidate.desired()));
					continue;
				}
				EffectiveFingerprint desiredFingerprint = candidate.desired().fingerprint();
				if (previous != null && !previous.handle().active()) {
					if (!retire(previous)) {
						mark(candidate.key(), State.FAILED, desiredFingerprint.exported(), previous.fingerprint().exported(),
								previous.handle().generation(), "bundle-removal-failed");
						categories.add("bundle-removal-failed");
						continue;
					}
					active.remove(candidate.key());
					refreshReverseDependencies();
					previous = null;
				}
				if (previous != null && previous.handle().active()
						&& previous.fingerprint().sameEffectiveConfiguration(desiredFingerprint)) {
					active.put(candidate.key(), new Active(candidate.key(), previous.bundleId(), previous.handle(),
							desiredFingerprint, dependencyIds(candidate)));
					refreshReverseDependencies();
					mark(candidate.key(), State.INSTALLED, desiredFingerprint.exported(), desiredFingerprint.exported(),
							previous.handle().generation(), "");
					unchanged++;
					continue;
				}
				try {
					String bundleId = candidate.desired().bundle().bundleId();
					Supplier<RoutesConfiguration> definition = definitions.freshDefinition(candidate);
					if (superseded(epoch, candidate.key(), dependencies)) { superseded = true; continue; }
					publishPendingDependencies(bundleId, dependencyIds(candidate));
					long generation = component.nextBundleGeneration(bundleId);
					DmpComponent.StagedBundle staged = component.stageBundle(bundleId, generation, definition);
					if (superseded(epoch, candidate.key(), dependencies)) {
						staged.close();
						if (previous == null) component.abandonUninstalledBundle(bundleId);
						refreshReverseDependencies();
						superseded = true;
						continue;
					}
					ManagedBundle installed = previous == null
							? component.installBundle(staged) : component.replaceBundle(previous.handle(), staged);
					active.put(candidate.key(), new Active(candidate.key(), bundleId, installed, desiredFingerprint,
							dependencyIds(candidate)));
					refreshReverseDependencies();
					revisions.merge(candidate.key(), 1L, Long::sum);
					mark(candidate.key(), State.INSTALLED, desiredFingerprint.exported(), desiredFingerprint.exported(), generation, "");
					changed++;
				} catch (RuntimeException failure) {
					ManagedBundle surviving = component.managedBundle(previous == null
							? candidate.desired().bundle().bundleId() : previous.bundleId());
					if (surviving != null) {
						active.put(candidate.key(), new Active(candidate.key(), surviving.bundleId(), surviving,
								previous == null ? desiredFingerprint : previous.fingerprint(),
								previous == null ? dependencyIds(candidate) : previous.dependencies()));
					} else {
						active.remove(candidate.key());
						component.abandonUninstalledBundle(candidate.desired().bundle().bundleId());
					}
					refreshReverseDependencies();
					mark(candidate.key(), State.FAILED, desiredFingerprint.exported(),
							surviving == null || previous == null ? null : previous.fingerprint().exported(),
							surviving == null ? 0 : surviving.generation(), "bundle-installation-failed");
					if (superseded(epoch, candidate.key(), dependencies)) { superseded = true; continue; }
					categories.add("bundle-installation-failed");
				}
			}
			appliedEpoch = epoch;
			synchronized (dependencyEpochs) {
				superseded |= epoch != invalidationEpoch.get();
				dependencyEpochs.entrySet().removeIf(entry -> entry.getValue() <= epoch);
			}
			if (superseded) categories.add("scan-superseded");
			return report(true, superseded, changed, unchanged, removed, categories);
		}
	}

	public List<MappingStatus> status() {
		synchronized (lock) {
			return statuses.entrySet().stream().map(entry -> new MappingStatus(entry.getKey(), entry.getValue().state(),
					entry.getValue().desired(), entry.getValue().installed(), entry.getValue().generation(),
					entry.getValue().category(), revisions.getOrDefault(entry.getKey(), 0L))).toList();
		}
	}

	public long acceptedInvalidationCount() { return invalidationEpoch.get(); }
	public int consecutiveScanFailures() { return consecutiveFullScanFailures.get(); }
	public int overflowMappingCount() { return overflowMappings; }
	public long invalidationOverflowCount() { return invalidationOverflows.get(); }
	public long ignoredEventCount() { return ignoredEvents.get(); }
	public boolean repairPending() { return invalidationEpoch.get() > appliedEpoch; }
	public long fullScanCount() { return discovery == null ? 0L : discovery.fullScanCount(); }
	public long targetedScanCount() { return discovery == null ? 0L : discovery.targetedScanCount(); }

	private boolean superseded(long epoch, MappingKey key, Set<String> dependencies) {
		synchronized (dependencyEpochs) {
			if (fullRescanEpoch > epoch || dependencyEpochs.getOrDefault(key.aimcId(), 0L) > epoch) return true;
			return dependencies.stream().anyMatch(id -> dependencyEpochs.getOrDefault(id, 0L) > epoch);
		}
	}

	private Report failWholeScan(String category) {
		for (Active value : active.values()) mark(value.key(), State.STALE, null, value.fingerprint().exported(),
				value.handle().generation(), category);
		return report(false, false, 0, 0, 0, List.of(category));
	}

	private boolean retire(Active activeBundle) {
		try {
			ManagedBundle owned = component.managedBundle(activeBundle.bundleId());
			if (owned == null) {
				component.abandonUninstalledBundle(activeBundle.bundleId());
				return true;
			}
			long generation = component.nextBundleGeneration(activeBundle.bundleId());
			component.removeBundle(owned, generation);
			return true;
		} catch (RuntimeException failure) { return false; }
	}

	private static Set<String> dependencyIds(MappingCandidate candidate) {
		Set<String> ids = candidate.desired().desired().dependencies().stream().map(
				MappingPlanCompiler.Dependency::id).collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
		ids.add(candidate.key().aimcId());
		return Set.copyOf(ids);
	}

	private void refreshReverseDependencies() {
		Map<String, Set<String>> reverse = new LinkedHashMap<>();
		for (Active installed : active.values()) for (String dependency : installed.dependencies())
			reverse.computeIfAbsent(dependency, ignored -> new LinkedHashSet<>()).add(installed.bundleId());
		Map<String, Set<String>> immutable = new LinkedHashMap<>();
		reverse.forEach((id, bundleIds) -> immutable.put(id, Set.copyOf(bundleIds)));
		activeBundleDependencies = Map.copyOf(immutable);
	}

	private void publishPendingDependencies(String bundleId, Set<String> dependencies) {
		Map<String, Set<String>> next = new LinkedHashMap<>();
		activeBundleDependencies.forEach((id, bundleIds) -> next.put(id, new LinkedHashSet<>(bundleIds)));
		for (String dependency : dependencies)
			next.computeIfAbsent(dependency, ignored -> new LinkedHashSet<>()).add(bundleId);
		Map<String, Set<String>> immutable = new LinkedHashMap<>();
		next.forEach((id, bundleIds) -> immutable.put(id, Set.copyOf(bundleIds)));
		activeBundleDependencies = Map.copyOf(immutable);
	}

	private static boolean confirmedMissingDependency(DesiredBundle desired) {
		return desired.desired().failures().stream()
				.anyMatch(failure -> failure.status() == AasRepositoryClient.Status.NOT_FOUND);
	}

	private static Set<MappingKey> restConflicts(Iterable<MappingCandidate> candidates) {
		Map<String, List<MappingKey>> reservations = new LinkedHashMap<>();
		for (MappingCandidate candidate : candidates) {
			if (!candidate.desired().valid() || candidate.desired().bundle() == null) continue;
			for (RouteConfiguration route : candidate.desired().bundle().configuration().getRoutes()) {
				if (!"bridge".equals(route.getRouteTrigger()) || route.getTriggerData() == null) continue;
				Object method = route.getTriggerData().get("restMethod");
				Object path = route.getTriggerData().get("restPath");
				if (method instanceof String verb && path instanceof String restPath && !restPath.isBlank()) {
					String reservation = verb.toUpperCase(java.util.Locale.ROOT) + " " + restPath;
					reservations.computeIfAbsent(reservation, ignored -> new ArrayList<>()).add(candidate.key());
				}
			}
		}
		Set<MappingKey> conflicts = new LinkedHashSet<>();
		reservations.values().stream().filter(keys -> keys.size() > 1).forEach(conflicts::addAll);
		return Set.copyOf(conflicts);
	}

	private static String failureCategory(DesiredBundle desired) {
		if (!desired.desired().failures().isEmpty())
			return "mapping-resolution-" + desired.desired().failures().get(0).status().name().toLowerCase(java.util.Locale.ROOT);
		return desired.issues().isEmpty() ? "invalid-desired-mapping" : "unsupported-connector-capability";
	}

	private void mark(MappingKey key, State state, String desired, String installed, long generation, String category) {
		statuses.remove(key);
		statuses.put(key, new MutableStatus(state, desired, installed, generation, category));
	}

	private Report report(boolean complete, boolean superseded, int changed, int unchanged, int removed, List<String> categories) {
		Report result = new Report(complete, superseded, changed, unchanged, removed, status(), categories);
		try { reportListener.accept(result); } catch (RuntimeException ignored) { }
		return result;
	}

	private void queueScan() {
		ScheduledExecutorService worker = executor;
		long retryWait = retryNotBeforeNanos - System.nanoTime();
		if (retryWait > 0) {
			scheduleRetry(Math.max(1, TimeUnit.NANOSECONDS.toMillis(retryWait)));
			return;
		}
		if (closed || worker == null || !scanQueued.compareAndSet(false, true)) return;
		synchronized (scheduleLock) {
			if (scheduledRetry != null) scheduledRetry.cancel(false);
			scheduledRetry = null;
		}
		try { worker.execute(() -> {
			boolean retry = false;
			long delay = retryMillis;
			try {
				Report report;
				try { report = reconcile(false); }
				catch (RuntimeException failure) { report = failWholeScan("repository-scan-failed"); }
				retry = !report.complete() || hasTransientFailure(report.categories());
				if (!retry) consecutiveFullScanFailures.set(0);
				else consecutiveFullScanFailures.updateAndGet(count -> Math.min(10, count + 1));
				delay = nextRetryDelay();
				retryNotBeforeNanos = retry ? System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay) : 0;
			} finally {
				scanQueued.set(false);
				if (!closed && !retry && invalidationEpoch.get() != appliedEpoch) queueScan();
				else if (retry) scheduleRetry(delay);
			}
		}); } catch (java.util.concurrent.RejectedExecutionException closedDuringSubmit) {
			scanQueued.set(false);
		}
	}

	private long nextRetryDelay() {
		int failures = consecutiveFullScanFailures.get();
		if (failures == 0) return retryMillis;
		long multiplier = 1L << Math.min(4, failures - 1);
		return Math.min(TimeUnit.HOURS.toMillis(1), retryMillis * multiplier);
	}

	private static boolean hasTransientFailure(List<String> categories) {
		return categories.stream().anyMatch(category -> category.equals("aimc-fetch-failed")
				|| category.equals("aimc-membership-unconfirmed") || category.startsWith("bundle-")
				|| category.equals("aimc-scan-timeout") || category.equals("aimc-scan-interrupted")
				|| category.contains("transient_failure") || category.startsWith("repository-"));
	}

	private void scheduleRetry(long delayMillis) {
		ScheduledExecutorService worker = executor;
		if (closed || worker == null || scanQueued.get()) return;
		synchronized (scheduleLock) {
			if (closed || worker != executor || scanQueued.get()) return;
			if (scheduledRetry != null) scheduledRetry.cancel(false);
			scheduledRetry = worker.schedule(() -> { retryNotBeforeNanos = 0; queueScan(); }, Math.max(0, delayMillis), TimeUnit.MILLISECONDS);
		}
	}

	@Override public synchronized void close() {
		closed = true;
		ScheduledExecutorService worker = executor;
		if (worker != null) {
			if (scheduledRetry != null) scheduledRetry.cancel(false);
			worker.shutdownNow();
			try { worker.awaitTermination(2, TimeUnit.SECONDS); }
			catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			executor = null;
		}
	}
}
