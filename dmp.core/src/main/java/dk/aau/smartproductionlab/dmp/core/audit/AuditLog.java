/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.core.audit;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Log of AAS-side interactions (reads, events, writes, Operation calls). An
 * implementation must return at once and never throw: logging may lose points
 * but must never hold back or fail the data flow. The process has one current
 * log; without one installed, points are discarded.
 */
@FunctionalInterface
public interface AuditLog {

	void record(AuditPoint point);

	AuditLog NONE = point -> { };

	/**
	 * Builds and records a point through the current log. The point is only
	 * built when a log is installed; anything building or recording throws is
	 * swallowed.
	 */
	static void emit(java.util.function.Supplier<AuditPoint> point) {
		AuditLog log = Holder.CURRENT.get();
		if (log == NONE) return;
		try { log.record(point.get()); }
		catch (RuntimeException | LinkageError ignored) { /* logging must never fail the data flow */ }
	}

	/** True when a log is installed, so callers can skip building points. */
	static boolean enabled() {
		return Holder.CURRENT.get() != NONE;
	}

	static void install(AuditLog log) {
		Holder.CURRENT.set(log == null ? NONE : log);
	}

	/** Removes {@code log} if it is still the current one. */
	static void uninstall(AuditLog log) {
		Holder.CURRENT.compareAndSet(log, NONE);
	}

	final class Holder {
		private static final AtomicReference<AuditLog> CURRENT = new AtomicReference<>(NONE);
		private Holder() { }
	}
}
