/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey;

/**
 * One application-log line per mapping whose state, fingerprints, category, cause or diagnostics changed: a
 * warning when it is blocked or failed, otherwise information, followed by one line per diagnostic. Repository IDs
 * appear as the tokens of {@code /management/status}; diagnostic messages are the compiler's, as served by
 * {@code /management/mappings}.
 */
final class MappingStateLog {
	private static final Logger LOGGER = LoggerFactory.getLogger(MappingStateLog.class);

	private record Seen(MappingReconciliationService.State state, String desired, String installed, String category,
			String cause, java.util.List<MappingReconciliationService.Diagnostic> diagnostics) { }

	private final Map<MappingKey, Seen> seen = new ConcurrentHashMap<>();
	/** Receives (warning?, line); the logger by default, a list in tests. */
	private final BiConsumer<Boolean, String> sink;

	MappingStateLog() {
		this((warning, line) -> { if (warning) LOGGER.warn(line); else LOGGER.info(line); });
	}

	MappingStateLog(BiConsumer<Boolean, String> sink) { this.sink = Objects.requireNonNull(sink); }

	void publish(MappingReconciliationService.Report report) {
		for (MappingReconciliationService.MappingStatus mapping : report.mappings()) {
			Seen current = new Seen(mapping.state(), mapping.desiredFingerprint(), mapping.installedFingerprint(),
					mapping.category(), mapping.cause(), mapping.diagnostics());
			if (Objects.equals(seen.put(mapping.key(), current), current)) continue;
			boolean warning = mapping.state() == MappingReconciliationService.State.BLOCKED
					|| mapping.state() == MappingReconciliationService.State.FAILED;
			String category = mapping.category() == null || mapping.category().isBlank() ? "" : " (" + mapping.category()
					+ (mapping.cause().isBlank() ? "" : ": " + mapping.cause()) + ")";
			String name = "mapping " + ManagedRuntimeStatus.token(mapping.key().mappingIdentity()) + " of AIMC "
					+ ManagedRuntimeStatus.token(mapping.key().aimcId());
			sink.accept(warning, name + " is " + mapping.state().name().toLowerCase(java.util.Locale.ROOT) + category
					+ ", generation " + mapping.generation());
			for (var diagnostic : mapping.diagnostics())
				sink.accept(warning, name + ": " + (diagnostic.path().isBlank() ? "" : diagnostic.path() + ": ") + diagnostic.message());
		}
	}
}
