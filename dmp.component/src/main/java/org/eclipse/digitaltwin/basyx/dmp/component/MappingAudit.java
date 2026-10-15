/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.component;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditLog;
import org.eclipse.digitaltwin.basyx.dmp.core.audit.AuditPoint;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.MappingReconciliationService;
import org.eclipse.digitaltwin.basyx.dmp.management.repository.RepositoryDiscovery.MappingKey;

/**
 * Logs mapping lifecycle changes ({@code dmp_mapping}) from reconciliation
 * reports: a point whenever a mapping's state, fingerprints or category change,
 * so the first report logs every mapping's starting state.
 */
final class MappingAudit {
	private record Seen(MappingReconciliationService.State state, String desired, String installed, String category) { }

	private final Map<MappingKey, Seen> seen = new ConcurrentHashMap<>();

	void publish(MappingReconciliationService.Report report) {
		for (MappingReconciliationService.MappingStatus mapping : report.mappings()) {
			Seen current = new Seen(mapping.state(), mapping.desiredFingerprint(), mapping.installedFingerprint(),
					mapping.category());
			if (Objects.equals(seen.put(mapping.key(), current), current)) continue;
			AuditLog.emit(() -> AuditPoint.of("dmp_mapping").tag("aimc", mapping.key().aimcId())
					.tag("mapping", mapping.key().mappingIdentity())
					.tag("state", mapping.state().name().toLowerCase(java.util.Locale.ROOT))
					.field("generation", mapping.generation()).field("category", mapping.category())
					.field("desiredFingerprint", mapping.desiredFingerprint())
					.field("installedFingerprint", mapping.installedFingerprint()));
		}
	}
}
