/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import dk.aau.smartproductionlab.dmp.core.audit.AuditLog;
import dk.aau.smartproductionlab.dmp.core.audit.AuditPoint;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService.MappingStatus;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService.Report;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService.State;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey;
import org.junit.jupiter.api.Test;

class MappingAuditTest {
	private static Report report(State state, String fingerprint) {
		return new Report(true, false, 0, 0, 0, List.of(new MappingStatus(new MappingKey("urn:aimc", "mapping_0", 0),
				state, fingerprint, state == State.INSTALLED ? fingerprint : null, 1, "", 1)), List.of());
	}

	@Test
	void logsEachMappingChangeOnce() {
		List<AuditPoint> points = new CopyOnWriteArrayList<>();
		AuditLog capture = points::add;
		AuditLog.install(capture);
		try {
			MappingAudit audit = new MappingAudit();
			audit.publish(report(State.INSTALLED, "f1"));
			audit.publish(report(State.INSTALLED, "f1"));
			audit.publish(report(State.INSTALLED, "f2"));
			audit.publish(report(State.BLOCKED, "f3"));
		} finally {
			AuditLog.uninstall(capture);
		}
		assertEquals(List.of("installed", "installed", "blocked"), points.stream().map(p -> p.tags().get("state")).toList(),
				"an unchanged report logs nothing");
		assertEquals("urn:aimc", points.get(0).tags().get("aimc"));
		assertEquals("mapping_0", points.get(0).tags().get("mapping"));
		assertEquals("f2", points.get(1).fields().get("installedFingerprint"));
	}
}
