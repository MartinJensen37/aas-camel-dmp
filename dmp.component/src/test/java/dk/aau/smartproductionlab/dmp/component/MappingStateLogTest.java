/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;

import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService.MappingStatus;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService.Report;
import dk.aau.smartproductionlab.dmp.management.repository.MappingReconciliationService.State;
import dk.aau.smartproductionlab.dmp.management.repository.RepositoryDiscovery.MappingKey;

class MappingStateLogTest {
	private record Line(boolean warning, String text) { }

	private static Report report(State state, String fingerprint, String category) {
		return new Report(true, false, 0, 0, 0, List.of(new MappingStatus(new MappingKey("urn:aimc", "mapping_0", 0),
				state, fingerprint, state == State.INSTALLED ? fingerprint : null, 1, category, 1)), List.of());
	}

	@Test
	void logsEachMappingChangeOnceAndBlockedOnesAsWarnings() {
		List<Line> lines = new CopyOnWriteArrayList<>();
		MappingStateLog log = new MappingStateLog((warning, text) -> lines.add(new Line(warning, text)));
		log.publish(report(State.INSTALLED, "f1", ""));
		log.publish(report(State.INSTALLED, "f1", ""));
		log.publish(report(State.INSTALLED, "f2", ""));
		log.publish(report(State.BLOCKED, "f3", "invalid-reference"));

		assertEquals(3, lines.size(), "an unchanged report logs nothing");
		assertFalse(lines.get(0).warning());
		assertTrue(lines.get(0).text().contains(" is installed"), lines.get(0).text());
		assertTrue(lines.get(2).warning());
		assertTrue(lines.get(2).text().contains(" is blocked (invalid-reference)"), lines.get(2).text());
		assertFalse(lines.get(2).text().contains("urn:aimc"), "repository IDs appear as tokens");
		assertTrue(lines.get(2).text().contains(ManagedRuntimeStatus.token("urn:aimc")));
	}
}
