package org.eclipse.digitaltwin.basyx.dmp.aas.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ServiceLoader;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SourceEventAdapter;
import org.junit.jupiter.api.Test;

class AasPropertyEventAdapterRegistrationTest {
	@Test
	void connectorRegistersItsEventSourceAdapterThroughTheNeutralSpi() {
		SourceEventAdapter adapter = ServiceLoader.load(SourceEventAdapter.class).stream()
				.map(ServiceLoader.Provider::get)
				.filter(candidate -> AasPropertyEventAdapter.SOURCE_KIND.equals(candidate.sourceKind()))
				.findFirst().orElse(null);
		assertNotNull(adapter);
		assertEquals(AasPropertyEventAdapter.SOURCE_KIND, adapter.sourceKind());
	}
}
