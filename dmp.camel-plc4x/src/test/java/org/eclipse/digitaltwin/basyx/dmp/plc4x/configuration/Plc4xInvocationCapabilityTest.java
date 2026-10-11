package org.eclipse.digitaltwin.basyx.dmp.plc4x.configuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.junit.Test;

public class Plc4xInvocationCapabilityTest {
	@Test
	public void tagEndpointDoesNotMasqueradeAsSynchronousAction() {
		Plc4xSynchronousInvocationAdapter adapter = new Plc4xSynchronousInvocationAdapter();
		assertEquals(true, adapter.supports("plc4x:modbus-tcp://localhost:502"));

		ConnectorInvocationException error = assertThrows(ConnectorInvocationException.class,
				() -> adapter.invoke(new DefaultCamelContext(), "plc4x:modbus-tcp://localhost:502",
						Map.of("command", "start"), null));
		assertEquals("PLC4X_ACTION_UNSUPPORTED", error.getConnectorCode());
		assertEquals(ConnectorInvocationException.Category.CONFIGURATION, error.getCategory());
	}
}
