package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.Assert.assertTrue;

import java.util.ServiceLoader;
import java.util.List;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousInvocationAdapter;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.SynchronousSourceReadAdapter;
import org.junit.Test;

public class ConnectorInvocationAdapterDiscoveryTest {
	@Test
	public void componentClasspathDiscoversSynchronousConnectorAdapters() {
		List<SynchronousInvocationAdapter> adapters = ServiceLoader
				.load(SynchronousInvocationAdapter.class).stream()
				.map(ServiceLoader.Provider::get).toList();
		assertTrue(adapters.stream().anyMatch(a -> a.supports("milo-client:test")));
		assertTrue(adapters.stream().anyMatch(a -> a.supports("paho:command")));
		assertTrue(adapters.stream().anyMatch(a -> a.supports("http://device/action")));
		assertTrue(adapters.stream().anyMatch(a -> a.supports("plc4x:driver://device")));
	}

	@Test
	public void componentClasspathDiscoversFreshSourceReadAdapters() {
		List<SynchronousSourceReadAdapter> adapters = ServiceLoader
				.load(SynchronousSourceReadAdapter.class).stream()
				.map(ServiceLoader.Provider::get).toList();
		assertTrue(adapters.stream().anyMatch(a -> a.supports("milo-client:test")));
	}
}
