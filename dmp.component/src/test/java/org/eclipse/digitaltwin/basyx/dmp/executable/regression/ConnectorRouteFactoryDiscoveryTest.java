package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.component.DmpRuntimeExtension;
import org.junit.Test;

public class ConnectorRouteFactoryDiscoveryTest {
	@Test
	public void componentClasspathDiscoversCoreAndAasRouteFactoriesWithoutCoreImports() {
		Set<String> triggers = ServiceLoader.load(RouteCreatorFactory.class).stream()
				.map(ServiceLoader.Provider::get)
				.map(RouteCreatorFactory::routeTrigger)
				.collect(Collectors.toSet());
		assertTrue(triggers.contains("execution-policy"));
		assertTrue(triggers.contains("bridge"));
		assertFalse(triggers.contains("write-delegation"));
		assertTrue(ServiceLoader.load(DmpRuntimeExtension.class).stream()
				.map(ServiceLoader.Provider::get)
				.anyMatch(extension -> extension instanceof org.eclipse.digitaltwin.basyx.dmp.component.health.HealthRouteExtension));
	}
}
