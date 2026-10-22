package dk.aau.smartproductionlab.dmp.executable.regression;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteCreatorFactory;
import dk.aau.smartproductionlab.dmp.core.component.DmpRuntimeExtension;
import org.junit.jupiter.api.Test;

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
				.anyMatch(extension -> extension instanceof dk.aau.smartproductionlab.dmp.component.health.HealthRouteExtension));
	}
}
