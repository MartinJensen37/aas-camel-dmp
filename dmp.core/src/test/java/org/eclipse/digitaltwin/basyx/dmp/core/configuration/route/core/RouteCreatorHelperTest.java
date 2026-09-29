package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteCreatorHelper;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.junit.jupiter.api.Test;

class RouteCreatorHelperTest {
	private final RoutesConfiguration routes = new RoutesConfiguration();

	@Test
	void missingDatasourceNamesRouteAndIdEvenWhenMapIsNull() {
		assertEquals("route r1 references unknown datasource 'input'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataSourceEndpoints(routes, List.of("input"), "r1")).getMessage());
		routes.setDatasources(null);
		assertEquals("route r1 references unknown datasource 'input'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataSourceEndpoint(routes, "input", "r1")).getMessage());
	}

	@Test
	void missingDatasinkNamesRouteAndIdEvenWhenMapIsNull() {
		assertEquals("route r2 references unknown datasink 'output'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataSinkEndpoints(routes, List.of("output"), "r2")).getMessage());
		routes.setDatasinks(null);
		assertEquals("route r2 references unknown datasink 'output'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataSinkMapping(routes, Map.of("output", new String[0]), "r2")).getMessage());
	}

	@Test
	void missingTransformerNamesRouteAndIdEvenWhenMapIsNull() {
		assertEquals("route r3 references unknown transformer 'convert'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataTransformerEndpoints(routes, List.of("convert"), "r3")).getMessage());
		routes.setTransformers(null);
		assertEquals("route r3 references unknown transformer 'convert'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataTransformerEndpoints(routes, List.of("convert"), "r3")).getMessage());
		routes.addDatasink(new DataSinkConfiguration("output") {
			@Override
			public String getConnectionURI() {
				return "direct:output";
			}
		});
		assertEquals("route r3 references unknown transformer 'convert'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataSinkMapping(routes,
								Map.of("output", new String[] {"convert"}), "r3")).getMessage());
	}

	@Test
	void knownEntitiesResolveThroughMapping() {
		routes.addDatasource(new DataSourceConfiguration("input", "", 0) {
			@Override
			public String getConnectionURI() {
				return "direct:input";
			}
		});
		routes.addDatasink(new DataSinkConfiguration("output") {
			@Override
			public String getConnectionURI() {
				return "direct:output";
			}
		});
		routes.addTransformer(new DataTransformerConfiguration("convert") {
			@Override
			public String getConnectionURI() {
				return "direct:convert";
			}
		});

		assertArrayEquals(new String[] {"direct:input"},
				RouteCreatorHelper.getDataSourceEndpoints(routes, List.of("input"), "r4"));
		assertArrayEquals(new String[] {"direct:output"},
				RouteCreatorHelper.getDataSinkEndpoints(routes, List.of("output"), "r4"));
		assertArrayEquals(new String[] {"direct:convert"},
				RouteCreatorHelper.getDataSinkMapping(routes, Map.of("output", new String[] {"convert"}), "r4")
						.get("direct:output"));
	}
}
