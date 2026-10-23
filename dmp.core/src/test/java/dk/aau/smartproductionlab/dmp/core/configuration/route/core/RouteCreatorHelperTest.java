package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.junit.jupiter.api.Test;

class RouteCreatorHelperTest {
	private final RoutesConfiguration routes = new RoutesConfiguration();

	@Test
	void missingDatasourceNamesRouteAndIdEvenWhenMapIsNull() {
		assertEquals("route r1 references unknown datasource 'input'",
				assertThrows(IllegalArgumentException.class,
						() -> RouteCreatorHelper.getDataSourceEndpoint(routes, "input", "r1")).getMessage());
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
						() -> RouteCreatorHelper.getDataSinkEndpoint(routes, "output", "r2")).getMessage());
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

		assertEquals("direct:input", RouteCreatorHelper.getDataSourceEndpoint(routes, "input", "r4"));
		assertArrayEquals(new String[] {"direct:output"},
				RouteCreatorHelper.getDataSinkEndpoints(routes, List.of("output"), "r4"));
		assertArrayEquals(new String[] {"direct:convert"},
				RouteCreatorHelper.getDataTransformerEndpoints(routes, List.of("convert"), "r4"));
	}
}
