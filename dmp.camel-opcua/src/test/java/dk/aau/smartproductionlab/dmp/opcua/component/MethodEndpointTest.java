package dk.aau.smartproductionlab.dmp.opcua.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSinkConfiguration;
import dk.aau.smartproductionlab.dmp.opcua.configuration.OpcuaDataSourceConfiguration;

/** A Method whose owning Object is named by a browse path, as a server that numbers its nodes anew at each start needs. */
class MethodEndpointTest {
	private static final String OWNER = "/0:Objects/1:Filling/1:Skills/1:Dispensing";
	private static final String METHOD = OWNER + "/1:Start";

	@Test
	void aMethodOwnedByABrowsePathKeepsBothAndReachesTheServerAtTheConfiguredHost() throws Exception {
		Map<String, String> options = new LinkedHashMap<>();
		options.put("method", "RAW(" + METHOD + ")");
		var sink = new OpcuaDataSinkConfiguration("command", "host.docker.internal", 4841, "", OWNER, null, null, options);
		String uri = sink.getConnectionURI();
		assertTrue(uri.contains("browsePath=RAW(" + OWNER + ")"), uri);
		assertTrue(uri.contains("overrideHost=true"), uri);

		try (var context = new DefaultCamelContext()) {
			var endpoint = assertInstanceOf(DmpMiloClientEndpoint.class, context.getEndpoint(uri));
			// The owner is resolved from the browse path at the call; the signature is read from the method alone.
			assertEquals(OWNER, endpoint.getBrowsePath());
			assertNull(endpoint.getNode());
			assertEquals(METHOD, endpoint.getMethod());
			// The host the server names in its endpoints (e.g. its hostname) need not resolve here.
			assertTrue(endpoint.getConfiguration().isOverrideHost());
		}
	}

	@Test
	void anObservedVariableIsReachedAtTheConfiguredHostToo() {
		var source = new OpcuaDataSourceConfiguration("state", "host.docker.internal", 4841, "", OWNER + "/1:State", null, null,
				new LinkedHashMap<>());
		assertTrue(source.getConnectionURI().contains("overrideHost=true"), source.getConnectionURI());
	}
}
