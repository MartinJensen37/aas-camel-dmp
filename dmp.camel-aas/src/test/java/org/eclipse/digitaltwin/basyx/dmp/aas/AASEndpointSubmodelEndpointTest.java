package org.eclipse.digitaltwin.basyx.dmp.aas;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

/**
 * The endpoint URI is {@code aas:} + an ABSOLUTE http(s) URL. Camel normalises
 * the scheme-relative remainder, so the captured path can come back with leading
 * slashes — and the HTTP client then rejects it with
 * {@code ProtocolException: Target host is not specified}, so every read of a
 * DotAAS-V3 source fails and the mapping never receives a value.
 */
class AASEndpointSubmodelEndpointTest {

	private static String captured(String uri) throws Exception {
		try (DefaultCamelContext context = new DefaultCamelContext()) {
			context.start();
			context.addComponent("aas", new AASComponent());
			AASComponent component = context.getComponent("aas", AASComponent.class);
			return ((AASEndpoint) component.createEndpoint(uri)).getSubmodelEndpoint();
		}
	}

	@Test
	public void anAbsoluteSubmodelUrlIsCapturedWithoutPadding() throws Exception {
		assertEquals("http://localhost:4001/submodels/abc",
				captured("aas:http://localhost:4001/submodels/abc"));
	}

	@Test
	public void aNormalisedPathDoesNotYieldASlashPrefixedHost() throws Exception {
		assertEquals("http://localhost:4001/submodels/abc",
				captured("aas:http://localhost:4001/submodels/abc?propertyPath=State"),
				"a leading slash makes the HTTP client fail with 'Target host is not specified'");
	}
}
