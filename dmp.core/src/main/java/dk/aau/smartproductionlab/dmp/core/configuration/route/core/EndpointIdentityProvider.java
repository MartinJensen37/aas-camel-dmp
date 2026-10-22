package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

/** Connector-owned equivalence key for endpoints that can share one grouped send. */
public interface EndpointIdentityProvider {
	boolean supports(String endpointUri);

	String identity(String endpointUri);
}
