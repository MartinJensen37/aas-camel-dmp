package dk.aau.smartproductionlab.dmp.component.health;

import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.component.DmpRuntimeExtension;

/** Adds the executable's HTTP health surface outside protocol-neutral core. */
public final class HealthRouteExtension implements DmpRuntimeExtension {
	@Override
	public void configure(CamelContext context) throws Exception {
		context.addRoutes(new HealthCheckRouteBuilder());
	}
}
