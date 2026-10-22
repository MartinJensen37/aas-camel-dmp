package dk.aau.smartproductionlab.dmp.aas.event;

import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.SourceEventAdapter;

/** BaSyx element-event connector adapter for normalized AAS source contributions. */
public final class AasPropertyEventAdapter implements SourceEventAdapter {
	public static final String SOURCE_KIND = "aas-element-event";

	@Override
	public String sourceKind() {
		return SOURCE_KIND;
	}

	@Override
	public Processor normalizer(DataSourceConfiguration configuration) {
		return new AasPropertyEventProcessor(configuration);
	}
}
