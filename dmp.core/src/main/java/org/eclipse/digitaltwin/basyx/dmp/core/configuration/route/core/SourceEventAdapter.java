package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core;

import org.apache.camel.Processor;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSourceConfiguration;

/** Connector extension for normalizing push-source event envelopes. */
public interface SourceEventAdapter {
	String sourceKind();

	Processor normalizer(DataSourceConfiguration configuration);

	/** Generic contribution marker consumed by the route's readiness pipeline. */
	String CONTRIBUTION_PROPERTY = "dmp.source.contribution";
	/** Set when a connector has decoded the message into its logical Java value. */
	String NORMALIZED_VALUE_PROPERTY = "dmp.source.normalizedValue";
}
