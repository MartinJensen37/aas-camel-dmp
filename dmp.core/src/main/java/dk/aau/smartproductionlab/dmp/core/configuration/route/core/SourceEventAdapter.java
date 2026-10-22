package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

import org.apache.camel.Processor;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSourceConfiguration;

/** Connector extension for normalizing push-source event envelopes. */
public interface SourceEventAdapter {
	String sourceKind();

	Processor normalizer(DataSourceConfiguration configuration);

	/** Generic contribution marker consumed by the route's readiness pipeline. */
	String CONTRIBUTION_PROPERTY = "dmp.source.contribution";
	/** Set when a connector has decoded the message into its logical Java value. */
	String NORMALIZED_VALUE_PROPERTY = "dmp.source.normalizedValue";
	/**
	 * Header with what the device itself declares for the value, as a JSON-schema map (e.g. an
	 * OPC UA variable's DataType). The source's AID schema must agree with it
	 * ({@link dk.aau.smartproductionlab.dmp.core.util.SchemaCompatibility}).
	 */
	String DEVICE_SCHEMA_HEADER = "DmpDeviceSchema";
}
