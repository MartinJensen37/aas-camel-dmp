package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/** Compiled affordance schema guard; the node sets DMP_AFFORDANCE_VALIDATION. */
public final class AffordanceValidation {
	private static final Logger LOGGER = LoggerFactory.getLogger(AffordanceValidation.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final JsonSchemaFactory FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

	public enum Mode { OFF, ON, REPORT }

	private final Mode mode;
	private final String label;
	private final JsonSchema validator;

	public AffordanceValidation(Object schema, String label) {
		this(schema, label, configuredMode());
	}

	public AffordanceValidation(Object schema, String label, Mode mode) {
		this.mode = mode;
		this.label = label;
		if (schema != null && !(schema instanceof java.util.Map<?, ?>) && !(schema instanceof Boolean)) {
			throw new IllegalArgumentException("Invalid JSON Schema for " + label + ": expected object or boolean");
		}
		try {
			// Compile at route build, including in OFF mode: malformed descriptors fail loudly.
			this.validator = schema == null ? null : FACTORY.getSchema(MAPPER.valueToTree(schema));
		} catch (RuntimeException e) {
			throw new IllegalArgumentException("Invalid JSON Schema for " + label, e);
		}
	}

	public static Mode configuredMode() {
		String value = System.getenv().getOrDefault("DMP_AFFORDANCE_VALIDATION", "OFF");
		try {
			return Mode.valueOf(value.trim().toUpperCase(java.util.Locale.ROOT));
		} catch (IllegalArgumentException e) {
			throw new IllegalArgumentException("DMP_AFFORDANCE_VALIDATION must be OFF, ON or REPORT: " + value, e);
		}
	}

	/** Returns false only when an incoming invalid contribution must be dropped. */
	public boolean incoming(Object value) {
		return validate(value, false);
	}

	/** An outgoing invalid payload is a mapping failure, never sent to the endpoint. */
	public void outgoing(Object value) {
		if (!validate(value, true)) {
			throw new PayloadValidationException(
					PayloadValidationException.Stage.COMMAND,
					"Invalid outgoing affordance payload for " + label,
					messages(value).stream().map(ValidationMessage::getMessage).toList());
		}
	}

	/** Correlated synchronous action replies fail their caller instead of being dropped. */
	public void correlatedResponse(Object value) {
		if (validator == null || mode == Mode.OFF) {
			return;
		}
		Set<ValidationMessage> violations = messages(value);
		if (violations.isEmpty()) {
			return;
		}
		LOGGER.warn("response affordance {} violates schema: {}", label, violations);
		if (mode == Mode.ON) {
			throw new PayloadValidationException(
					PayloadValidationException.Stage.RESPONSE,
					"Device response violates the declared output schema", violations.stream()
							.map(ValidationMessage::getMessage).toList());
		}
	}

	private boolean validate(Object value, boolean outgoing) {
		if (validator == null || mode == Mode.OFF) {
			return true;
		}
		JsonNode node = MAPPER.valueToTree(value);
		Set<ValidationMessage> violations = validator.validate(node);
		if (violations.isEmpty()) {
			return true;
		}
		LOGGER.warn("{} affordance {} violates schema: {}", outgoing ? "outgoing" : "incoming", label, violations);
		return mode == Mode.REPORT;
	}

	private Set<ValidationMessage> messages(Object value) {
		return validator == null ? Set.of()
				: validator.validate(MAPPER.valueToTree(JsonCodec.toJsonCompatibleValue(value)));
	}
}
