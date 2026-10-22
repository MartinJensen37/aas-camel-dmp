package dk.aau.smartproductionlab.dmp.core.configuration.route.pipeline;

import java.util.List;

/** Protocol-neutral payload/mapping failure; source connectors decide how to report it. */
public final class PayloadValidationException extends RuntimeException {
	public enum Stage { COMMAND, RESPONSE, MAPPING }

	private final Stage stage;
	private final List<String> violations;

	public PayloadValidationException(Stage stage, String message, List<String> violations) {
		super(message);
		this.stage = stage;
		this.violations = List.copyOf(violations);
	}

	public Stage getStage() { return stage; }
	public List<String> getViolations() { return violations; }
}
