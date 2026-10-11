package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import java.util.List;

/** Structured validation failure returned by an operation bridge. */
public final class OperationValidationException extends IllegalArgumentException {
	private final String code;
	private final int status;
	private final List<String> violations;

	public OperationValidationException(String code, int status, String message, List<String> violations) {
		super(message);
		this.code = code;
		this.status = status;
		this.violations = List.copyOf(violations);
	}

	public String getCode() { return code; }
	public int getStatus() { return status; }
	public List<String> getViolations() { return violations; }
}
