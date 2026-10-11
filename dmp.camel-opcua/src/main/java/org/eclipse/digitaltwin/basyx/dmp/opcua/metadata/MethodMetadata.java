package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import java.util.List;

/** Ordered input and output argument definitions for one OPC UA method. */
public record MethodMetadata(List<MethodArgumentDefinition> inputArguments,
		List<MethodArgumentDefinition> outputArguments) {
	public MethodMetadata {
		inputArguments = List.copyOf(inputArguments);
		outputArguments = List.copyOf(outputArguments);
	}
}
