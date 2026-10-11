package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import java.util.List;

import org.apache.camel.CamelContext;

/** OPC UA module SPI for the server-declared positional Method signature. */
public interface OpcUaMethodMetadataProvider {
	MethodSignature read(CamelContext context, String commandEndpointUri);

	/** Converts already ordered Java values to the OPC UA client's native Variant[] body. */
	Object toVariantArray(List<Object> orderedValues, List<Argument> arguments);

	record MethodSignature(List<Argument> inputs, List<Argument> outputs) {
		public MethodSignature {
			inputs = List.copyOf(inputs);
			outputs = List.copyOf(outputs);
		}
	}

	record Argument(String name, String dataType, int valueRank, List<Long> arrayDimensions) {
		public Argument {
			arrayDimensions = List.copyOf(arrayDimensions);
		}
	}
}
