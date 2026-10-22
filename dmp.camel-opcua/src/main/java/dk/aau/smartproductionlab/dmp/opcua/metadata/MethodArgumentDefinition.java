package dk.aau.smartproductionlab.dmp.opcua.metadata;

import java.util.List;

import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;

/** An OPC UA method argument definition in server-declared order. */
public record MethodArgumentDefinition(String name, NodeId dataType, Integer valueRank,
		List<UInteger> arrayDimensions) {
	public MethodArgumentDefinition {
		arrayDimensions = arrayDimensions == null ? List.of() : List.copyOf(arrayDimensions);
	}
}
