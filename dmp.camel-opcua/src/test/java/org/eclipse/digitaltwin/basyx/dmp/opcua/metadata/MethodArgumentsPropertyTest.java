package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.eclipse.milo.opcua.stack.core.StatusCodes;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.types.structured.Argument;
import org.junit.jupiter.api.Test;

/** OPC UA makes a Method's InputArguments/OutputArguments properties optional: absent means none. */
class MethodArgumentsPropertyTest {

	@Test
	void anAbsentArgumentsPropertyMeansNoArguments() {
		// What Milo answers for a Method without OutputArguments (e.g. an invoke-only Halt).
		var absent = CompletableFuture.<Argument[]>failedFuture(new UaException(StatusCodes.Bad_NotFound, "not found"));
		assertEquals(0, CamelOpcUaMethodMetadataProvider.arguments(absent).length);
	}

	@Test
	void otherReadFailuresStillFail() {
		var broken = CompletableFuture.<Argument[]>failedFuture(new UaException(StatusCodes.Bad_CommunicationError, "down"));
		assertThrows(CompletionException.class, () -> CamelOpcUaMethodMetadataProvider.arguments(broken));
	}
}
