package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.camel.component.milo.MiloConstants;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.junit.jupiter.api.Test;

class OpcUaSynchronousSourceReadAdapterTest {
	@Test
	void recognizesMiloEndpointsAndExtractsTheReadNode() {
		var adapter = new OpcUaSynchronousSourceReadAdapter();
		String uri = "milo-client:opc.tcp://localhost:4840/server?node=RAW(ns=2;s=Machine.State)";

		assertTrue(adapter.supports(uri));
		assertFalse(adapter.supports("paho:state?brokerUrl=tcp://localhost:1883"));
		assertEquals("ns=2;s=Machine.State", OpcUaSynchronousSourceReadAdapter.nodeIdFromEndpoint(uri));
	}

	@Test
	void convertsFreshReadResultToTheCanonicalSourceObject() {
		var value = new DataValue(new Variant("READY"));

		assertEquals("READY", OpcUaSynchronousSourceReadAdapter
				.normalizeReadResult(List.of(value), "ns=2;s=Machine.State").get("value"));
	}

	@Test
	void requestsAnAwaitedMiloReadForTheSelectedNode() {
		var headers = OpcUaSynchronousSourceReadAdapter.readHeaders("ns=2;s=Machine.State");
		assertEquals(List.of("ns=2;s=Machine.State"), headers.get(MiloConstants.HEADER_NODE_IDS));
		assertEquals(Boolean.TRUE, headers.get(MiloConstants.HEADER_AWAIT));
	}

	@Test
	void awaitsMiloReadCompletionBeforeNormalizing() {
		var value = new DataValue(new Variant("READY"));
		var pending = new CompletableFuture<List<DataValue>>();
		var reader = CompletableFuture.supplyAsync(() -> OpcUaSynchronousSourceReadAdapter
				.normalizeReadResult(OpcUaSynchronousSourceReadAdapter.awaitReadResult(pending),
						"ns=2;s=Machine.State"));
		assertFalse(reader.isDone());
		pending.complete(List.of(value));
		assertEquals("READY", reader.join().get("value"));
	}

	@Test
	void failedMiloReadCompletionDoesNotBecomeAContribution() {
		var failed = new CompletableFuture<Object>();
		failed.completeExceptionally(new IllegalStateException("read failed"));
		IllegalStateException failure = assertThrows(IllegalStateException.class,
				() -> OpcUaSynchronousSourceReadAdapter.awaitReadResult(failed));
		assertEquals("OPC UA Read failed", failure.getMessage());
	}

	@Test
	void absentAndBadStatusReadsNeverBecomeContributions() {
		assertThrows(IllegalStateException.class,
				() -> OpcUaSynchronousSourceReadAdapter.normalizeReadResult(null, "ns=2;s=Machine.State"));
		assertThrows(IllegalStateException.class, () -> OpcUaSynchronousSourceReadAdapter.normalizeReadResult(
				List.of(new DataValue(new Variant("BAD"), StatusCode.BAD)), "ns=2;s=Machine.State"));
	}
}
