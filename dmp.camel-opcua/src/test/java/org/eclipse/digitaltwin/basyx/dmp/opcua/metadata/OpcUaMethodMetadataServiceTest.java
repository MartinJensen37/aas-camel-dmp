package org.eclipse.digitaltwin.basyx.dmp.opcua.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.junit.jupiter.api.Test;

class OpcUaMethodMetadataServiceTest {
	private static final String SERVER = "opc.tcp://localhost:4840";
	private static final NodeId OBJECT = new NodeId(2, "Machine");
	private static final NodeId METHOD = new NodeId(2, "Machine/Start");

	@Test
	void cachesByServerObjectAndMethodUntilTtlExpires() {
		AtomicLong time = new AtomicLong();
		AtomicInteger reads = new AtomicInteger();
		OpcUaMethodMetadataService service = service(Duration.ofSeconds(10), time, reads);

		MethodMetadata first = service.getMethodMetadata(SERVER, OBJECT, METHOD);
		assertEquals(first, service.getMethodMetadata(SERVER, OBJECT, METHOD));
		assertEquals(1, reads.get());

		service.getMethodMetadata("opc.tcp://other:4840", OBJECT, METHOD);
		service.getMethodMetadata(SERVER, new NodeId(2, "Other"), METHOD);
		service.getMethodMetadata(SERVER, OBJECT, new NodeId(2, "Machine/Stop"));
		assertEquals(4, reads.get());

		time.set(Duration.ofSeconds(10).toNanos());
		assertNotSame(first, service.getMethodMetadata(SERVER, OBJECT, METHOD));
		assertEquals(5, reads.get());
	}

	@Test
	void refreshReplacesEntryAndInvalidationForcesNextRead() {
		AtomicLong time = new AtomicLong();
		AtomicInteger reads = new AtomicInteger();
		OpcUaMethodMetadataService service = service(Duration.ofMinutes(1), time, reads);

		MethodMetadata cached = service.getMethodMetadata(SERVER, OBJECT, METHOD);
		MethodMetadata refreshed = service.refresh(SERVER, OBJECT, METHOD);
		assertNotSame(cached, refreshed);
		assertEquals(refreshed, service.getMethodMetadata(SERVER, OBJECT, METHOD));
		service.invalidate(SERVER, OBJECT, METHOD);
		assertNotSame(refreshed, service.getMethodMetadata(SERVER, OBJECT, METHOD));
		service.invalidateAll();
		service.getMethodMetadata(SERVER, OBJECT, METHOD);
		assertEquals(4, reads.get());
	}

	@Test
	void returnedMetadataAndArgumentCollectionsAreImmutableAndOrdered() {
		AtomicInteger reads = new AtomicInteger();
		OpcUaMethodMetadataService service = service(Duration.ofMinutes(1), new AtomicLong(), reads);
		MethodMetadata metadata = service.getMethodMetadata(SERVER, OBJECT, METHOD);

		assertEquals(List.of("first", "second"), metadata.inputArguments().stream()
				.map(MethodArgumentDefinition::name).toList());
		assertEquals(new NodeId(0, 6), metadata.inputArguments().get(0).dataType());
		assertEquals(-1, metadata.inputArguments().get(0).valueRank());
		assertEquals(List.of(Unsigned.uint(2)), metadata.inputArguments().get(1).arrayDimensions());
		assertEquals(List.of("result"), metadata.outputArguments().stream()
				.map(MethodArgumentDefinition::name).toList());
		assertThrows(UnsupportedOperationException.class,
				() -> metadata.inputArguments().add(metadata.inputArguments().get(0)));
	}

	@Test
	void rejectsNonPositiveRefreshInterval() {
		assertThrows(IllegalArgumentException.class, () -> new OpcUaMethodMetadataService(Duration.ZERO));
		assertTrue(OpcUaMethodMetadataService.DEFAULT_REFRESH_AFTER.compareTo(Duration.ZERO) > 0);
	}

	private static OpcUaMethodMetadataService service(Duration ttl, AtomicLong time, AtomicInteger reads) {
		return new OpcUaMethodMetadataService(ttl, time::get, (url, objectId, methodId) -> {
			reads.incrementAndGet();
			return new MethodMetadata(
					List.of(new MethodArgumentDefinition("first", new NodeId(0, 6), -1, List.of()),
							new MethodArgumentDefinition("second", new NodeId(0, 6), 1, List.of(Unsigned.uint(2)))),
					List.of(new MethodArgumentDefinition("result", new NodeId(0, 1), -1, List.of())));
		});
	}
}
