package dk.aau.smartproductionlab.dmp.opcua.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/** A request not sent yet waits while its connection (re)connects, then goes; it does not wait for ever. */
class OpcUaConnectionsTest {
	@Test
	void waitsWhileTheConnectionReconnects() throws InterruptedException {
		AtomicInteger asked = new AtomicInteger();
		// No client for the first three looks, as while the connection manager connects anew.
		assertEquals("client", OpcUaConnections.await(() -> asked.incrementAndGet() > 3 ? "client" : null, 5_000));
		assertEquals(4, asked.get());
	}

	@Test
	void givesUpAfterItsTime() throws InterruptedException {
		long started = System.nanoTime();
		assertNull(OpcUaConnections.await(() -> null, 300));
		long waited = (System.nanoTime() - started) / 1_000_000;
		assertTrue(waited >= 300 && waited < 2_000, "waited " + waited + " ms");
		assertNull(OpcUaConnections.awaitClient(null, 0));
	}
}
