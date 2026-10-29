/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.opcua.component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.camel.component.milo.client.MiloClientConnection;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;

/**
 * A shared camel-milo connection has no client while its connection manager (re)connects: after a
 * server restart it learns of the stale session only from a failed request, then connects anew. A
 * request that has not been sent yet waits for the client instead of failing at once.
 */
public final class OpcUaConnections {
	private static final long POLL_MS = 100;

	private OpcUaConnections() { }

	/** The connection's client once it is connected, waiting up to {@code timeoutMs}; null if it is not by then. */
	public static OpcUaClient awaitClient(MiloClientConnection connection, long timeoutMs) throws InterruptedException {
		return await(connection == null ? () -> null : connection::getOpcUaClient, timeoutMs);
	}

	/** What {@code present} gives once it gives something, asked every 100 ms for up to {@code timeoutMs}; else null. */
	static <T> T await(Supplier<T> present, long timeoutMs) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeoutMs));
		while (true) {
			T found = present.get();
			if (found != null || System.nanoTime() >= deadline) return found;
			Thread.sleep(POLL_MS);
		}
	}
}
