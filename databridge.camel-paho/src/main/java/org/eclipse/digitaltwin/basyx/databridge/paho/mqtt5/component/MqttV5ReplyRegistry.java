/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.databridge.paho.mqtt5.component;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The pending-reply registry shared by the {@code paho-mqtt5-inout:}
 * producers (register + await) and the {@code paho-mqtt5:} reply-consumer
 * routes' {@link MqttV5ReplyMatcher}s (complete). Process-scoped (one DMP
 * JVM); correlation ids are UUIDs, so a flat map suffices.
 *
 * @author aas-camel-dmp
 */
final class MqttV5ReplyRegistry {

	/** Reply-wait margin beyond the endpoint's replyTimeout so the future
	 * (not the get() race) decides the timeout. */
	static final long WAIT_MARGIN_MILLIS = 1_000L;

	private static final Map<String, CompletableFuture<String>> PENDING =
			new ConcurrentHashMap<>();

	private MqttV5ReplyRegistry() {
	}

	static CompletableFuture<String> register(String correlation) {
		return PENDING.computeIfAbsent(correlation, k -> new CompletableFuture<>());
	}

	/**
	 * Removes the entry when THIS future is still the registered one (an
	 * in-flight invocation that reused the correlation keeps its own).
	 */
	static void unregister(String correlation, CompletableFuture<String> future) {
		PENDING.remove(correlation, future);
	}

	/** The pending future for the correlation, or null. */
	static CompletableFuture<String> pending(String correlation) {
		return correlation == null ? null : PENDING.get(correlation);
	}

	static int pendingCount() {
		return PENDING.size();
	}
}