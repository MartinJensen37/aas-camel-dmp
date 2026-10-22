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
package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The pending-reply registry shared by the {@code paho-mqtt5-inout:}
 * producers (register + await) and the {@code paho-mqtt5:} reply-consumer
 * routes' {@link MqttV5ReplyMatcher}s (complete). Each endpoint in a context
 * owns a registry. Wire reservations span registries because identical data
 * on the same broker/reply channel cannot distinguish their callers.
 *
 * @author aas-camel-dmp
 */
final class MqttV5ReplyRegistry {

	/** Scheduling margin beyond the endpoint's replyTimeout. */
	static final long WAIT_MARGIN_MILLIS = 1_000L;

	/** Authored IDs cannot prove generations indefinitely; reject reuse during this fence. */
	static final long FENCE_MILLIS = 60_000L;
	private static final WireState WIRE = new WireState(System::nanoTime, 8_192, FENCE_MILLIS);
	private final WireState wire;

	MqttV5ReplyRegistry() {
		this(WIRE);
	}

	MqttV5ReplyRegistry(WireState wire) {
		this.wire = wire;
	}

	CompletableFuture<String> register(Object owner, String broker, String topic, String correlation) {
		synchronized (wire) {
			wire.expire();
			Key key = new Key(broker, topic, correlation);
			if (wire.pending.containsKey(key) || wire.fences.containsKey(key)) {
				throw new IllegalStateException("MQTT correlation is already pending or fenced on the reply channel");
			}
			// Reserve room for the eventual fence; never evict an unexpired fence.
			if (wire.pending.size() + wire.fences.size() >= wire.capacity) {
				throw new IllegalStateException("MQTT reply reservation capacity exhausted");
			}
			CompletableFuture<String> future = new CompletableFuture<>();
			wire.pending.put(key, new Pending(this, owner, future));
			return future;
		}
	}

	void unregister(Object owner, String broker, String topic, String correlation,
			CompletableFuture<String> future) {
		synchronized (wire) {
			Key key = new Key(broker, topic, correlation);
			Pending pending = wire.pending.get(key);
			if (pending != null && pending.registry == this && pending.owner == owner && pending.future == future) {
				wire.retire(key);
			}
		}
	}

	boolean complete(String broker, String topic, String correlation, String payload) {
		synchronized (wire) {
			Key key = new Key(broker, topic, correlation);
			Pending pending = wire.pending.get(key);
			if (pending == null || pending.registry != this) return false;
			wire.retire(key);
			return pending.future.complete(payload);
		}
	}

	void cancelOwner(Object owner) {
		synchronized (wire) {
			for (Key key : java.util.List.copyOf(wire.pending.keySet())) {
				Pending pending = wire.pending.get(key);
				if (pending.registry == this && pending.owner == owner) {
					wire.retire(key);
					pending.future.cancel(false);
				}
			}
		}
	}

	int pendingCount() {
		synchronized (wire) {
			return (int) wire.pending.values().stream().filter(p -> p.registry == this).count();
		}
	}

	private record Key(String broker, String topic, String correlation) { }
	private record Pending(MqttV5ReplyRegistry registry, Object owner, CompletableFuture<String> future) { }

	/** Bounded, monotonic-time wire fences. Expired entries are pruned on access. */
	static final class WireState {
		private final Map<Key, Pending> pending = new HashMap<>();
		private final Map<Key, Long> fences = new HashMap<>();
		private final LongSupplier clock;
		private final int capacity;
		private final long fenceNanos;

		WireState(LongSupplier clock, int capacity, long fenceMillis) {
			this.clock = clock;
			this.capacity = capacity;
			this.fenceNanos = TimeUnit.MILLISECONDS.toNanos(fenceMillis);
		}

		private void expire() {
			long now = clock.getAsLong();
			fences.values().removeIf(deadline -> now - deadline >= 0);
		}

		private void retire(Key key) {
			expire();
			pending.remove(key);
			fences.put(key, clock.getAsLong() + fenceNanos);
		}
	}
}
