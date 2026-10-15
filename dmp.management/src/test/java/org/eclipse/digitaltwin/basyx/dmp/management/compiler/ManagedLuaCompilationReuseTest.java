/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.compiler;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.Test;

class ManagedLuaCompilationReuseTest {
	@Test void independentConcurrentCompilersReuseTheSameImmutableSource() throws Exception {
		var compile = ConnectorConfigurationFactory.class.getDeclaredMethod("compiledLua", byte[].class, String.class);
		compile.setAccessible(true);
		var workers = java.util.concurrent.Executors.newFixedThreadPool(4);
		try {
			byte[] script = "function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8);
			var requests = new java.util.ArrayList<java.util.concurrent.Future<Object>>();
			for (int i = 0; i < 16; i++) requests.add(workers.submit(() -> compile.invoke(null, script, "concurrent-compile-proof")));
			Object first = requests.get(0).get(3, java.util.concurrent.TimeUnit.SECONDS);
			for (var request : requests) assertSame(first, request.get(3, java.util.concurrent.TimeUnit.SECONDS));
		} finally { workers.shutdownNow(); assertTrue(workers.awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS)); }
	}

	@Test @SuppressWarnings("unchecked") void capacityRejectsNewSourcesWithoutEvictingLiveCompiledSources() throws Exception {
		var field = ConnectorConfigurationFactory.class.getDeclaredField("LUA_SOURCES");
		field.setAccessible(true);
		Map<Object, Object> cache = (Map<Object, Object>) field.get(null);
		var capacityField = ConnectorConfigurationFactory.class.getDeclaredField("MAX_CACHED_LUA_SOURCES");
		capacityField.setAccessible(true);
		int capacity = capacityField.getInt(null);
		var identity = Class.forName(ConnectorConfigurationFactory.class.getName() + "$ScriptIdentity").getDeclaredConstructor(String.class, String.class);
		identity.setAccessible(true);
		var compile = ConnectorConfigurationFactory.class.getDeclaredMethod("compiledLua", byte[].class, String.class);
		compile.setAccessible(true);
		byte[] bytes = "function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8);
		LuaCompiledSource live = LuaCompiledSource.fromBytes(bytes, "capacity-proof", 1024);
		Object retained = CompletableFuture.completedFuture(new WeakReference<>(live));
		synchronized (cache) {
			Map<Object, Object> original = Map.copyOf(cache);
			try {
				cache.clear();
				Object same = compile.invoke(null, bytes, "capacity-proof");
				for (int i = 1; i < capacity; i++) cache.put(identity.newInstance("capacity-" + i, "seed"), retained);
				assertSame(same, compile.invoke(null, bytes, "capacity-proof"), "live resource reuse wins even when capacity is full");
				var failure = assertThrows(java.lang.reflect.InvocationTargetException.class,
						() -> compile.invoke(null, bytes, "new-capacity-proof"));
				assertInstanceOf(IllegalStateException.class, failure.getCause());
				assertEquals("managed Lua compilation capacity exceeded", failure.getCause().getMessage());
				assertEquals(capacity, cache.size());
				assertSame(same, compile.invoke(null, bytes, "capacity-proof"), "a rejected compile cannot evict installed prototypes");
			} finally { java.lang.ref.Reference.reachabilityFence(live); cache.clear(); cache.putAll(original); }
		}
	}
}
