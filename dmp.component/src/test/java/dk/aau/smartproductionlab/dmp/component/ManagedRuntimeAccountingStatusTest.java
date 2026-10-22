/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import dk.aau.smartproductionlab.dmp.core.component.DmpComponent;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RouteConfiguration;
import dk.aau.smartproductionlab.dmp.core.configuration.route.core.RoutesConfiguration;
import dk.aau.smartproductionlab.dmp.lua.configuration.LuaTransformerConfiguration;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaCompiledSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ManagedRuntimeAccountingStatusTest {
	@AfterEach void reset() { ManagedRuntimeStatus.reset(); }
	private static final class Runtime extends DmpComponent implements AutoCloseable {
		Runtime() throws Exception { super(new RoutesConfiguration()); camelContext.start(); }
		@Override public void close() { stopComponent(); }
	}
	private static RoutesConfiguration configuration(DataTransformerConfiguration transformer) {
		var route = new RouteConfiguration("execution-policy", List.of(), List.of("transform"), List.of());
		route.setRouteId("private-mapping");
		return new RoutesConfiguration(Map.of(), Map.of("transform", transformer), Map.of(), List.of(route));
	}

	@Test
	void statusProjectsSafeOwnerTokensAndExactPayloadBytesNotHeapEstimates() throws Exception {
		byte[] bytes = "function aimc_main(sources) return sources end".getBytes(StandardCharsets.UTF_8);
		try (var runtime = new Runtime()) {
			ManagedRuntimeStatus.beginManaged();
			ManagedRuntimeStatus.trackReconciliation(null, runtime);
			var bundle = runtime.installBundle("private-bundle", configuration(new LuaTransformerConfiguration("private-lua",
					LuaCompiledSource.fromBytes(bytes, "private-script", 256))));
			var status = ManagedRuntimeStatus.asMap();
			var owners = (List<?>) status.get("resourceOwners");
			assertEquals(1, owners.size());
			var owner = (Map<?, ?>) owners.get(0);
			assertEquals(ManagedRuntimeStatus.token("private-bundle"), owner.get("bundleToken"));
			assertEquals(bundle.generation(), owner.get("generation"));
			assertEquals(true, owner.get("active"));
			var resources = (Map<?, ?>) owner.get("resources");
			assertEquals((long) bytes.length, resources.get("exclusiveLuaSourceRetainedBytes"));
			assertFalse(resources.containsKey("heapUsedBytes"));
			assertTrue(((Map<?, ?>) status.get("process")).containsKey("heapUsedBytes"));
			assertFalse(status.toString().contains("private-"));
			runtime.removeBundle(bundle);
			assertEquals(List.of(), ManagedRuntimeStatus.asMap().get("resourceOwners"));
		}
	}

	private static final class BlockingClose extends DataTransformerConfiguration implements ManagedRouteResource {
		final CountDownLatch closing = new CountDownLatch(1), release = new CountDownLatch(1);
		@Override public String getConnectionURI() { return "direct:private-transform"; }
		@Override public void bind(CamelContext context) { }
		@Override public synchronized void close() throws Exception {
			closing.countDown();
			if (!release.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("test close deadline");
		}
	}

	@Test
	void ownerStatusDoesNotWaitForTheLifecycleOrResourceCloseMonitor() throws Exception {
		var resource = new BlockingClose();
		var workers = java.util.concurrent.Executors.newFixedThreadPool(2);
		try (var runtime = new Runtime()) {
			ManagedRuntimeStatus.beginManaged();
			ManagedRuntimeStatus.trackReconciliation(null, runtime);
			var bundle = runtime.installBundle("private-bundle", configuration(resource));
			var removal = workers.submit(() -> runtime.removeBundle(bundle));
			try {
				assertTrue(resource.closing.await(2, TimeUnit.SECONDS));
				var status = workers.submit(ManagedRuntimeStatus::asMap).get(500, TimeUnit.MILLISECONDS);
				var owner = (Map<?, ?>) ((List<?>) status.get("resourceOwners")).get(0);
				assertEquals(false, owner.get("active"));
				assertFalse(status.toString().contains("private-"));
				assertFalse(removal.isDone());
			} finally { resource.release.countDown(); }
			removal.get(2, TimeUnit.SECONDS);
		} finally { resource.release.countDown(); workers.shutdownNow(); assertTrue(workers.awaitTermination(3, TimeUnit.SECONDS)); }
	}
}
