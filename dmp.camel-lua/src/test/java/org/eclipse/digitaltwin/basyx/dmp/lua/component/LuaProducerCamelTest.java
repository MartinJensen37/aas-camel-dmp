/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.lua.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.LuaTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaCompiledSource;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end test of the {@code lua:} component inside a Camel route.
 */
class LuaProducerCamelTest extends CamelTestSupport {

	@TempDir
	Path tempDir;

	private String writeBlob(String name) throws Exception {
		String content = new String(
				getClass().getResourceAsStream("/blobs/" + name).readAllBytes(),
				StandardCharsets.UTF_8);
		Path file = tempDir.resolve(name);
		Files.writeString(file, content);
		return file.toString();
	}

	/** Compiles the blob text in memory and binds it like the runtime does; Lua never loads from files. */
	private String managedUri(Object blob) throws Exception {
		String name = "test-lua-" + java.util.UUID.randomUUID();
		byte[] bytes = Files.readAllBytes(Path.of(blob.toString()));
		context.getRegistry().bind(name, LuaCompiledSource.fromBytes(bytes, Path.of(blob.toString()).getFileName().toString(), 1 << 20));
		return "lua:managed:" + name;
	}

	@Test
	void transformBodyWithLuaBlob() throws Exception {
		String script = writeBlob("simple.lua");
		String uri = managedUri(script) + "?sourceKeys=a,b&timeoutSeconds=2";

		context.getRouteDefinitions().clear();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start").to(uri).to("mock:result");
			}
		});
		context.start();

		template.sendBody("direct:start", Map.of("a", 5, "b", 2));
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);

		assertEquals(7, ((Number) result.get("sum")).intValue());
		assertEquals(10, ((Number) result.get("product")).intValue());
	}

	@Test
	void singleSourceKeyWrapsWholePayload() throws Exception {
		// AIMC contract: sources[SourceId] = the whole payload of that source.
		// A single sourceKeys value must key the whole payload (not extract a
		// field from it, as the comma-list form does).
		Path blob = tempDir.resolve("stationstate.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    local st = sources.StationState or {}
				    return { State = st.State }
				end
				""", StandardCharsets.UTF_8);
		String uri = managedUri(blob) + "?sourceKeys=StationState&timeoutSeconds=2";

		context.getRouteDefinitions().clear();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start").to(uri).to("mock:result");
			}
		});
		context.start();

		template.sendBody("direct:start", Map.of("State", "EXECUTE", "ProcessQueue", java.util.List.of()));
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);

		assertEquals("EXECUTE", result.get("State"));
	}

	@Test
	void malformedBlobFailsRouteWithClearError() throws Exception {
		String script = writeBlob("nonTable.lua");
		String uri = managedUri(script);

		context.getRouteDefinitions().clear();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start").to(uri).to("mock:result");
			}
		});
		context.start();

		CamelExecutionException ex = assertThrows(CamelExecutionException.class,
				() -> template.sendBody("direct:start", "{}"));
		Throwable cause = ex.getCause();
		assertEquals(true, cause.getMessage().contains("must return a table"));
	}

	@Test
	void fineSourceValuePathDisassemblesPayloadToLeaf() throws Exception {
		// SPEC §8.4 #8 (fine source): valuePath=$.State auto-extracts the nested
		// property into sources[SourceId] at the DMP boundary — the blob sees
		// the leaf value, no in-blob traversal needed (it must NOT see the full
		// payload object).
		Path blob = tempDir.resolve("fine.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    local st = sources.StationState
				    if type(st) == "table" then
				        return { error = "expected leaf, got object" }
				    end
				    return { State = st }
				end
				""", StandardCharsets.UTF_8);
		String uri = managedUri(blob) + "?sourceKeys=StationState&valuePath=$.State&timeoutSeconds=2";

		context.getRouteDefinitions().clear();
		addRoute(uri);
		context.start();

		template.sendBody("direct:start", Map.of("State", "EXECUTE",
				"ProcessQueue", Map.of("mode", "AUTO")));
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);

		assertEquals("EXECUTE", result.get("State"));
	}

	@Test
	void coarseSourceDeliversWholePayloadToBlob() throws Exception {
		// SPEC §8.4 #7 (coarse source): no valuePath → sources[SourceId] is the
		// whole decoded payload; nested access happens in-blob.
		Path blob = tempDir.resolve("coarse.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    local st = sources.StationState or {}
				    local state = st.State  -- in-blob drill-down (coarse)
				    local mode  = (st.ProcessQueue or {}).mode
				    return { State = state, Mode = mode }
				end
				""", StandardCharsets.UTF_8);
		String uri = managedUri(blob) + "?sourceKeys=StationState&timeoutSeconds=2";

		context.getRouteDefinitions().clear();
		addRoute(uri);
		context.start();

		template.sendBody("direct:start", Map.of("State", "EXECUTE",
				"ProcessQueue", Map.of("mode", "AUTO")));
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);

		assertEquals("EXECUTE", result.get("State"));
		assertEquals("AUTO", result.get("Mode"));
	}

	@Test
	void fineSourceMissingValuePathFailsClearly() throws Exception {
		// SPEC §8.4 #8: pointer on a missing path → clear route error.
		Path blob = tempDir.resolve("fine-missing.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    return { State = sources.StationState or "none" }
				end
				""", StandardCharsets.UTF_8);
		String uri = managedUri(blob) + "?sourceKeys=StationState&valuePath=$.Missing&timeoutSeconds=2";

		context.getRouteDefinitions().clear();
		addRoute(uri);
		context.start();

		CamelExecutionException ex = assertThrows(CamelExecutionException.class,
				() -> template.sendBody("direct:start", Map.of("State", "EXECUTE")));
		assertEquals(true, ex.getCause().getMessage().contains("does not resolve"));
	}

	@Test
	void jsonLookingStringsAndBytesRemainOpaqueValues() throws Exception {
		Path blob = tempDir.resolve("opaque.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    return { value = sources.body, kind = type(sources.body) }
				end
				""", StandardCharsets.UTF_8);
		context.getRouteDefinitions().clear();
		addRoute(managedUri(blob));
		context.start();

		String opaqueText = "{\"looks\":\"like JSON\"}";
		template.sendBody("direct:start", opaqueText);
		Map<?, ?> textResult = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);
		assertEquals("string", textResult.get("kind"));
		assertEquals(opaqueText, textResult.get("value"));

		getMockEndpoint("mock:result").reset();
		byte[] bytes = new byte[] { 0, 1, 2, (byte) 0xff };
		template.sendBody("direct:start", bytes);
		Map<?, ?> byteResult = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);
		assertArrayEquals(bytes, (byte[]) byteResult.get("value"));
		assertEquals("userdata", byteResult.get("kind"));
	}

	@Test
	void explicitJsonNullRemainsAValueAndIsNotConfusedWithAbsence() throws Exception {
		Path blob = tempDir.resolve("json-null.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    local value = sources.body["nullable"]
				    return { present = (value ~= nil), isNull = (value == json.null), encoded = json.encode(sources.body) }
				end
				""", StandardCharsets.UTF_8);
		context.getRouteDefinitions().clear();
		addRoute(managedUri(blob));
		context.start();

		Map<String, Object> value = new java.util.LinkedHashMap<>();
		value.put("nullable", JsonPointer.JSON_NULL);
		template.sendBody("direct:start", value);
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);
		assertEquals(true, result.get("present"));
		assertEquals(true, result.get("isNull"));
		assertEquals("{\"nullable\":null}", result.get("encoded"));
	}

	@Test
	void numericLookingLuaStringsRemainStrings() throws Exception {
		Path blob = tempDir.resolve("numeric-string.lua");
		Files.writeString(blob, """
				function aimc_main(sources)
				    return { text = sources.body, number = tonumber(sources.body) }
				end
				""", StandardCharsets.UTF_8);
		context.getRouteDefinitions().clear();
		addRoute(managedUri(blob));
		context.start();
		template.sendBody("direct:start", "103.5585973");
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);
		assertEquals("103.5585973", result.get("text"));
		assertTrue(result.get("text") instanceof String);
		assertTrue(result.get("number") instanceof Number);
	}

	@Test
	void managedSourceRunsFromMemoryWithoutCreatingFiles() throws Exception {
		byte[] bytes = "function aimc_main(sources) return { answer = 42 } end".getBytes(StandardCharsets.UTF_8);
		LuaCompiledSource source = LuaCompiledSource.fromBytes(bytes, "mapping:managed-memory", 128);
		bytes[0] = 'x';
		assertEquals("function aimc_main(sources) return { answer = 42 } end",
				new String(source.bytes(), StandardCharsets.UTF_8));

		LuaTransformerConfiguration configuration = new LuaTransformerConfiguration("managed", source);
		configuration.bind(context);
		try {
			String uri = configuration.getConnectionURI();
			assertTrue(uri.startsWith("lua:managed:lua-source-"));
			assertTrue(!uri.contains("function aimc_main"));
			context.getRouteDefinitions().clear();
			addRoute(uri);
			context.start();
			for (int i = 0; i < 3; i++) {
				template.sendBody("direct:start", Map.of("n", i));
				Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(i)
						.getMessage().getBody(Map.class);
				assertEquals(42, ((Number) result.get("answer")).intValue());
			}
			assertEquals(64, source.contentIdentity().length());
			try (var files = Files.list(tempDir)) {
				assertEquals(0, files.count(), "managed scripts must not be materialized as files");
			}
		} finally {
			try {
				context.stop();
			} finally {
				configuration.close();
			}
		}
	}

	@Test
	void managedSourceEagerlyRejectsOversizeAndInvalidSyntax() {
		assertThrows(IllegalArgumentException.class, () -> LuaCompiledSource.fromBytes(
				"1234".getBytes(StandardCharsets.UTF_8), "mapping:too-large", 3));
		assertThrows(RuntimeException.class, () -> LuaCompiledSource.fromBytes(
				"function aimc_main( ".getBytes(StandardCharsets.UTF_8), "mapping:syntax-error", 128));
	}

	@Test
	void concurrentManagedVersionsStayBoundToTheirOwnRoutes() throws Exception {
		LuaTransformerConfiguration oldConfig = managedConfiguration("old", "old-generation");
		LuaTransformerConfiguration newConfig = managedConfiguration("new", "new-generation");
		try {
			oldConfig.bind(context);
			newConfig.bind(context);
			context.getRouteDefinitions().clear();
			context.addRoutes(new RouteBuilder() {
				@Override
				public void configure() {
					from("direct:old").to(oldConfig.getConnectionURI());
					from("direct:new").to(newConfig.getConnectionURI());
				}
			});
			context.start();

			var oldRuns = IntStream.range(0, 20).mapToObj(i -> CompletableFuture.supplyAsync(
					() -> template.requestBody("direct:old", Map.of(), Map.class))).toList();
			var newRuns = IntStream.range(0, 20).mapToObj(i -> CompletableFuture.supplyAsync(
					() -> template.requestBody("direct:new", Map.of(), Map.class))).toList();
			for (var result : oldRuns) {
				assertEquals("old-generation", result.join().get("version"));
			}
			for (var result : newRuns) {
				assertEquals("new-generation", result.join().get("version"));
			}
			assertTrue(!oldConfig.getContentIdentity().equals(newConfig.getContentIdentity()));
		} finally {
			try {
				context.stop();
			} finally {
				try {
					oldConfig.close();
				} finally {
					newConfig.close();
				}
			}
		}
	}

	private LuaTransformerConfiguration managedConfiguration(String id, String version) {
		String script = "function aimc_main(sources) return { version = '" + version + "' } end";
		LuaCompiledSource source = LuaCompiledSource.fromBytes(script.getBytes(StandardCharsets.UTF_8),
				"mapping:" + id, 256);
		return new LuaTransformerConfiguration(id, source);
	}

	private void addRoute(String luaUri) throws Exception {
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start").to(luaUri).to("mock:result");
			}
		});
	}
}
