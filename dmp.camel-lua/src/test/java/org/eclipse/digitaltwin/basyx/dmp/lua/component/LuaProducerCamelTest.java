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

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
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

	@Test
	void transformBodyWithLuaBlob() throws Exception {
		String script = writeBlob("simple.lua");
		String uri = "lua:file:" + script + "?sourceKeys=a,b&timeoutSeconds=2";

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
		String uri = "lua:file:" + blob + "?sourceKeys=StationState&timeoutSeconds=2";

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
		String uri = "lua:file:" + script;

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
	void retiredJsonStringEndpointOptionIsRejected() throws Exception {
		String script = writeBlob("simple.lua");
		String uri = "lua:file:" + script + "?inputType=JsonString";
		context.getRouteDefinitions().clear();
		assertThrows(Exception.class, () -> context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:start").to(uri);
			}
		}));
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
		String uri = "lua:file:" + blob + "?sourceKeys=StationState&valuePath=$.State&timeoutSeconds=2";

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
		String uri = "lua:file:" + blob + "?sourceKeys=StationState&timeoutSeconds=2";

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
		String uri = "lua:file:" + blob + "?sourceKeys=StationState&valuePath=$.Missing&timeoutSeconds=2";

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
		addRoute("lua:file:" + blob);
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
		addRoute("lua:file:" + blob);
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
		addRoute("lua:file:" + blob);
		context.start();
		template.sendBody("direct:start", "103.5585973");
		Map<?, ?> result = getMockEndpoint("mock:result").getExchanges().get(0)
				.getMessage().getBody(Map.class);
		assertEquals("103.5585973", result.get("text"));
		assertTrue(result.get("text") instanceof String);
		assertTrue(result.get("number") instanceof Number);
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
