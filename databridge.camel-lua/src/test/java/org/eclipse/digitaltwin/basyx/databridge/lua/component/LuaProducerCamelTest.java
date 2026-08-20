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
package org.eclipse.digitaltwin.basyx.databridge.lua.component;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.camel.CamelExecutionException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
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

		template.sendBody("direct:start", "{\"a\": 5, \"b\": 2}");
		String result = getMockEndpoint("mock:result").getExchanges().get(0).getMessage().getBody(String.class);

		assertEquals("{\"sum\":7.0,\"product\":10.0}", result);
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

		template.sendBody("direct:start", "{\"State\": \"EXECUTE\", \"ProcessQueue\": []}");
		String result = getMockEndpoint("mock:result").getExchanges().get(0).getMessage().getBody(String.class);

		assertEquals("{\"State\":\"EXECUTE\"}", result);
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
}
