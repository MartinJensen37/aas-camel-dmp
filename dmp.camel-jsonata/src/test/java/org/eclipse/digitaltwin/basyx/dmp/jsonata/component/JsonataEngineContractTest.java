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
package org.eclipse.digitaltwin.basyx.dmp.jsonata.component;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Contract test for the JSONata transformation engine (ADR-015 / SPEC §7.6,
 * §8.4 #11): the JSONata expression is evaluated over the same
 * SourceId-keyed {@code sources} shape the Lua engine receives (the aggregated
 * fan-in body), returning a SinkId-keyed result.
 */
class JsonataEngineContractTest extends CamelTestSupport {

	@TempDir
	Path tempDir;

	@Test
	void jsonataEvaluatesSourceIdKeyedSources() throws Exception {
		Path expression = tempDir.resolve("combine.jsonata");
		Files.writeString(expression, "{\"combined\": station_state + station_mode, \"mode\": station_mode}",
				StandardCharsets.UTF_8);

		context.getRouteDefinitions().clear();
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start")
						.to("jsonata:file:" + expression + "?inputType=JsonString&outputType=JsonString")
						.to("mock:result");
			}
		});
		context.start();

		// The body is the aggregated `sources` map (keyed by SourceId) produced by
		// the fan-in aggregator / datasource pipeline.
		template.sendBody("direct:start", "{\"station_state\": 3, \"station_mode\": 4}");
		String result = getMockEndpoint("mock:result").getExchanges().get(0).getMessage().getBody(String.class);

		assertEquals("{\"combined\":7,\"mode\":4}", result);
	}
}
