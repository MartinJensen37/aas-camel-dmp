/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
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

package org.eclipse.digitaltwin.basyx.dmp.executable.regression;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.digitaltwin.basyx.dmp.component.RoutesConfigurationLoader;

/** Provides the test environment variables used by the ActiveMQ config tests. */
public class RoutesConfigurationTestEnvironmentVariables {
	public static Map<String, String> get() {
		Map<String, String> variables = new HashMap<>();
		putRoutes(variables);
		putAasServer(variables);
		putLuaTransformers(variables);
		putLuaTransformerFiles(variables);
		putActiveMQ(variables);
		return variables;
	}

	private static void putLuaTransformerFiles(Map<String, String> variables) {
		variables.put(RoutesConfigurationLoader.LUA_ENV_VAR_NAME, "[\"luaA.lua\", \"luaB.lua\"]");
	}

	private static void putActiveMQ(Map<String, String> variables) {
		variables.put("activemqdatasource_json", """
				[
				  {"uniqueId":"property1","serverUrl":"127.0.0.1","serverPort":61616,"queue":"first-topic"},
				  {"uniqueId":"property2","serverUrl":"127.0.0.1","serverPort":9092,"queue":"second-topic"}
				]
				""");
	}

	private static void putLuaTransformers(Map<String, String> variables) {
		variables.put("luatransformer.json", """
				[
				  {"uniqueId":"luaA","queryPath":"luaA.lua","sourceKeys":"property1"},
				  {"uniqueId":"luaB","queryPath":"luaB.lua","sourceKeys":"property2"}
				]
				""");
		variables.put("luaA.lua", "function aimc_main(sources) return { Value = sources.property1 } end");
		variables.put("luaB.lua", "function aimc_main(sources) return { Value = sources.property2 } end");
	}

	private static void putAasServer(Map<String, String> variables) {
		variables.put("aasdatasink.json", """
				[
				  {"uniqueId":"ConnectedSubmodel/ConnectedPropertyA","submodelEndpoint":"http://localhost:4001/shells/TestUpdatedDeviceAAS/aas/submodels/ConnectedSubmodel/submodel","idShortPath":"ConnectedPropertyA"},
				  {"uniqueId":"ConnectedSubmodel/ConnectedPropertyB","submodelEndpoint":"http://localhost:4001/shells/TestUpdatedDeviceAAS/aas/submodels/ConnectedSubmodel/submodel","idShortPath":"ConnectedPropertyB"}
				]
				""");
	}

	private static void putRoutes(Map<String, String> variables) {
		variables.put("routes.json", """
				[
				  {"datasources":["property1"],"transformers":["luaA"],"datasinks":["ConnectedSubmodel/ConnectedPropertyA"],"trigger":"execution-policy"},
				  {"datasources":["property2"],"transformers":["luaB"],"datasinks":["ConnectedSubmodel/ConnectedPropertyB"],"trigger":"execution-policy"}
				]
				""");
	}
}
