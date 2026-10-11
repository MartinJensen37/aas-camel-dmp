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
package org.eclipse.digitaltwin.basyx.dmp.lua.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.factory.LuaDefaultConfigurationFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the DMP config integration: {@code luatransformer.json} is parsed
 * by {@link LuaDefaultConfigurationFactory} into
 * {@link LuaTransformerConfiguration} entries whose
 * {@link LuaTransformerConfiguration#getConnectionURI()} yields a valid
 * {@code lua:file:...} endpoint (SPEC §8.5).
 */
class LuaDefaultConfigurationFactoryTest {

	@TempDir
	Path tempDir;

	@Test
	void parsesLuatransformersJsonFromClasspath() {
		ClassLoader loader = getClass().getClassLoader();
		LuaDefaultConfigurationFactory factory = new LuaDefaultConfigurationFactory(loader);

		List<DataTransformerConfiguration> configs = factory.create();

		assertEquals(1, configs.size());

		LuaTransformerConfiguration config = (LuaTransformerConfiguration) configs.get(0);
		assertEquals("syntegonStopperingSystemAAS_PackMLState_State_transformer", config.getUniqueId());
		assertTrue(config.getQueryPath().endsWith("queries/syntegonStopperingSystemAAS_PackMLState_State.lua"));

		String uri = config.getConnectionURI();
		assertTrue(uri.startsWith("lua:file:"),
				"unexpected URI: " + uri);
		assertTrue(uri.contains("queries/syntegonStopperingSystemAAS_PackMLState_State.lua?"),
				"unexpected resolved query path: " + uri);
		assertFalse(uri.contains("inputType"), "transform modes are not part of the URI: " + uri);
		assertFalse(uri.contains("outputType"), "transform modes are not part of the URI: " + uri);
		assertTrue(uri.contains("timeoutSeconds=2"), "missing default timeout: " + uri);
		assertTrue(uri.contains("contentCache=true"), "missing default contentCache: " + uri);
		assertTrue(uri.contains("maxScriptBytes=1048576"), "missing default maxScriptBytes: " + uri);
		assertTrue(uri.contains("sourceKeys=body"), "missing default sourceKeys: " + uri);
	}

	@Test
	void resolvesQueryPathAndEmitsFileUriForExistingBlob() throws Exception {
		// simulate the DMP config dir: luatransformer.json + a real Lua blob
		Path queries = Files.createDirectories(tempDir.resolve("queries"));
		Files.writeString(queries.resolve("syntegonStopperingSystemAAS_PackMLState_State.lua"),
				"function aimc_main(sources) return {} end", StandardCharsets.UTF_8);

		Path configFile = tempDir.resolve("luatransformer.json");
		Files.writeString(configFile, """
				[
				  {
				    "uniqueId": "syntegonStopperingSystemAAS_PackMLState_State_transformer",
				    "queryPath": "queries/syntegonStopperingSystemAAS_PackMLState_State.lua"
				  }
				]
				""", StandardCharsets.UTF_8);

		LuaDefaultConfigurationFactory factory = new LuaDefaultConfigurationFactory(configFile.toString(),
				getClass().getClassLoader());

		List<DataTransformerConfiguration> configs = factory.create();

		LuaTransformerConfiguration config = (LuaTransformerConfiguration) configs.get(0);
		assertEquals(queries.resolve("syntegonStopperingSystemAAS_PackMLState_State.lua").toString(),
				config.getQueryPath());

		String uri = config.getConnectionURI();
		assertTrue(uri.startsWith("lua:file:" + queries.resolve("syntegonStopperingSystemAAS_PackMLState_State.lua")),
				"unexpected URI: " + uri);
		assertFalse(uri.contains("inputType"));
		assertFalse(uri.contains("outputType"));
	}

	@Test
	void rejectsRetiredLuaTransformModes() throws Exception {
		Path configFile = tempDir.resolve("luatransformer.json");
		Files.writeString(configFile, """
				[{"uniqueId":"stale","queryPath":"old.lua","inputType":"JsonString"}]
				""", StandardCharsets.UTF_8);
		LuaDefaultConfigurationFactory factory = new LuaDefaultConfigurationFactory(configFile.toString(),
				getClass().getClassLoader());
		IllegalArgumentException error = assertThrows(IllegalArgumentException.class, factory::create);
		assertTrue(error.getMessage().contains("inputType"));
	}
}
