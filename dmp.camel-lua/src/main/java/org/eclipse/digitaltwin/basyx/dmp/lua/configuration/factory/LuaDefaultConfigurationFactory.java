/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.lua.configuration.factory;

import java.util.List;
import java.util.Set;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.apache.commons.io.FilenameUtils;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.factory.DataTransformerConfigurationFactory;
import org.eclipse.digitaltwin.basyx.dmp.lua.configuration.LuaTransformerConfiguration;

/**
 * Lua default configuration factory from the default path.
 *
 * <p>
 * The default file is {@code luatransformer.json}; each entry references a Lua
 * blob via {@code queryPath} (AIMC transformation, only source of truth).
 *
 * @author aas-camel-dmp
 */
public class LuaDefaultConfigurationFactory extends DataTransformerConfigurationFactory {
	public static final String DEFAULT_FILE_PATH = "luatransformer.json";
	private static final Set<String> ALLOWED_FIELDS = Set.of("uniqueId", "queryPath", "timeoutSeconds",
			"contentCache", "maxScriptBytes", "sourceKeys", "valuePath");

	private String filePath;
	private final ClassLoader classLoader;

	public LuaDefaultConfigurationFactory(ClassLoader loader) {
		super(DEFAULT_FILE_PATH, loader, LuaTransformerConfiguration.class);
		this.classLoader = loader;
	}

	public LuaDefaultConfigurationFactory(String filePath, ClassLoader loader) {
		super(filePath, loader, LuaTransformerConfiguration.class);
		this.filePath = filePath;
		this.classLoader = loader;
	}

	@Override
	public List<DataTransformerConfiguration> create() {
		validateLuaConfigurations();
		List<DataTransformerConfiguration> configs = super.create();

		configs.stream().forEach(this::setQueryPath);

		return configs;
	}

	private void validateLuaConfigurations() {
		com.google.gson.JsonElement root = getConfigurationLoader().loadJsonElement();
		if (root == null || !root.isJsonArray()) {
			throw new IllegalArgumentException("Lua transformer configuration must be a JSON array");
		}
		for (int index = 0; index < root.getAsJsonArray().size(); index++) {
			com.google.gson.JsonElement element = root.getAsJsonArray().get(index);
			if (!element.isJsonObject()) {
				throw new IllegalArgumentException("Lua transformer entry " + index + " must be an object");
			}
			for (String field : element.getAsJsonObject().keySet()) {
				if (!ALLOWED_FIELDS.contains(field)) {
					throw new IllegalArgumentException("Unsupported Lua transformer field '" + field
							+ "' in entry " + index + "; transform modes and non-Lua engines are not supported");
				}
			}
		}
	}

	private void setQueryPath(DataTransformerConfiguration config) {
		if (isConfiguredFromDefaultPath()) {
			LuaTransformerConfiguration lua = (LuaTransformerConfiguration) config;
			lua.setQueryPath(resolveClasspathOrFile(lua.getQueryPath()));
			return;
		}

		String queryPath = ((LuaTransformerConfiguration) config).getQueryPath();

		((LuaTransformerConfiguration) config).setQueryPath(FilenameUtils.getFullPath(filePath) + queryPath);
	}

	private String resolveClasspathOrFile(String queryPath) {
		String path = queryPath.startsWith("classpath:") ? queryPath.substring("classpath:".length()) : queryPath;
		Path file = Path.of(path);
		if (Files.isRegularFile(file)) return file.toAbsolutePath().toString();
		URL resource = classLoader.getResource(path.startsWith("/") ? path.substring(1) : path);
		if (resource == null) throw new IllegalArgumentException("Lua blob resource not found: " + queryPath);
		if ("file".equalsIgnoreCase(resource.getProtocol())) {
			try {
				return Path.of(resource.toURI()).toString();
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("Invalid Lua blob resource URI: " + resource, e);
			}
		}
		try (var input = resource.openStream()) {
			Path materialized = Files.createTempFile("dmp-lua-", ".lua");
			Files.copy(input, materialized, StandardCopyOption.REPLACE_EXISTING);
			materialized.toFile().deleteOnExit();
			return materialized.toString();
		} catch (IOException e) {
			throw new IllegalStateException("Could not materialize Lua blob resource " + queryPath, e);
		}
	}

	private boolean isConfiguredFromDefaultPath() {
		return filePath == null || filePath.isEmpty();
	}
}
