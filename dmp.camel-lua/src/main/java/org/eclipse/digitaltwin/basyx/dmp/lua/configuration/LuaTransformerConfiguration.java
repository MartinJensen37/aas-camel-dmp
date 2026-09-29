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
package org.eclipse.digitaltwin.basyx.dmp.lua.configuration;

import java.io.File;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataTransformerConfiguration;

/**
 * An implementation of the Lua transformer configuration using the custom
 * {@code lua:} Camel component (databridge.camel-lua).
 *
 * <p>
 * The connection URI follows the AIMC transformation contract:
 * {@code lua:file:<path>?inputType=JsonString&outputType=JsonString&...}.
 * The Lua blob must expose {@code function aimc_main(sources)}.
 *
 * @author aas-camel-dmp
 */
public class LuaTransformerConfiguration extends DataTransformerConfiguration {

	public static final String DEFAULT_INPUT_TYPE = "JsonString";
	public static final String DEFAULT_OUTPUT_TYPE = "JsonString";
	public static final String DEFAULT_TIMEOUT_SECONDS = "2";
	public static final String DEFAULT_CONTENT_CACHE = "true";
	public static final String DEFAULT_MAX_SCRIPT_BYTES = "1048576";
	public static final String DEFAULT_SOURCE_KEYS = "body";

	private String queryPath;
	private String inputType;
	private String outputType;
	private String timeoutSeconds;
	private String contentCache;
	private String maxScriptBytes;
	private String sourceKeys;
	private String valuePath;

	public LuaTransformerConfiguration() {
		this.inputType = DEFAULT_INPUT_TYPE;
		this.outputType = DEFAULT_OUTPUT_TYPE;
		this.timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
		this.contentCache = DEFAULT_CONTENT_CACHE;
		this.maxScriptBytes = DEFAULT_MAX_SCRIPT_BYTES;
		this.sourceKeys = DEFAULT_SOURCE_KEYS;
	}

	public LuaTransformerConfiguration(String uniqueId, String queryPath, String inputType, String outputType) {
		super(uniqueId);
		this.queryPath = queryPath;
		this.inputType = inputType;
		this.outputType = outputType;
		this.timeoutSeconds = DEFAULT_TIMEOUT_SECONDS;
		this.contentCache = DEFAULT_CONTENT_CACHE;
		this.maxScriptBytes = DEFAULT_MAX_SCRIPT_BYTES;
		this.sourceKeys = DEFAULT_SOURCE_KEYS;
	}

	public String getQueryPath() {
		return queryPath;
	}

	public void setQueryPath(String queryPath) {
		this.queryPath = queryPath;
	}

	@Override
	public String getConnectionURI() {
		String path = getQueryPath();
		File luaFile = new File(path);
		String resource = luaFile.exists() ? "file:" + path : path;

		StringBuilder uri = new StringBuilder("lua:" + resource);
		uri.append("?inputType=").append(getInputType());
		uri.append("&outputType=").append(getOutputType());
		uri.append("&timeoutSeconds=").append(getTimeoutSeconds());
		uri.append("&contentCache=").append(getContentCache());
		uri.append("&maxScriptBytes=").append(getMaxScriptBytes());
		uri.append("&sourceKeys=").append(getSourceKeys());
		if (getValuePath() != null && !getValuePath().isBlank()) {
			uri.append("&valuePath=").append(getValuePath());
		}
		return uri.toString();
	}

	public String getInputType() {
		return inputType;
	}

	public void setInputType(String inputType) {
		this.inputType = inputType;
	}

	public String getOutputType() {
		return outputType;
	}

	public void setOutputType(String outputType) {
		this.outputType = outputType;
	}

	public String getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(String timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public String getContentCache() {
		return contentCache;
	}

	public void setContentCache(String contentCache) {
		this.contentCache = contentCache;
	}

	public String getMaxScriptBytes() {
		return maxScriptBytes;
	}

	public void setMaxScriptBytes(String maxScriptBytes) {
		this.maxScriptBytes = maxScriptBytes;
	}

	public String getSourceKeys() {
		return sourceKeys;
	}

	public void setSourceKeys(String sourceKeys) {
		this.sourceKeys = sourceKeys;
	}

	/**
	 * Optional source-side fine resolution (ADR-014 / SPEC §8.3): a JSON
	 * Pointer applied to the incoming canonical payload before it is placed
	 * under a {@code SourceId}. Absent → coarse (whole payload).
	 */
	public String getValuePath() {
		return valuePath;
	}

	public void setValuePath(String valuePath) {
		this.valuePath = valuePath;
	}
}
