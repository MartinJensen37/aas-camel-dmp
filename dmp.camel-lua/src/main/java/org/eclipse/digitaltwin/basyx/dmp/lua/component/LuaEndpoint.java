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
package org.eclipse.digitaltwin.basyx.dmp.lua.component;

import org.apache.camel.Category;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;

/**
 * Endpoint for the {@code lua:} scheme (producer-only transformation).
 *
 * <p>
 * URI syntax: {@code lua:file:<path>?timeoutSeconds=2&contentCache=true
 * &maxScriptBytes=1048576&sourceKeys=body}
 *
 * @author aas-camel-dmp
 */
@UriEndpoint(firstVersion = "1.0.0-SNAPSHOT", scheme = "lua", title = "Lua",
		syntax = "lua:resourcePath", producerOnly = true, category = { Category.SCRIPT })
public class LuaEndpoint extends DefaultEndpoint {

	@UriPath(description = "Path to the Lua blob, optionally prefixed with file:")
	@Metadata(required = true)
	private String resourcePath;

	@UriParam(defaultValue = "2", description = "CPU budget per invocation in seconds")
	private double timeoutSeconds = 2;

	@UriParam(defaultValue = "true", description = "Cache the script and reload on mtime/sha change")
	private boolean contentCache = true;

	@UriParam(defaultValue = "1048576", description = "Max blob size in bytes")
	private long maxScriptBytes = 1048576;

	@UriParam(defaultValue = "body", description = "How to assemble the sources map: 'body' wraps the whole payload, or a comma-separated list of keys to extract from the payload object")
	private String sourceKeys = "body";

	@UriParam(description = "Source-side fine resolution / disassembly (ADR-014 / SPEC §6.5): a JSON Pointer (RFC 6901, e.g. $.State) applied to the incoming canonical payload before it is placed under a SourceId. When set, only the extracted leaf is delivered under SourceId (fine); when absent, the whole payload is delivered (coarse). Applies alongside a single sourceKeys entry.")
	private String valuePath;

	public LuaEndpoint() {
	}

	public LuaEndpoint(String endpointUri, LuaComponent component) {
		super(endpointUri, component);
	}

	@Override
	public Producer createProducer() throws Exception {
		return new LuaProducer(this);
	}

	@Override
	public Consumer createConsumer(Processor processor) throws Exception {
		throw new UnsupportedOperationException("The lua component is producer-only (transformation step).");
	}

	/** Returns the script path (strips a leading {@code file:} prefix). */
	public String getScriptPath() {
		String path = getResourcePath();
		if (path != null && path.startsWith("file:")) {
			return path.substring("file:".length());
		}
		return path;
	}

	public String getResourcePath() {
		return resourcePath;
	}

	public void setResourcePath(String resourcePath) {
		this.resourcePath = resourcePath;
	}

	public double getTimeoutSeconds() {
		return timeoutSeconds;
	}

	public void setTimeoutSeconds(double timeoutSeconds) {
		this.timeoutSeconds = timeoutSeconds;
	}

	public boolean isContentCache() {
		return contentCache;
	}

	public void setContentCache(boolean contentCache) {
		this.contentCache = contentCache;
	}

	public long getMaxScriptBytes() {
		return maxScriptBytes;
	}

	public void setMaxScriptBytes(long maxScriptBytes) {
		this.maxScriptBytes = maxScriptBytes;
	}

	public String getSourceKeys() {
		return sourceKeys;
	}

	public void setSourceKeys(String sourceKeys) {
		this.sourceKeys = sourceKeys;
	}

	/**
	 * The JSON Pointer (SPEC §6.5.1) used to disassemble the incoming payload
	 * before it is placed under a {@code SourceId}; absent → coarse (whole
	 * payload delivered, SPEC §8.3).
	 */
	public String getValuePath() {
		return valuePath;
	}

	public void setValuePath(String valuePath) {
		this.valuePath = valuePath;
	}
}
