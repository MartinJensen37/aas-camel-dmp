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
package org.eclipse.digitaltwin.basyx.databridge.lua.component;

import java.io.IOException;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.digitaltwin.basyx.databridge.lua.sandbox.LuaJsonCodec;
import org.eclipse.digitaltwin.basyx.databridge.lua.sandbox.LuaSandbox;
import org.eclipse.digitaltwin.basyx.databridge.lua.sandbox.LuaScriptLoader;
import org.eclipse.digitaltwin.basyx.databridge.lua.sandbox.LuaTimeoutHook;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Executes the AIMC-compliant Lua blob for each exchange.
 *
 * <p>
 * Per invocation (SPEC §7.5 — LuaJ {@code Globals} are not thread-safe):
 * <ol>
 * <li>load the script (cached, reload on change) from the endpoint path</li>
 * <li>build a fresh sandboxed {@link Globals} with the whitelist</li>
 * <li>install the CPU timeout hook</li>
 * <li>decode the exchange body (host-side JSON, ADR-004)</li>
 * <li>call {@code aimc_main(sources)} where {@code sources} is keyed by
 * {@code SourceId}</li>
 * <li>JSON-encode the returned table (keyed by {@code SinkId}) back to the
 * body</li>
 * </ol>
 *
 * @author aas-camel-dmp
 */
public class LuaProducer extends DefaultProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(LuaProducer.class);

	private final LuaEndpoint endpoint;
	private final ObjectMapper mapper = new ObjectMapper();
	private volatile LuaScriptLoader scriptLoader;

	public LuaProducer(LuaEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		String script;
		try {
			script = getScriptLoader().load(endpoint.getScriptPath());
		} catch (IOException e) {
			throw new IllegalStateException(
					"Unable to load Lua blob '" + endpoint.getScriptPath() + "': " + e.getMessage(), e);
		}

		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, endpoint.getTimeoutSeconds());

		LuaValue chunk = globals.load(script, "@" + endpoint.getScriptPath());
		chunk.call();

		LuaValue aimcMain = globals.get("aimc_main");
		if (!aimcMain.isfunction()) {
			throw new IllegalStateException("Lua blob '" + endpoint.getScriptPath()
					+ "' must define the AIMC entry point: function aimc_main(sources)");
		}

		Object body = getBody(exchange);
		LuaValue sources = buildSources(body);

		LuaValue result;
		try {
			result = aimcMain.invoke(sources).arg1();
		} catch (LuaError e) {
			throw new RuntimeException("Lua transformation '" + endpoint.getScriptPath() + "' failed: "
					+ e.getMessage(), e);
		}

		if (!result.istable()) {
			throw new IllegalStateException("Lua blob '" + endpoint.getScriptPath()
					+ "' must return a table (map keyed by SinkId), got: " + result.typename());
		}

		Object javaResult = LuaJsonCodec.fromLua(result);
		setBody(exchange, javaResult);

		LOGGER.debug("Lua transformation '{}' applied to exchange {}", endpoint.getScriptPath(), exchange.getExchangeId());
	}

	private LuaScriptLoader getScriptLoader() {
		LuaScriptLoader current = scriptLoader;
		if (current == null) {
			synchronized (this) {
				current = scriptLoader;
				if (current == null) {
					current = new LuaScriptLoader(endpoint.isContentCache(), endpoint.getMaxScriptBytes());
					scriptLoader = current;
				}
			}
		}
		return current;
	}

	private Object getBody(Exchange exchange) throws IOException {
		Object body = exchange.getMessage().getBody();
		if (body == null) {
			return null;
		}
		if ("JsonValue".equalsIgnoreCase(endpoint.getInputType())) {
			return body;
		}
		String json = exchange.getMessage().getBody(String.class);
		return mapper.readValue(json, Object.class);
	}

	private LuaValue buildSources(Object body) {
		LuaValue sources = LuaValue.tableOf();
		String keys = endpoint.getSourceKeys();
		if (keys == null || keys.trim().isEmpty() || "body".equalsIgnoreCase(keys.trim())) {
			sources.set("body", LuaJsonCodec.toLua(body));
			return sources;
		}
		String[] parts = keys.split(",");
		if (parts.length == 1) {
			// single SourceId: the whole payload IS that source's data message
			// (AIMC contract: sources[SourceId] = the data from that source)
			sources.set(parts[0].trim(), LuaJsonCodec.toLua(body));
			return sources;
		}
		// comma-separated list: extract each key from the payload object
		LuaValue payload = LuaJsonCodec.toLua(body);
		for (String key : parts) {
			key = key.trim();
			if (!key.isEmpty()) {
				sources.set(key, payload.get(key));
			}
		}
		return sources;
	}

	private void setBody(Exchange exchange, Object value) throws IOException {
		if ("JsonValue".equalsIgnoreCase(endpoint.getOutputType())) {
			exchange.getMessage().setBody(value);
		} else {
			exchange.getMessage().setBody(mapper.writeValueAsString(value));
		}
	}
}
