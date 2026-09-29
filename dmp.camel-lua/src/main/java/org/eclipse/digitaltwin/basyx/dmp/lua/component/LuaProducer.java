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

import java.io.IOException;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.CompiledLuaScript;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaJsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaSandbox;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaScriptLoader;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaTimeoutHook;
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
 * The blob is compiled ONCE per route ({@link CompiledLuaScript}); per
 * invocation (SPEC §7.5 — LuaJ {@code Globals} are not thread-safe):
 * <ol>
 * <li>load the cached compiled prototype (recompiled only on change)</li>
 * <li>build a fresh sandboxed {@link Globals} with the whitelist</li>
 * <li>install the CPU timeout hook</li>
 * <li>bind the pre-compiled prototype to the fresh environment (defines
 * {@code aimc_main}) — no re-parsing/re-compiling</li>
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
		// The blob is compiled ONCE per route and reused; only the sandbox is
		// rebuilt per invocation (SPEC §7.5: LuaJ Globals are not thread-safe,
		// and blobs may mutate their environment — full isolation per message).
		CompiledLuaScript compiled;
		try {
			compiled = getScriptLoader().loadCompiled(endpoint.getScriptPath());
		} catch (IOException e) {
			throw new IllegalStateException(
					"Unable to load Lua blob '" + endpoint.getScriptPath() + "': " + e.getMessage(), e);
		}

		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, endpoint.getTimeoutSeconds());

		// Bind the pre-compiled prototype to the fresh environment and run the
		// top-level chunk (defines aimc_main) — no re-parsing/re-compiling.
		CompiledLuaScript.runChunk(compiled.prototype(), globals);

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

		LOGGER.debug("lua {}: input-keys={} sourceKeys={} result={}",
				endpoint.getScriptPath(),
				body == null ? "null" : (body instanceof Map<?, ?> m ? m.keySet() : body.getClass().getSimpleName()),
				endpoint.getSourceKeys(), javaResult);

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
			sources.set("body", LuaJsonCodec.toLua(disassemble(body)));
			return sources;
		}
		String[] parts = keys.split(",");
		if (parts.length == 1) {
			// single SourceId: the whole payload IS that source's data message
			// (AIMC contract: sources[SourceId] = the data from that source).
			// The execution-policy merge envelope keys the payload by the
			// SourceId — unwrap exactly that level (a legacy single-source
			// body that is not the envelope passes through unchanged).
			String key = parts[0].trim();
			Object payload = body;
			if (body instanceof Map<?, ?> map && map.size() == 1
					&& map.containsKey(key)) {
				payload = map.get(key);
			}
			// A valuePath (fine resolution, SPEC §6.5) disassembles the
			// payload into exactly the leaf the mapping needs.
			sources.set(key, LuaJsonCodec.toLua(disassemble(payload)));
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

	/**
	 * Source-side fine resolution (ADR-014 / SPEC §6.5): when a {@code valuePath}
	 * is configured, reduces the incoming canonical payload to the pinned leaf
	 * (disassembly) before it is placed under the {@code SourceId}; absent →
	 * coarse (whole payload delivered). A pointer on a missing path is a clear
	 * route error (SPEC §8.4 #8).
	 */
	private Object disassemble(Object body) {
		String pointer = endpoint.getValuePath();
		if (pointer == null || pointer.isBlank() || body == null) {
			return body;
		}
		java.util.Optional<Object> leaf = org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.extract(body, pointer);
		if (leaf.isEmpty()) {
			throw new IllegalArgumentException("Lua source valuePath '" + pointer
					+ "' does not resolve in the incoming payload for script '" + endpoint.getScriptPath() + "'");
		}
		// a legit JSON-null leaf delivers a Java null under the SourceId;
		// only a MISSING path is a route error (SPEC §8.4 #8, T1.12)
		Object value = leaf.get();
		return value == org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer.JSON_NULL ? null : value;
	}

	private void setBody(Exchange exchange, Object value) throws IOException {
		if ("JsonValue".equalsIgnoreCase(endpoint.getOutputType())) {
			exchange.getMessage().setBody(value);
		} else {
			exchange.getMessage().setBody(mapper.writeValueAsString(value));
		}
	}
}
