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
package dk.aau.smartproductionlab.dmp.lua.component;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaJsonCodec;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaSandbox;
import dk.aau.smartproductionlab.dmp.lua.sandbox.LuaTimeoutHook;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataTransformerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executes the AIMC-compliant Lua blob for each exchange.
 *
 * <p>
 * The blob arrives from the AIMC and is compiled once when the mapping is
 * compiled ({@code LuaCompiledSource}); per invocation (SPEC §7.5 — LuaJ
 * {@code Globals} are not thread-safe):
 * <ol>
 * <li>build a fresh sandboxed {@link Globals} with the whitelist</li>
 * <li>install the CPU timeout hook</li>
 * <li>bind the pre-compiled prototype to the fresh environment (defines
 * {@code aimc_main}) — no re-parsing/re-compiling</li>
 * <li>convert the decoded Java value tree to isolated Lua values</li>
 * <li>call {@code aimc_main(sources)} where {@code sources} is keyed by
 * {@code SourceId}; the global {@code meta} describes the sources, the sinks
 * and an Operation's invocation (SPEC §8), a fresh copy per call</li>
 * <li>convert the returned Lua table (keyed by {@code SinkId}) back to a
 * decoded Java value tree</li>
 * </ol>
 *
 * @author aas-camel-dmp
 */
public class LuaProducer extends DefaultProducer {

	private static final Logger LOGGER = LoggerFactory.getLogger(LuaProducer.class);

	private final LuaEndpoint endpoint;

	public LuaProducer(LuaEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	public void process(Exchange exchange) throws Exception {
		// Only the sandbox is rebuilt per invocation (SPEC §7.5: LuaJ Globals are
		// not thread-safe, and blobs may mutate their environment).
		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, endpoint.getTimeoutSeconds());
		globals.set("meta", meta(exchange));

		// Bind the pre-compiled prototype to the fresh environment and run the
		// top-level chunk (defines aimc_main) — no re-parsing/re-compiling.
		endpoint.getSource().runChunk(globals);

		LuaValue aimcMain = globals.get("aimc_main");
		if (!aimcMain.isfunction()) {
			throw new IllegalStateException("Lua blob '" + endpoint.getDiagnosticIdentity()
					+ "' must define the AIMC entry point: function aimc_main(sources)");
		}

		Object body = getBody(exchange);
		LuaValue sources = buildSources(body);

		LuaValue result;
		try {
			// An Operation's response transformation: aimc_main(sources, requestSources).
			Object requestSources = exchange.getProperty(DataTransformerConfiguration.REQUEST_SOURCES);
			result = requestSources == null ? aimcMain.invoke(sources).arg1()
					: aimcMain.invoke(sources, LuaJsonCodec.toLua(requestSources)).arg1();
		} catch (LuaError e) {
			throw new RuntimeException("Lua transformation '" + endpoint.getDiagnosticIdentity() + "' failed: "
					+ e.getMessage(), e);
		}

		if (!result.istable()) {
			throw new IllegalStateException("Lua blob '" + endpoint.getDiagnosticIdentity()
					+ "' must return a table (map keyed by SinkId), got: " + result.typename());
		}

		Object javaResult = LuaJsonCodec.fromLua(result);
		setBody(exchange, javaResult);

		LOGGER.debug("lua {}: input-keys={} sourceKeys={} result={}",
				endpoint.getDiagnosticIdentity(),
				body == null ? "null" : (body instanceof Map<?, ?> m ? m.keySet() : body.getClass().getSimpleName()),
				endpoint.getSourceKeys(), javaResult);

		LOGGER.debug("Lua transformation '{}' applied to exchange {}", endpoint.getDiagnosticIdentity(), exchange.getExchangeId());
	}

	/** The exchange's {@link DataTransformerConfiguration#META}; {@code sources} and {@code sinks} are always tables. */
	private static LuaValue meta(Exchange exchange) {
		Object meta = exchange.getProperty(DataTransformerConfiguration.META);
		LuaValue table = meta instanceof Map<?, ?> ? LuaJsonCodec.toLua(meta) : LuaValue.tableOf();
		for (String part : new String[] { "sources", "sinks" })
			if (!table.get(part).istable()) table.set(part, LuaValue.tableOf());
		return table;
	}

	private Object getBody(Exchange exchange) {
		return exchange.getMessage().getBody();
	}

	private LuaValue buildSources(Object body) {
		if (endpoint.isKeyedInput()) {
			if (!(body instanceof Map<?, ?> map) || map.keySet().stream().anyMatch(key -> !(key instanceof String))) {
				throw new IllegalArgumentException("Keyed Lua input must be a SourceId-keyed map");
			}
			return LuaJsonCodec.toLua(body);
		}
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
		java.util.Optional<Object> leaf = dk.aau.smartproductionlab.dmp.core.util.JsonPointer.extract(body, pointer);
		if (leaf.isEmpty()) {
			throw new IllegalArgumentException("Lua source valuePath '" + pointer
					+ "' does not resolve in the incoming payload for script '" + endpoint.getDiagnosticIdentity() + "'");
		}
		// a legit JSON-null leaf delivers a Java null under the SourceId;
		// only a MISSING path is a route error (SPEC §8.4 #8, T1.12)
		Object value = leaf.get();
		return value == dk.aau.smartproductionlab.dmp.core.util.JsonPointer.JSON_NULL ? null : value;
	}

	private void setBody(Exchange exchange, Object value) {
		exchange.getMessage().setBody(value);
	}
}
