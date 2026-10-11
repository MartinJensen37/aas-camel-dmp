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
package org.eclipse.digitaltwin.basyx.dmp.lua.harness;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaJsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaSandbox;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaTimeoutHook;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;


/**
 * Local harness (SPEC §8.2 / G7) to verify a Lua blob before deployment.
 *
 * <p>
 * Usage:
 *
 * <pre>
 * java ... LuaHarness &lt;script.lua&gt; [sources.json] [timeoutSeconds]
 * </pre>
 *
 * Loads the blob, builds the sandbox, calls {@code aimc_main(sources)} with the
 * sample {@code sources} (defaults to an empty map) and prints the returned map
 * as JSON.
 *
 * @author aas-camel-dmp
 */
public final class LuaHarness {

	private LuaHarness() {
		throw new IllegalStateException("Utility class");
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: LuaHarness <script.lua> [sources.json] [timeoutSeconds]");
			System.exit(1);
		}

		String scriptPath = args[0];
		String sourcesPath = args.length > 1 ? args[1] : null;
		double timeoutSeconds = args.length > 2 ? Double.parseDouble(args[2]) : 2.0;

		Object sources = sourcesPath == null ? new java.util.HashMap<String, Object>()
				: JsonCodec.parseJson(new String(Files.readAllBytes(Paths.get(sourcesPath)), StandardCharsets.UTF_8));

		String script = new String(Files.readAllBytes(Paths.get(scriptPath)), StandardCharsets.UTF_8);

		Object result = run(script, scriptPath, sources, timeoutSeconds);

		System.out.println(JsonCodec.toJson(result));
	}

	/**
	 * Runs a blob against the given {@code sources} and returns the decoded
	 * result.
	 */
	public static Object run(String script, String chunkName, Object sources, double timeoutSeconds) {
		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, timeoutSeconds);

		LuaValue chunk = globals.load(script, "@" + chunkName);
		chunk.call();

		LuaValue aimcMain = globals.get("aimc_main");
		if (!aimcMain.isfunction()) {
			throw new IllegalArgumentException(
					"Blob must define the AIMC entry point: function aimc_main(sources)");
		}

		LuaValue result = aimcMain.invoke(LuaJsonCodec.toLua(sources)).arg1();
		if (!result.istable()) {
			throw new IllegalArgumentException(
					"Blob must return a table (map keyed by SinkId), got: " + result.typename());
		}

		return LuaJsonCodec.fromLua(result);
	}

	/** Convenience overload that loads the blob from a file. */
	public static Object runFile(String scriptPath, Object sources, double timeoutSeconds) throws IOException {
		String script = new String(Files.readAllBytes(Paths.get(scriptPath)), StandardCharsets.UTF_8);
		return run(script, scriptPath, sources, timeoutSeconds);
	}
}
