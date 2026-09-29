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
package org.eclipse.digitaltwin.basyx.dmp.lua.harness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.CompiledLuaScript;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaJsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaSandbox;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaScriptLoader;
import org.eclipse.digitaltwin.basyx.dmp.lua.sandbox.LuaTimeoutHook;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;

/**
 * Contract test matrix for the Lua transformation blob (SPEC §8.4). These
 * tests exercise the same blob files that run in the DMP.
 */
class LuaContractTest {

	private static final double TEST_TIMEOUT_SECONDS = 1.0;

	@TempDir
	Path tempDir;

	private String blob(String name) throws IOException {
		return new String(
				getClass().getResourceAsStream("/blobs/" + name).readAllBytes(),
				StandardCharsets.UTF_8);
	}

	@Test
	void test01LoadBlobDefinesAimcMain() throws IOException {
		String script = blob("simple.lua");
		Globals globals = LuaSandbox.createGlobals();
		globals.load(script, "@simple.lua").call();

		LuaValue aimcMain = globals.get("aimc_main");
		assertTrue(aimcMain.isfunction(), "aimc_main must exist after loading");
	}

	@Test
	void test02CallAimcMainReturnsMappedResult() throws IOException {
		Object result = LuaHarness.run(blob("simple.lua"), "@simple.lua",
				java.util.Map.of("a", 3.0, "b", 4.0), TEST_TIMEOUT_SECONDS);

		assertTrue(result instanceof java.util.Map, "result must be a map");
		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		// integral results come back as Long (see codecKeepsIntegralNumbersIntegral)
		assertEquals(7L, map.get("sum"));
		assertEquals(12L, map.get("product"));
	}

	@Test
	void test02bMathLibraryWorks() throws IOException {
		Object result = LuaHarness.run(blob("math.lua"), "@math.lua",
				java.util.Map.of("source1", 3.0, "source2", 4.0), TEST_TIMEOUT_SECONDS);

		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		assertEquals(5L, map.get("hypot"));
		assertEquals(3L, map.get("abs"));
		assertEquals("(3.0, 4.0)", map.get("label"));
	}

	@Test
	void test02cJsonLibWorks() throws IOException {
		Object result = LuaHarness.run(blob("usesJson.lua"), "@usesJson.lua",
				java.util.Map.of("body", "{\"a\": 2, \"b\": 3}"), TEST_TIMEOUT_SECONDS);

		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		// json.encode renders integral numbers without a decimal tail
		assertEquals("{\"a\":4,\"b\":6}", map.get("encoded"));
		assertEquals("a,b", map.get("keys"));
		assertEquals("AIMC", map.get("upper"));
	}

	@Test
	void jsonCodecPreservesNullAndEmptyContainerShapes() throws IOException {
		Object result = LuaHarness.run(
				"function aimc_main(sources)\n"
						+ " local v = json.decode('{\"n\":null,\"a\":[],\"o\":{}}')\n"
						+ " return { n = v.n, a = v.a, o = v.o }\n"
						+ "end\n",
				"@shape.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS);

		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> map = (java.util.Map<String, Object>) result;
		assertTrue(map.containsKey("n"));
		assertEquals(null, map.get("n"));
		assertEquals(java.util.List.of(), map.get("a"));
		assertEquals(java.util.Map.of(), map.get("o"));
	}

	@Test
	void codecRejectsMixedKeyTables() {
		assertThrows(LuaError.class, () -> LuaHarness.run(
				"function aimc_main(sources)\n"
						+ " return { out = { [1] = 'x', name = 'y' } }\n"
						+ "end\n",
				"@mixed.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void codecPreservesLargeIntegersOutsideLuaNumberRange() {
		BigInteger value = new BigInteger("9007199254740993");
		assertEquals(value, LuaJsonCodec.fromLua(LuaJsonCodec.toLua(value)));
	}

	/**
	 * An integral value must stay an integer across the Lua boundary. LuaJ
	 * stores every number as a double, so a caller passing {@code 1} (e.g. a
	 * PackML occupation Uuid) used to leave the DMP as {@code 1.0} — which the
	 * device then stored verbatim and echoed back in its process queue.
	 */
	@Test
	void codecKeepsIntegralNumbersIntegral() {
		Object uuid = LuaJsonCodec.fromLua(LuaJsonCodec.toLua(1L));
		assertInstanceOf(Long.class, uuid, "1 must not become a float");
		assertEquals(1L, uuid);

		Object queue = LuaJsonCodec.fromLua(LuaJsonCodec.toLua(java.util.List.of(1L, 2L)));
		assertEquals(java.util.List.of(1L, 2L), queue);
	}

	@Test
	void codecStillEncodesGenuineFractionsAsFloatingPoint() {
		assertEquals(0.5, LuaJsonCodec.fromLua(LuaJsonCodec.toLua(0.5)));
	}

	@Test
	void test03InfiniteLoopTimesOut() throws IOException {
		long start = System.nanoTime();
		LuaError error = assertThrows(LuaError.class, () -> LuaHarness.run(
				blob("infinite.lua"), "@infinite.lua", java.util.Map.of(), 0.5));
		long elapsedMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

		assertTrue(error.getMessage().contains("timed out"), "unexpected error: " + error.getMessage());
		assertTrue(elapsedMs < 10_000, "timeout must fire promptly, took " + elapsedMs + " ms");
	}

	@Test
	void test04BlobCallingOsExecuteFails() throws IOException {
		assertThrows(LuaError.class, () -> LuaHarness.run(
				blob("usesOsExecute.lua"), "@usesOsExecute.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test04bBlobCallingRequireFails() throws IOException {
		assertThrows(LuaError.class, () -> LuaHarness.run(
				blob("usesRequire.lua"), "@usesRequire.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test04cBlobCallingIoFails() throws IOException {
		assertThrows(LuaError.class, () -> LuaHarness.run(
				blob("usesIo.lua"), "@usesIo.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test04dBlobCallingLuajavaFails() throws IOException {
		assertThrows(LuaError.class, () -> LuaHarness.run(
				blob("usesLuajava.lua"), "@usesLuajava.lua", java.util.Map.of(), TEST_TIMEOUT_SECONDS));
	}

	@Test
	void test05NonTableReturnFailsWithClearError() throws IOException {
		IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
				() -> LuaHarness.run(blob("nonTable.lua"), "@nonTable.lua",
						java.util.Map.of(), TEST_TIMEOUT_SECONDS));
		assertTrue(ex.getMessage().contains("must return a table"), "unexpected message: " + ex.getMessage());
	}

	@Test
	void test06ReloadUsesNewCodeOnFileChange() throws IOException {
		Path scriptFile = tempDir.resolve("reload.lua");
		Files.writeString(scriptFile, blob("simple.lua"));

		LuaScriptLoader loader = new LuaScriptLoader(true, 1048576);

		Object first = runThroughLoader(loader, scriptFile, java.util.Map.of("a", 3.0, "b", 4.0));
		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> firstMap = (java.util.Map<String, Object>) first;
		assertEquals(7L, firstMap.get("sum"));

		// drop in a replacement blob (updated AIMC transformation)
		Files.writeString(scriptFile,
				"function aimc_main(sources)\n"
						+ "    return { [\"sum\"] = (sources[\"a\"] or 0) - (sources[\"b\"] or 0) }\n"
						+ "end\n");

		Object second = runThroughLoader(loader, scriptFile, java.util.Map.of("a", 3.0, "b", 4.0));
		@SuppressWarnings("unchecked")
		java.util.Map<String, Object> secondMap = (java.util.Map<String, Object>) second;
		assertEquals(-1L, secondMap.get("sum"), "reloaded blob must use the new code");
	}

	private Object runThroughLoader(LuaScriptLoader loader, Path scriptFile, Object sources) throws IOException {
		String script = loader.load(scriptFile.toString());
		assertNotNull(script);
		return LuaHarness.run(script, scriptFile.toString(), sources, TEST_TIMEOUT_SECONDS);
	}

	@Test
	void test07CompiledPrototypeIsReusedAcrossInvocations() throws IOException {
		Path scriptFile = tempDir.resolve("reuse.lua");
		Files.writeString(scriptFile, blob("simple.lua"));

		LuaScriptLoader loader = new LuaScriptLoader(true, 1048576);

		// Two "incoming messages": the loader returns the SAME compiled
		// prototype (no re-parsing/re-compiling on the hot path).
		CompiledLuaScript first = loader.loadCompiled(scriptFile.toString());
		CompiledLuaScript second = loader.loadCompiled(scriptFile.toString());
		assertSame(first, second);
		assertSame(first.prototype(), second.prototype());

		// Both invocations still run in fresh, isolated environments.
		java.util.Map<String, Object> r1 = runCompiled(loader, scriptFile, java.util.Map.of("a", 3.0, "b", 4.0));
		java.util.Map<String, Object> r2 = runCompiled(loader, scriptFile, java.util.Map.of("a", 10.0, "b", 5.0));
		assertEquals(7L, r1.get("sum"));
		assertEquals(15L, r2.get("sum"));
	}

	@Test
	void test08CompiledReloadRecompilesOnBlobChange() throws IOException {
		Path scriptFile = tempDir.resolve("reload-compiled.lua");
		Files.writeString(scriptFile, blob("simple.lua"));

		LuaScriptLoader loader = new LuaScriptLoader(true, 1048576);

		CompiledLuaScript first = loader.loadCompiled(scriptFile.toString());

		// drop in a replacement blob (updated AIMC transformation)
		Files.writeString(scriptFile,
				"function aimc_main(sources)\n"
						+ "    return { [\"sum\"] = (sources[\"a\"] or 0) - (sources[\"b\"] or 0) }\n"
						+ "end\n");

		CompiledLuaScript second = loader.loadCompiled(scriptFile.toString());
		assertNotSame(first.prototype(), second.prototype(), "blob change must recompile");

		java.util.Map<String, Object> result = runCompiled(loader, scriptFile, java.util.Map.of("a", 3.0, "b", 4.0));
		assertEquals(-1L, result.get("sum"), "recompiled blob must use the new code");
	}

	private java.util.Map<String, Object> runCompiled(
			LuaScriptLoader loader, Path scriptFile, Object sources) throws IOException {
		CompiledLuaScript compiled = loader.loadCompiled(scriptFile.toString());
		Globals globals = LuaSandbox.createGlobals();
		LuaTimeoutHook.install(globals, TEST_TIMEOUT_SECONDS);
		CompiledLuaScript.runChunk(compiled.prototype(), globals);

		LuaValue aimcMain = globals.get("aimc_main");
		if (!aimcMain.isfunction()) {
			throw new IllegalArgumentException("Blob must define aimc_main");
		}
		LuaValue result = aimcMain.invoke(LuaJsonCodec.toLua(sources)).arg1();
		return (java.util.Map<String, Object>) LuaJsonCodec.fromLua(result);
	}
}
