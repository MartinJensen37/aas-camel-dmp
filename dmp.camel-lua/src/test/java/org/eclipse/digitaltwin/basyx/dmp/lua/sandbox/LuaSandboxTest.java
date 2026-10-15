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
package org.eclipse.digitaltwin.basyx.dmp.lua.sandbox;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;

/**
 * Verifies the whitelisted global environment (SPEC §7.2).
 */
class LuaSandboxTest {

	private final Globals globals = LuaSandbox.createGlobals();

	@Test
	void whitelistedBaseFunctionsAreAvailable() {
		for (String name : new String[] { "assert", "error", "ipairs", "next", "pairs", "pcall",
				"select", "tonumber", "tostring", "type" }) {
			assertTrue(globals.get(name).isfunction(), "expected base function: " + name);
		}
	}

	@Test
	void whitelistedLibrariesAreAvailable() {
		assertTrue(globals.get("math").istable(), "math library missing");
		assertTrue(globals.get("math").get("sqrt").isfunction(), "math.sqrt missing");
		assertTrue(globals.get("table").istable(), "table library missing");
		assertTrue(globals.get("string").istable(), "string library missing");
	}

	@Test
	void jsonLibIsAvailable() {
		assertTrue(globals.get("json").istable(), "json library missing");
		assertTrue(globals.get("json").get("decode").isfunction(), "json.decode missing");
		assertTrue(globals.get("json").get("encode").isfunction(), "json.encode missing");
	}

	@Test
	void osOnlyExposesClock() {
		assertTrue(globals.get("os").istable(), "os table missing");
		assertTrue(globals.get("os").get("clock").isfunction(), "os.clock missing");
		assertTrue(globals.get("os").get("execute").isnil(), "os.execute must not be exposed");
		assertTrue(globals.get("os").get("getenv").isnil(), "os.getenv must not be exposed");
		assertTrue(globals.get("os").get("date").isnil(), "os.date must not be exposed");
	}

	@Test
	void dangerousGlobalsAreExcluded() {
		for (String name : new String[] { "io", "require", "dofile", "loadfile", "load",
				"luajava", "package", "coroutine", "debug", "getmetatable", "setmetatable",
				"collectgarbage" }) {
			LuaValue v = globals.get(name);
			assertTrue(v.isnil() || v.isfunction() == false, "dangerous global not excluded: " + name);
		}
	}
}
