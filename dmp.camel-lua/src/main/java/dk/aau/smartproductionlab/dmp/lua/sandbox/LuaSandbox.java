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
package dk.aau.smartproductionlab.dmp.lua.sandbox;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.BaseLib;
import org.luaj.vm2.lib.MathLib;
import org.luaj.vm2.lib.StringLib;
import org.luaj.vm2.lib.TableLib;
import org.luaj.vm2.lib.ZeroArgFunction;

/**
 * Whitelisted global environment factory for Lua blobs (SPEC §7.2).
 *
 * <p>
 * Available to blobs: base functions ({@code assert, error, ipairs, next,
 * pairs, pcall, select, tonumber, tostring, type}, plus harmless
 * {@code print}), {@code math}, {@code table}, {@code string}, a custom
 * {@code json} lib, and {@code os.clock} only.
 *
 * <p>
 * Explicitly excluded: {@code os} (except {@code clock}), {@code io},
 * {@code require}/{@code dofile}/{@code loadfile}/{@code load},
 * {@code debug}, {@code luajava}, {@code package}, {@code coroutine},
 * {@code getmetatable}/{@code setmetatable}.
 *
 * <p>
 * LuaJ {@code Globals} are not thread-safe (SPEC §7.5): the caller creates one
 * sandbox per exchange/invocation.
 *
 * @author aas-camel-dmp
 */
public final class LuaSandbox {

	/** Base functions stripped from {@link BaseLib} that are not whitelisted. */
	private static final Set<String> STRIPPED_BASE_FUNCTIONS = new HashSet<>(Arrays.asList(
			"dofile", "loadfile", "load", "getmetatable", "setmetatable", "collectgarbage"));

	static {
		// LuaJ makes the first loaded string library the metatable of every string,
		// process-wide and permanently. Load a private one before any blob runs, so
		// no blob owns the table behind ("..."):method() calls in other sandboxes.
		Globals primer = new Globals();
		LuaValue packageTable = LuaValue.tableOf();
		packageTable.set("loaded", LuaValue.tableOf());
		primer.set("package", packageTable);
		primer.load(new StringLib());
	}

	private LuaSandbox() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Creates a fresh sandboxed {@link Globals} with the whitelisted
	 * environment.
	 */
	public static Globals createGlobals() {
		Globals globals = new Globals();

		// install the Lua source compiler (Globals.load requires it)
		LuaC.install(globals);

		// LuaJ library loaders register themselves into package.loaded; provide a
		// minimal package table first (it is removed again before blobs run).
		LuaValue packageTable = LuaValue.tableOf();
		packageTable.set("loaded", LuaValue.tableOf());
		packageTable.set("preload", LuaValue.tableOf());
		globals.set("package", packageTable);

		// whitelisted libraries
		globals.load(new BaseLib());
		globals.load(new TableLib());
		globals.load(new StringLib());
		globals.load(new MathLib());

		// strip non-whitelisted base functions
		STRIPPED_BASE_FUNCTIONS.forEach(name -> globals.set(name, LuaValue.NIL));

		// os: only clock (SPEC §7.2)
		LuaValue os = LuaValue.tableOf();
		os.set("clock", new ZeroArgFunction() {
			@Override
			public LuaValue call() {
				return LuaValue.valueOf(System.nanoTime() / 1_000_000_000d);
			}
		});
		globals.set("os", os);

		// optional json lib for advanced use
		LuaJsonCodec.install(globals);

		// hide package/require machinery from blobs
		globals.set("package", LuaValue.NIL);

		return globals;
	}
}
