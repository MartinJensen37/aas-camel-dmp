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

import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.DebugLib;
import org.luaj.vm2.lib.VarArgFunction;

/**
 * CPU timeout guard for Lua blobs (SPEC §7.3).
 *
 * <p>
 * LuaJ is not interruptible mid-evaluation, so the guard relies on an
 * instruction-count hook installed via {@code debug.sethook}: every
 * {@link #HOOK_INSTRUCTION_COUNT} VM instructions the hook checks the elapsed
 * wall time against the deadline and raises a {@link LuaError} when exceeded.
 * A misbehaving blob can therefore never hang the route.
 *
 * <p>
 * After installing the hook the {@code debug} table is removed from the
 * sandbox so blobs cannot tamper with the hook or inspect the debug API.
 *
 * @author aas-camel-dmp
 */
public final class LuaTimeoutHook {

	/** Check the deadline every N VM instructions (tunable). */
	public static final int HOOK_INSTRUCTION_COUNT = 10000;

	private LuaTimeoutHook() {
		throw new IllegalStateException("Utility class");
	}

	/**
	 * Installs the timeout hook on the given globals and hides {@code debug}
	 * from the blob.
	 *
	 * @param globals        the sandbox globals
	 * @param timeoutSeconds the CPU budget per invocation
	 */
	public static void install(Globals globals, double timeoutSeconds) {
		if (timeoutSeconds <= 0) {
			return;
		}

		// DebugLib registers itself into package.loaded; provide a minimal package
		// table for the load, then hide it again (SPEC §7.2: package is excluded).
		LuaValue packageTable = LuaValue.tableOf();
		packageTable.set("loaded", LuaValue.tableOf());
		packageTable.set("preload", LuaValue.tableOf());
		globals.set("package", packageTable);

		globals.load(new DebugLib());

		final long deadlineNanos = System.nanoTime() + (long) (timeoutSeconds * 1_000_000_000L);

		LuaValue hook = new VarArgFunction() {
			@Override
			public Varargs invoke(Varargs args) {
				if (System.nanoTime() - deadlineNanos > 0) {
					throw new LuaError("Lua transformation timed out after " + timeoutSeconds + " s");
				}
				return LuaValue.NONE;
			}
		};

		globals.get("debug").get("sethook")
				.invoke(hook, LuaValue.valueOf(""), LuaValue.valueOf(HOOK_INSTRUCTION_COUNT));

		// hide the debug library and package machinery from blobs (SPEC §7.2)
		globals.set("debug", LuaValue.NIL);
		globals.set("package", LuaValue.NIL);
	}
}
