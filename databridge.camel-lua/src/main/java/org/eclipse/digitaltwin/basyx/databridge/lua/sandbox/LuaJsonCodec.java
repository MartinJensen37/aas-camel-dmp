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
package org.eclipse.digitaltwin.basyx.databridge.lua.sandbox;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.TwoArgFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON bridge between the host and the Lua sandbox (ADR-004: JSON lives in the
 * host, blobs are table-in/table-out).
 *
 * <p>
 * The optional in-blob {@code json} library is registered by
 * {@link #install(org.luaj.vm2.Globals)} as {@code json.decode} /
 * {@code json.encode} for advanced use.
 *
 * @author aas-camel-dmp
 */
public final class LuaJsonCodec extends TwoArgFunction {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private LuaJsonCodec() {
	}

	/**
	 * Registers the {@code json} table in the given globals.
	 */
	public static void install(LuaValue globals) {
		LuaValue json = LuaValue.tableOf();
		json.set("decode", new DecodeFunction());
		json.set("encode", new EncodeFunction());
		globals.set("json", json);
	}

	@Override
	public LuaValue call(LuaValue modname, LuaValue env) {
		install(env);
		return env.get("json");
	}

	/**
	 * Converts a host-side (Jackson-parsed) JSON value to a real Lua value
	 * (LuaTable for maps/lists, LuaNumber, LuaString, LuaBoolean). Does NOT use
	 * {@code CoerceJavaToLua} so blobs operate on genuine Lua values (and no
	 * JVM reflection / module access is involved).
	 */
	public static LuaValue toLua(Object jsonValue) {
		if (jsonValue == null) {
			return LuaValue.NIL;
		}
		if (jsonValue instanceof Map) {
			LuaValue table = LuaValue.tableOf();
			for (Map.Entry<?, ?> entry : ((Map<?, ?>) jsonValue).entrySet()) {
				table.set(String.valueOf(entry.getKey()), toLua(entry.getValue()));
			}
			return table;
		}
		if (jsonValue instanceof List) {
			LuaValue table = LuaValue.tableOf();
			int i = 1;
			for (Object item : (List<?>) jsonValue) {
				table.set(i++, toLua(item));
			}
			return table;
		}
		if (jsonValue instanceof Boolean) {
			return LuaValue.valueOf((Boolean) jsonValue);
		}
		if (jsonValue instanceof Number) {
			return LuaValue.valueOf(((Number) jsonValue).doubleValue());
		}
		if (jsonValue instanceof byte[]) {
			return LuaValue.valueOf((byte[]) jsonValue);
		}
		return LuaValue.valueOf(jsonValue.toString());
	}

	/** Converts a Lua value to a host-side JSON-compatible Java value. */
	public static Object fromLua(LuaValue value) {
		if (value.isnil()) {
			return null;
		}
		if (value.isboolean()) {
			return value.toboolean();
		}
		if (value.isnumber()) {
			return value.todouble();
		}
		if (value.isstring()) {
			return value.tojstring();
		}
		if (value.istable()) {
			return toJavaMapOrList(value.checktable());
		}
		return value.tojstring();
	}

	private static Object toJavaMapOrList(LuaTable table) {
		List<Object> list = new ArrayList<>();
		for (int i = 1;; i++) {
			LuaValue item = table.get(i);
			if (item.isnil()) {
				break;
			}
			list.add(fromLua(item));
		}
		if (!list.isEmpty()) {
			return list;
		}

		Map<String, Object> map = new LinkedHashMap<>();
		LuaValue key = LuaValue.NIL;
		while (true) {
			Varargs n = table.next(key);
			if (n.arg1().isnil()) {
				break;
			}
			key = n.arg1();
			map.put(key.tojstring(), fromLua(n.arg(2)));
		}
		return map;
	}

	private static final class DecodeFunction extends org.luaj.vm2.lib.OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			try {
				Object parsed = MAPPER.readValue(arg.checkjstring(), Object.class);
				return toLua(parsed);
			} catch (Exception e) {
				throw new LuaError("json.decode failed: " + e.getMessage());
			}
		}
	}

	private static final class EncodeFunction extends org.luaj.vm2.lib.OneArgFunction {
		@Override
		public LuaValue call(LuaValue arg) {
			try {
				return LuaValue.valueOf(MAPPER.writeValueAsString(fromLua(arg)));
			} catch (Exception e) {
				throw new LuaError("json.encode failed: " + e.getMessage());
			}
		}
	}
}
