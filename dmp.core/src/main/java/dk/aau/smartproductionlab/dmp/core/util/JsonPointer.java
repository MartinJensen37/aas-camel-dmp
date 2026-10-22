/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
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
package dk.aau.smartproductionlab.dmp.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal JSON Pointer (RFC 6901) navigation over plain Java object trees
 * ({@code Map}/{@code List}/scalars), as produced by Jackson
 * {@code readValue(json, Object.class)} or Gson.
 *
 * <p>
 * Supported syntax (SPEC §6.5.1):
 * <ul>
 * <li>{@code $.State} — top-level object key</li>
 * <li>{@code /State} — RFC 6901 pointer</li>
 * <li>{@code $.ProcessQueue.mode} — dotted object keys</li>
 * <li>{@code $.WorkOrders[0].id} — 0-based array indices</li>
 * </ul>
 *
 * <p>
 * This is the shared unit of the fine (de)composition resolution (ADR-014):
 * the management node pins a {@code valuePath}, and the DMP applies it at the
 * boundary — disassembly pre-transform (source), assembly post-transform
 * (sink). The DMP never computes the pointer; it only applies what it is given.
 */
public final class JsonPointer {

	/** A token carrying an array index, e.g. {@code "[3]"} (0-based). */
	private static final String INDEX_PREFIX = "[";
	private static final String INDEX_SUFFIX = "]";

	/**
	 * Marker returned by {@link #extract(Object)} when the pointer resolves to
	 * a <b>legit JSON {@code null}</b> leaf — present, but null. Distinct from
	 * an empty Optional (a missing path), so callers can tell a payload that
	 * really says {@code {"State": null}} apart from one without the key
	 * (T1.12). Callers should map this marker to a Java {@code null} value.
	 */
	public static final Object JSON_NULL = new Object() {
		@Override
		public String toString() {
			return "JSON_NULL";
		}
	};

	private final List<String> tokens;

	private JsonPointer(List<String> tokens) {
		this.tokens = tokens;
	}

	/**
	 * Parses a JSON Pointer / JSONPath-ish expression into tokens. An empty or
	 * blank expression yields an empty pointer (no navigation → deliver as-is).
	 *
	 * @param expression the pointer, e.g. {@code $.State} or {@code /a/b[0]}
	 * @return a compiled pointer
	 * @throws IllegalArgumentException on malformed syntax
	 */
	public static JsonPointer parse(String expression) {
		if (expression == null || expression.isBlank()) {
			return new JsonPointer(List.of());
		}
		String expr = expression.trim();
		if (expr.startsWith("/")) {
			return parseRfc6901(expr);
		}
		return parsePathish(expr);
	}

	private static JsonPointer parseRfc6901(String expr) {
		// "/" alone addresses the root (a single empty-token pointer is treated
		// as no-op here since data is delivered by the host, not the pointer).
		if (expr.length() == 1) {
			return new JsonPointer(List.of());
		}
		List<String> tokens = new ArrayList<>();
		for (String part : expr.substring(1).split("/", -1)) {
			tokens.add(part.replace("~1", "/").replace("~0", "~"));
		}
		return new JsonPointer(tokens);
	}

	private static JsonPointer parsePathish(String expr) {
		String s = expr;
		if (s.startsWith("$")) {
			s = s.substring(1);
		}
		List<String> tokens = new ArrayList<>();
		int i = 0;
		while (i < s.length()) {
			char c = s.charAt(i);
			if (c == '.' || c == ' ') {
				i++;
				continue;
			}
			if (c == '[') {
				int end = s.indexOf(']', i);
				if (end < 0) {
					throw new IllegalArgumentException("Unterminated '[' in JSON pointer: " + expr);
				}
				String inner = s.substring(i + 1, end).trim();
				if (inner.startsWith("'") && inner.endsWith("'") && inner.length() >= 2) {
					tokens.add(inner.substring(1, inner.length() - 1));
				} else if (inner.startsWith("\"") && inner.endsWith("\"") && inner.length() >= 2) {
					tokens.add(inner.substring(1, inner.length() - 1));
				} else if (inner.matches("-?\\d+")) {
					tokens.add(INDEX_PREFIX + inner + INDEX_SUFFIX);
				} else if (!inner.isEmpty()) {
					tokens.add(unescape(inner));
				}
				i = end + 1;
			} else {
				int dot = s.indexOf('.', i);
				int br = s.indexOf('[', i);
				int end;
				if (dot >= 0 && br >= 0) {
					end = Math.min(dot, br);
				} else if (dot >= 0) {
					end = dot;
				} else if (br >= 0) {
					end = br;
				} else {
					end = s.length();
				}
				String token = s.substring(i, end);
				if (!token.isEmpty()) {
					tokens.add(unescape(token));
				}
				i = end;
			}
		}
		return new JsonPointer(tokens);
	}

	/**
	 * Extracts the value at this pointer. Returns {@link Optional#empty()} when
	 * any token does not resolve (missing key / out-of-range index / navigating
	 * into a scalar), so callers can decide between a coarse fallback and a
	 * clear route error (SPEC §8.4 #8). A pointer that resolves to a JSON
	 * {@code null} leaf returns {@link Optional#of(JSON_NULL)} — present, but
	 * null (T1.12); callers map the marker to a Java {@code null}.
	 */
	public Optional<Object> extract(Object root) {
		Object current = root;
		for (String token : tokens) {
			if (current == null || current == JSON_NULL) {
				// cannot navigate below a null node → treat as missing
				return Optional.empty();
			}
			if (isIndex(token)) {
				int index = Integer.parseInt(token.substring(1, token.length() - 1));
				if (index < 0 || !(current instanceof List<?> list) || index >= list.size()) {
					return Optional.empty();
				}
				current = list.get(index);
			} else {
				if (!(current instanceof Map<?, ?> map) || !map.containsKey(token)) {
					return Optional.empty();
				}
				current = map.get(token);
			}
		}
		return current == null ? Optional.of(JSON_NULL) : Optional.of(current);
	}

	/**
	 * Convenience: parses and extracts in one call.
	 */
	public static Optional<Object> extract(Object root, String expression) {
		return parse(expression).extract(root);
	}

	/**
	 * The leaf object key of the pointer, e.g. {@code SetPoint} for
	 * {@code $.SetPoint} and {@code id} for {@code $.WorkOrders[0].id}; empty
	 * for an index-only pointer or an empty pointer. Used for post-transform
	 * assembly (placing the {@code SinkId} value into the constructed outgoing
	 * payload, SPEC §6.5.4).
	 */
	public String leafKey() {
		for (int i = tokens.size() - 1; i >= 0; i--) {
			if (!isIndex(tokens.get(i))) {
				return tokens.get(i);
			}
		}
		return "";
	}

	public boolean isEmpty() {
		return tokens.isEmpty();
	}

	public List<String> tokens() {
		return tokens;
	}

	private static boolean isIndex(String token) {
		return token.length() >= 2 && token.startsWith(INDEX_PREFIX) && token.endsWith(INDEX_SUFFIX);
	}

	/**
	 * RFC 6901 unescaping ({@code ~1} → {@code /}, {@code ~0} → {@code ~}) for
	 * plain pathish tokens; the RFC-style branch and quoted bracket keys
	 * already carry their literals verbatim (T1.12: previously only the
	 * {@code /a~1b} syntax decoded escapes, so {@code $.a~1b} silently missed).
	 */
	private static String unescape(String token) {
		if (token.indexOf('~') < 0) {
			return token;
		}
		return token.replace("~1", "/").replace("~0", "~");
	}

	private static String escape(String token) {
		return token.replace("~", "~0").replace("/", "~1");
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("$");
		for (String token : tokens) {
			if (isIndex(token)) {
				sb.append(token);
			} else {
				sb.append('.').append(escape(token));
			}
		}
		return sb.toString();
	}
}
