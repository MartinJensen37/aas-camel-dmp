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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Loads Lua blob files at runtime (SPEC §7.4).
 *
 * <p>
 * The blob content originates only from the AIMC
 * {@code MappingConfiguration.Transformation} Blob, materialized by the
 * management node into a file. With {@code contentCache=true} (default) the
 * script is cached AND pre-compiled (see {@link CompiledLuaScript}) and
 * reloaded/recompiled when its mtime or content hash changes, so a
 * dropped-in replacement (e.g. an updated AIMC blob) is picked up without a
 * restart — while steady-state messages reuse the compiled prototype.
 *
 * @author aas-camel-dmp
 */
public final class LuaScriptLoader {

	private final boolean contentCache;
	private final long maxScriptBytes;

	private volatile CachedScript cached;

	public LuaScriptLoader(boolean contentCache, long maxScriptBytes) {
		this.contentCache = contentCache;
		this.maxScriptBytes = maxScriptBytes;
	}

	/**
	 * Returns the current script content for the given path, honoring the cache
	 * and the max blob size.
	 */
	public String load(String path) throws IOException {
		return loadEntry(path).content;
	}

	/**
	 * Returns the blob compiled ONCE (prototype), honoring the cache and the
	 * max blob size. Recompiles only when the file's content actually changed,
	 * so incoming messages are transformed by the pre-compiled script (SPEC
	 * §7.5, M7.1).
	 */
	public CompiledLuaScript loadCompiled(String path) throws IOException {
		return loadEntry(path).compiled;
	}

	private CachedScript loadEntry(String path) throws IOException {
		Path file = Paths.get(path);
		long lastModified = Files.getLastModifiedTime(file).toMillis();
		long size = Files.size(file);

		CachedScript current = cached;
		if (contentCache && current != null && current.matches(lastModified, size)) {
			return current;
		}

		if (size > maxScriptBytes) {
			throw new IOException("Lua blob exceeds maxScriptBytes=" + maxScriptBytes + " (" + size + " bytes): " + path);
		}

		byte[] bytes = Files.readAllBytes(file);
		String content = new String(bytes, StandardCharsets.UTF_8);
		String sha = sha256(bytes);

		if (contentCache && current != null && current.sha.equals(sha)) {
			// content unchanged; only refresh metadata (keep the compiled prototype)
			current = new CachedScript(lastModified, size, sha, current.content, current.compiled);
			cached = current;
			return current;
		}

		CompiledLuaScript compiled = CompiledLuaScript.compile(content, path);
		current = new CachedScript(lastModified, size, sha, content, compiled);
		cached = current;
		return current;
	}

	private static String sha256(byte[] bytes) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return HexFormat.of().formatHex(digest.digest(bytes));
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 unavailable", e);
		}
	}

	private static final class CachedScript {
		final long lastModified;
		final long size;
		final String sha;
		final String content;
		final CompiledLuaScript compiled;

		CachedScript(long lastModified, long size, String sha, String content, CompiledLuaScript compiled) {
			this.lastModified = lastModified;
			this.size = size;
			this.sha = sha;
			this.content = content;
			this.compiled = compiled;
		}

		boolean matches(long lastModified, long size) {
			return this.lastModified == lastModified && this.size == size;
		}
	}
}
