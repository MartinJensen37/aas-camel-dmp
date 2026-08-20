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
package org.eclipse.digitaltwin.basyx.databridge.core.operation;

/**
 * Resolved endpoints of one operation route (M7), used by the
 * {@link MqttRequestReplyBridge}.
 *
 * @author aas-camel-dmp
 */
public class OperationContext {

	/** The action (skill) name, e.g. {@code Halt} — also the Lua source key. */
	public final String action;
	/** Lua endpoint URI of the request-direction blob (REST → MQTT). */
	public final String requestLuaUri;
	/** Lua endpoint URI of the response-direction blob (MQTT → REST). */
	public final String responseLuaUri;
	/** Paho producer URI of the native MQTT command topic (e.g. .../CMD/Halt). */
	public final String commandSinkUri;
	/** How long to wait for the correlated reply, in seconds. */
	public final long timeoutSeconds;

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, long timeoutSeconds) {
		this.action = action;
		this.requestLuaUri = requestLuaUri;
		this.responseLuaUri = responseLuaUri;
		this.commandSinkUri = commandSinkUri;
		this.timeoutSeconds = timeoutSeconds;
	}
}
