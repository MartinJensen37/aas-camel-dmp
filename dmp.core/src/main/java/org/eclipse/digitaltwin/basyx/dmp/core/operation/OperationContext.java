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
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.core.operation;

/**
 * Resolved endpoints of one operation route, shared by the declarative
 * operation-delegation processors and synchronous device invocation.
 *
 * <p>
 * The declared MQTT 5 request/reply correlation facts ({@code mqv_request_reply}
 * + {@code mqv_response_topic}) ride the {@code paho-mqtt5-inout:} endpoint options
 * (replyTopic/replyTimeout) — the route creator derives them from the
 * command-sink URI and {@code triggerData} at instantiation.
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
	/** Command sink URI (paho-mqtt5-inout:/http:/milo-client:... producer). */
	public final String commandSinkUri;
	/** How long to wait for the correlated reply, in seconds. */
	public final long timeoutSeconds;
	/** Caller-facing invocation method (DMP v3 convention: POST). */
	public final String restMethod;
	/**
	 * Native command protocol (triggerData.protocol: mqtt/http/opcua/plc4x);
	 * decides the command body encoding of the synchronous invocation.
	 */
	public final String protocol;
	/**
	 * DECLARED command parameter types (the AID action's embedded input
	 * DataSchema: name → JSON-schema type). Values are parsed with these
	 * types before they reach the native interface — the mapping is
	 * data-driven from the interface description, never name-based.
	 */
	public final java.util.Map<String, String> parameterTypes;
	/**
	 * DECLARED reply field types (the AID action's embedded output
	 * DataSchema): the reply envelope is built with the declared BaSyx
	 * value types.
	 */
	public final java.util.Map<String, String> responseTypes;
	/**
	 * DECLARED POSITIONAL reply order (the CCI operation's output_variable
	 * id_shorts, in order) — the positional contract of the awaited UA Call's
	 * output arguments. The AID's output DataSchema carries types, not the
	 * method-signature order; empty → no positional contract and an output
	 * argument reply is rejected as ambiguous.
	 */
	public final java.util.List<String> responseOrder;
	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, long timeoutSeconds) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, timeoutSeconds, "POST");
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, long timeoutSeconds, String restMethod) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, timeoutSeconds, restMethod, null);
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, long timeoutSeconds, String restMethod, String protocol) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, timeoutSeconds,
				restMethod, protocol, java.util.Map.of(), java.util.Map.of());
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, long timeoutSeconds, String restMethod, String protocol,
			java.util.Map<String, String> parameterTypes,
			java.util.Map<String, String> responseTypes) {
		this(action, requestLuaUri, responseLuaUri, commandSinkUri, timeoutSeconds,
				restMethod, protocol, parameterTypes, responseTypes, java.util.List.of());
	}

	public OperationContext(String action, String requestLuaUri, String responseLuaUri,
			String commandSinkUri, long timeoutSeconds, String restMethod, String protocol,
			java.util.Map<String, String> parameterTypes,
			java.util.Map<String, String> responseTypes, java.util.List<String> responseOrder) {
		this.action = action;
		this.requestLuaUri = requestLuaUri;
		this.responseLuaUri = responseLuaUri;
		this.commandSinkUri = commandSinkUri;
		this.timeoutSeconds = timeoutSeconds;
		this.restMethod = restMethod;
		this.protocol = protocol == null ? "" : protocol.toLowerCase();
		this.parameterTypes = parameterTypes == null ? java.util.Map.of() : java.util.Map.copyOf(parameterTypes);
		this.responseTypes = responseTypes == null ? java.util.Map.of() : java.util.Map.copyOf(responseTypes);
		this.responseOrder = responseOrder == null ? java.util.List.of() : java.util.List.copyOf(responseOrder);
	}
}
