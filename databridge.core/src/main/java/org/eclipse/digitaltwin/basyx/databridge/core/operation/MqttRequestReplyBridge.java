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

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

/**
 * Uuid-correlated MQTT request/reply bridge for operation delegation (M7).
 *
 * <p>
 * Flow per invocation (matches the Python operation-delegation reference):
 * <ol>
 * <li>run the request-direction Lua blob (REST operation variables → native
 * command message), keyed by the action;</li>
 * <li>inject a correlation Uuid and publish the command to the native MQTT
 * command topic;</li>
 * <li>wait (with timeout) for the correlated reply on the response topic —
 * the paho response consumer forwards payloads here via
 * {@link #onResponse(Object)};</li>
 * <li>run the response-direction Lua blob (native response → REST response).</li>
 * </ol>
 *
 * <p>
 * The bridge is thread-safe: concurrent invocations are correlated by Uuid.
 * Timeout → {@link OperationDelegationTimeoutException} (HTTP 504).
 *
 * @author aas-camel-dmp
 */
public class MqttRequestReplyBridge {

	private static final Logger LOGGER = LoggerFactory.getLogger(MqttRequestReplyBridge.class);

	private final ConcurrentMap<String, CompletableFuture<Object>> pending = new ConcurrentHashMap<>();
	private final Gson gson = new Gson();

	private volatile ProducerTemplate producerTemplate;

	public void init(CamelContext camelContext) {
		this.producerTemplate = camelContext.createProducerTemplate();
	}

	/**
	 * Delegate one operation invocation end-to-end. Blocks until the correlated
	 * reply arrives or the timeout elapses.
	 *
	 * @param ctx  resolved endpoints of the operation route
	 * @param body the raw HTTP request body (JSON, e.g. the AAS OperationVariables)
	 * @return the JSON response body for the REST caller
	 */
	public Object invoke(OperationContext ctx, Object body) {
		ensureInitialized();

		String uuid = UUID.randomUUID().toString();

		// 1. request-direction Lua transform (REST -> MQTT command payload)
		Object transformed = producerTemplate.requestBody(ctx.requestLuaUri, body);

		// 2. build the native command message: unwrap the SinkId (action) key and
		//    add the correlation Uuid (the native stations expect a flat payload)
		Map<String, Object> command = unwrap(transformed, ctx.action);
		command.put("Uuid", uuid);

		// 3. register the pending future BEFORE publishing (no missed replies)
		CompletableFuture<Object> future = new CompletableFuture<>();
		pending.put(uuid, future);
		try {
			producerTemplate.sendBody(ctx.commandSinkUri, gson.toJson(command));
		} catch (RuntimeException e) {
			pending.remove(uuid);
			throw e;
		}

		// 4. await the correlated reply
		Object response;
		try {
			response = future.get(ctx.timeoutSeconds, TimeUnit.SECONDS);
		} catch (TimeoutException e) {
			pending.remove(uuid);
			throw new OperationDelegationTimeoutException(uuid, ctx.action, ctx.timeoutSeconds);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			pending.remove(uuid);
			throw new RuntimeException("interrupted while awaiting operation '" + ctx.action + "'", e);
		} catch (ExecutionException e) {
			pending.remove(uuid);
			throw new RuntimeException("operation '" + ctx.action + "' failed", e.getCause());
		}

		// 5. response-direction Lua transform (native MQTT response -> REST), then
		//    unwrap the SinkId (action) key for the caller
		Object result = producerTemplate.requestBody(ctx.responseLuaUri, response);
		return unwrap(result, ctx.action);
	}

	/**
	 * Called by the paho response-topic consumer. Correlates the reply by its
	 * {@code Uuid} field and completes the matching pending invocation.
	 */
	public void onResponse(Object body) {
		String json = body instanceof byte[]
				? new String((byte[]) body, StandardCharsets.UTF_8)
				: String.valueOf(body);
		String uuid = extractUuid(json);
		if (uuid == null || uuid.isBlank()) {
			LOGGER.warn("response on operation topic without a Uuid, ignored: {}", json);
			return;
		}
		CompletableFuture<Object> future = pending.remove(uuid);
		if (future != null) {
			future.complete(json);
			LOGGER.debug("correlated response for Uuid {}", uuid);
		} else {
			LOGGER.debug("no pending invocation for Uuid {} (late/duplicate reply)", uuid);
		}
	}

	private void ensureInitialized() {
		if (producerTemplate == null) {
			throw new IllegalStateException("MqttRequestReplyBridge not initialized (missing CamelContext)");
		}
	}

	/**
	 * Lua blobs return {@code { <SinkId>: <payload> }} (AIMC contract). Unwrap
	 * the action key; if absent (or not JSON), fall back to the payload as-is.
	 */
	private Map<String, Object> unwrap(Object payload, String action) {
		if (payload instanceof Map) {
			@SuppressWarnings("unchecked")
			Object inner = ((Map<String, Object>) payload).get(action);
			if (inner instanceof Map) {
				return new HashMap<>((Map<String, Object>) inner);
			}
			return new HashMap<>((Map<String, Object>) payload);
		}
		try {
			Type type = new TypeToken<Map<String, Object>>() { }.getType();
			Map<String, Object> map = gson.fromJson(String.valueOf(payload), type);
			if (map == null) {
				return new HashMap<>();
			}
			Object inner = map.get(action);
			if (inner instanceof Map) {
				return new HashMap<>((Map<String, Object>) inner);
			}
			return new HashMap<>(map);
		} catch (JsonSyntaxException e) {
			LOGGER.warn("operation '{}': unable to parse transformed payload, "
					+ "publishing/replying as-is", action);
			return new HashMap<>();
		}
	}

	private String extractUuid(String json) {
		try {
			Type type = new TypeToken<Map<String, Object>>() { }.getType();
			Map<String, Object> map = gson.fromJson(json, type);
			Object uuid = map == null ? null : map.get("Uuid");
			return uuid == null ? null : String.valueOf(uuid);
		} catch (JsonSyntaxException e) {
			return null;
		}
	}
}
