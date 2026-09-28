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

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Synchronous operation invocation for protocols whose command leg answers
 * inline (DMP v3, ADR-022): HTTP — the HTTP response IS the reply — and
 * write-style protocols (OPC UA node write, Modbus/plc4x write, ack-only).
 *
 * <p>
 * Flow per invocation:
 * <ol>
 * <li>run the request-direction Lua blob (REST operation variables → native
 * command payload), keyed by the action;</li>
 * <li>unwrap the SinkId (action) key and invoke the command sink
 * synchronously (the declared HTTP method rides the Camel header);</li>
 * <li>run the response-direction Lua blob over the reply and unwrap the
 * action key for the caller (one-MC authoring: identity blob → pass
 * through). Write-style protocols reply with a bare ack.</li>
 * </ol>
 *
 * @author aas-camel-dmp
 */
public class SyncOperationProcessor implements Processor {

	private static final Logger LOGGER = LoggerFactory.getLogger(SyncOperationProcessor.class);
	private static final Gson GSON = new Gson();
	/** Camel-http producer: override the HTTP method per exchange. */
	public static final String CAMEL_HTTP_METHOD = "CamelHttpMethod";
	/** camel-milo: await the write/method-call completion (sync legs). */
	public static final String CAMEL_MILO_AWAIT = "CamelMiloAwait";
	/** camel-milo: the node-id list for the batched READ path. */
	public static final String CAMEL_MILO_NODE_IDS = "CamelMiloNodeIds";
	private static final java.lang.reflect.Type MAP_TYPE =
			new com.google.gson.reflect.TypeToken<Map<String, Object>>() { }.getType();

	private final OperationContext context;
	private final ProducerTemplate producerTemplate;

	public SyncOperationProcessor(CamelContext camelContext, OperationContext context) {
		this.context = context;
		this.producerTemplate = camelContext.createProducerTemplate();
	}

	@Override
	public void process(Exchange exchange) {
		// camel HTTP delivers the body as a stream — force the String view so
		// the delegation detection and the Lua transform see the text
		String requestBody = exchange.getMessage().getBody(String.class);
		// the BaSyx delegation client POSTs an OperationVariable[] (a JSON
		// ARRAY) body and expects the reply in the same shape; direct REST
		// callers POST an object
		boolean basyxDelegation = OperationPayloads.isJsonArrayBody(requestBody);
		if (basyxDelegation) {
			// flatten [ {value:{idShort: Uuid, value: "123"}} ] -> {"Uuid":
			// "123"} before the request Lua — the raw array defeats the Lua
			// field lookups, so the caller's correlation Uuid would be lost
			// and a fresh one minted, breaking the station's occupation gating
			requestBody = OperationPayloads.flattenOperationVariables(requestBody);
		}
		try {
			// 1. request-direction Lua transform (REST -> native command payload)
			Object transformed = producerTemplate.requestBody(
					context.requestLuaUri, requestBody);

			// 2. unwrap the SinkId (action) key and invoke the command sink
			//    synchronously; the declared HTTP method rides the Camel header
			Map<String, Object> command = OperationPayloads.unwrap(transformed, context.action);
			// parse every DECLARED parameter per its AID DataSchema type —
			// the wire representation is data-driven, never name-based
			OperationPayloads.coerceFields(command, context.parameterTypes);
			if (command.containsKey("Uuid") || context.parameterTypes.containsKey("Uuid")) {
				String correlationUuid = OperationPayloads.uuidString(command.get("Uuid"));
				if (correlationUuid != null) {
					command.put("Uuid", correlationUuid);
				}
			}
			LOGGER.debug("sync '{}': lua out={} command={} -> sink {}",
					context.action, transformed, command, context.commandSinkUri);
			// CamelMiloAwait: camel-milo completes the exchange only when the
			// header is true — without it the reply is the unmodified input
			// and the write/method call is fire-and-forget
			Map<String, Object> sinkHeaders = new java.util.HashMap<>();
			sinkHeaders.put(CAMEL_HTTP_METHOD, context.restMethod);
			sinkHeaders.put(CAMEL_MILO_AWAIT, Boolean.TRUE);
			Object reply = producerTemplate.requestBodyAndHeaders(
					context.commandSinkUri, commandBody(command), sinkHeaders);
			LOGGER.debug("sync '{}': reply type={} value={}", context.action,
					reply == null ? "null" : reply.getClass().getName(), reply);

			// 3. response-direction handling: the awaited UA Call carries the
			//    CallMethodResult — decode it positionally against the DECLARED
			//    response types. A response Lua runs when authored;
			//    one-MC authoring passes replies through.
			Object body;
			if ("opcua".equalsIgnoreCase(context.protocol)) {
				Map<String, Object> decoded = decodeMethodResult(reply);
				if (decoded != null) {
					body = GSON.toJson(Map.of(context.action, decoded));
				} else {
					body = reply == null ? "{}" : String.valueOf(reply);
				}
			} else if (context.responseLuaUri != null) {
				Object result = producerTemplate.requestBody(context.responseLuaUri, reply);
				body = GSON.toJson(OperationPayloads.unwrap(result, context.action));
			} else {
				body = reply == null ? "{}" : String.valueOf(reply);
			}
			if (basyxDelegation) {
				Map<String, Object> replyMap = GSON.fromJson(
						String.valueOf(body), MAP_TYPE);
				if (replyMap != null && replyMap.size() == 1
						&& replyMap.containsKey(context.action)
						&& replyMap.get(context.action) instanceof Map) {
					replyMap = GSON.fromJson(GSON.toJson(
							replyMap.values().iterator().next()), MAP_TYPE);
				}
				if (replyMap != null) {
					body = GSON.toJson(OperationPayloads.toOperationVariables(
							replyMap, context.responseTypes));
				}
			}
			exchange.getMessage().setBody(body);
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_OK);
		} catch (RuntimeException e) {
			LOGGER.error("synchronous operation '{}' failed", context.action, e);
			exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, HttpStatus.SC_INTERNAL_SERVER_ERROR);
			exchange.getMessage().setBody("{\"error\": \"" + e.getMessage() + "\"}");
		}
	}

	/** UA status code → good: a "Good" name or the numeric 0 (Good). */
	private static boolean isGoodStatusCode(Object code) {
		if (code == null) {
			return false;
		}
		if (code.toString().equalsIgnoreCase("Good")) {
			return true;
		}
		return code instanceof Number number && number.longValue() == 0;
	}

	/**
	 * Decodes the awaited command-sink reply into the reply map of the caller
	 * envelope {@code {"<action>": {...}}}:
	 * <ul>
	 * <li>a {@link CallMethodResult}-shaped body (registered canonical
	 * converter, no Milo dependency here): a BAD transport status maps to a
	 * FAILURE state; otherwise the POSITIONAL output arguments zip with the
	 * DECLARED response types order (State, Outcome) — undeclared positions
	 * ride as {@code arg<i>};</li>
	 * <li>a WriteResponse-shaped body decodes to a bare status ack;</li>
	 * <li>{@code null} when no declared reply payload can be decoded.</li>
	 * </ul>
	 */
	private Map<String, Object> decodeMethodResult(Object reply) {
		if (reply == null) {
			// An empty body is a legitimate no-payload result.
			LOGGER.debug("sync '{}': no awaited reply body", context.action);
			return null;
		}
		Map<String, Object> canonical;
		try {
			canonical = producerTemplate.getCamelContext().getTypeConverter()
					.convertTo(Map.class, reply);
		} catch (RuntimeException e) {
			LOGGER.info("sync '{}': canonical method-result conversion failed: {}",
					context.action, e.getMessage());
			return null;
		}
		if (canonical == null || canonical == reply) {
			LOGGER.info("sync '{}': awaited body {} did not decode canonically",
					context.action, reply.getClass().getName());
			return null;
		}
		Object status = canonical.get("statusCode");
		if (status instanceof Map<?, ?> statusEntry
				&& Boolean.FALSE.equals(statusEntry.get("good"))) {
			LOGGER.warn("sync '{}': UA Call transport status bad ({})",
					context.action, statusEntry.get("value"));
			return Map.of("State", "FAILURE");
		}
		Object outputs = canonical.get("outputArguments");
		if (!(outputs instanceof java.util.List<?> arguments) || arguments.isEmpty()) {
			// an ack-only result: the write-style status codes
			Object results = canonical.get("results");
			if (results instanceof java.util.List<?> statuses && !statuses.isEmpty()) {
				boolean good = statuses.stream()
						.allMatch(SyncOperationProcessor::isGoodStatusCode);
				return Map.of("State", good ? "SUCCESS" : "FAILURE");
			}
			// No output arguments means the protocol returned no payload.
			return null;
		}
		// zip positionally with the DECLARED output order: the CCI operation's
		// output_variable order is the positional contract of the method's
		// output arguments (the responseTypes map carries TYPES, not the
		// method-signature order). Without a positional contract the mapping is
		// ambiguous and must not depend on JSON/map iteration order.
		java.util.List<String> names = context.responseOrder;
		if (names.size() != arguments.size()) {
			LOGGER.info("sync '{}': {} awaited output arguments vs {} declared reply "
					+ "fields — positional zip is ambiguous",
					context.action, arguments.size(), names.size());
			return null;
		}
		Map<String, Object> replyMap = new java.util.LinkedHashMap<>();
		for (int i = 0; i < arguments.size(); i++) {
			replyMap.put(names.get(i), arguments.get(i));
		}
		return replyMap;
	}

	/**
	 * The body for the command sink: protocol-aware encoding, shared with the
	 * write-delegation processor (see {@link OperationPayloads#sinkBody}).
	 */
	private Object commandBody(Map<String, Object> command) {
		return OperationPayloads.sinkBody(context.protocol, command);
	}

}
