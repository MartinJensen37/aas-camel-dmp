/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.Exchange;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.structured.CallMethodRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.CallResponse;

/**
 * One UA {@code Call} of the endpoint's Method on its owning Object. The body
 * is the positional input arguments ({@code Variant[]}); the reply body is the
 * {@code CallMethodResult}. Its status is the caller's to judge; a Call that
 * cannot be made or times out raises a {@link ConnectorInvocationException}.
 */
public final class DmpMiloMethodProducer extends DefaultProducer {
	/** Optional header: how long to wait for the Call, in milliseconds (default 30 s). */
	public static final String CALL_TIMEOUT_HEADER = "DmpOpcUaCallTimeout";
	private static final long DEFAULT_TIMEOUT_MS = 30_000;
	private final DmpMiloClientEndpoint endpoint;
	private MiloClientConnection connection;

	DmpMiloMethodProducer(DmpMiloClientEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		connection = endpoint.createConnection();
	}

	@Override
	protected void doStop() throws Exception {
		if (connection != null) endpoint.releaseConnection(connection);
		connection = null;
		super.doStop();
	}

	@Override
	public void process(Exchange exchange) {
		Variant[] arguments = arguments(exchange.getMessage().getBody());
		Long timeout = exchange.getMessage().getHeader(CALL_TIMEOUT_HEADER, Long.class);
		long timeoutMs = timeout == null || timeout <= 0 ? DEFAULT_TIMEOUT_MS : timeout;
		try {
			var client = connection == null ? null : connection.getOpcUaClient();
			if (client == null) throw new IllegalStateException("OPC UA client is not connected");
			var namespaces = client.getNamespaceTable();
			var object = ExpandedNodeId.parse(endpoint.getNode()).toNodeIdOrThrow(namespaces);
			var method = ExpandedNodeId.parse(endpoint.getMethod()).toNodeIdOrThrow(namespaces);
			CallResponse response = client.callAsync(List.of(new CallMethodRequest(object, method, arguments)))
					.get(timeoutMs, TimeUnit.MILLISECONDS);
			if (response == null || response.getResults() == null || response.getResults().length != 1)
				throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"METHOD_RESULT_INVALID", "OPC UA Method returned an unreadable result", Map.of());
			exchange.getMessage().setBody(response.getResults()[0]);
		} catch (TimeoutException timeoutFailure) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT,
					"METHOD_CALL_TIMEOUT", "OPC UA Method invocation timed out", Map.of("timeoutMillis", timeoutMs));
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw transport(interrupted);
		} catch (ConnectorInvocationException typed) {
			throw typed;
		} catch (ExecutionException | RuntimeException | org.eclipse.milo.opcua.stack.core.UaException failure) {
			throw transport(failure);
		}
	}

	private static Variant[] arguments(Object body) {
		if (body == null) return new Variant[0];
		if (body instanceof Variant[] variants) return variants;
		if (body instanceof List<?> values) return values.stream()
				.map(value -> value instanceof Variant variant ? variant : new Variant(value)).toArray(Variant[]::new);
		throw new ConnectorInvocationException(ConnectorInvocationException.Category.CONFIGURATION,
				"METHOD_ARGUMENTS_INVALID", "OPC UA Method arguments must be Variant[] or a list", Map.of());
	}

	private static ConnectorInvocationException transport(Exception failure) {
		return new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
				"METHOD_CALL_FAILED", "OPC UA Method invocation failed", Map.of("cause", String.valueOf(failure.getMessage())));
	}
}
