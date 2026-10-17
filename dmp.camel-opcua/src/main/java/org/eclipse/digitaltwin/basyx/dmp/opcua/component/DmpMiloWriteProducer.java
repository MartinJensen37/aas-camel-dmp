/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import java.util.List;
import java.util.Map;

import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.component.milo.client.MiloClientProducer;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.QualifiedName;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;

/**
 * A Write that waits for the server's answer and fails on a Bad status. camel-milo 4.22
 * either does not wait for a write or, when asked to ({@code await}), puts the returned
 * StatusCode in the body without looking at it, so a rejected write (e.g.
 * {@code Bad_NotWritable}, {@code Bad_TypeMismatch}) goes unnoticed.
 *
 * <p>A scalar value is written as the variable's built-in DataType (read once from the
 * server): decoded JSON numbers are Java longs and doubles, which an Int32 or Float
 * variable rejects. The body keeps the written value.
 */
final class DmpMiloWriteProducer extends MiloClientProducer {
	private final DmpMiloClientEndpoint endpoint;
	private MiloClientConnection connection;
	private volatile NodeId dataType;

	DmpMiloWriteProducer(DmpMiloClientEndpoint endpoint) {
		super(endpoint, true);
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
	public boolean process(Exchange exchange, AsyncCallback callback) {
		Object written = exchange.getMessage().getBody();
		try {
			exchange.getMessage().setBody(typed(written));
		} catch (ConnectorInvocationException invalid) {
			exchange.setException(invalid);
			callback.done(true);
			return true;
		}
		return super.process(exchange, done -> {
			Object answer = exchange.getMessage().getBody();
			if (exchange.getException() == null && answer instanceof StatusCode status && !status.isGood()) {
				exchange.setException(new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
						"OPCUA_WRITE_REJECTED", "OPC UA server rejected the write",
						Map.of("status", Long.toHexString(status.getValue()))));
			} else if (exchange.getException() != null && !(exchange.getException() instanceof ConnectorInvocationException)) {
				exchange.setException(new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
						"OPCUA_WRITE_FAILED", "OPC UA write failed", Map.of()));
			}
			exchange.getMessage().setBody(written);
			callback.done(done);
		});
	}

	/** The value as a Variant of the variable's built-in DataType; other values pass unchanged. */
	private Object typed(Object value) {
		if (!(value instanceof Number || value instanceof Boolean || value instanceof String)) return value;
		NodeId type = dataType();
		if (type == null || type.getNamespaceIndex().intValue() != 0 || !(type.getIdentifier() instanceof Number id)) return value;
		try {
			Object converted = switch (id.intValue()) {
				case 1 -> value instanceof Boolean flag ? flag : value;
				case 2 -> number(value) == null ? value : (byte) Math.toIntExact(range(number(value).longValue(), Byte.MIN_VALUE, Byte.MAX_VALUE));
				case 3 -> number(value) == null ? value : Unsigned.ubyte(range(number(value).longValue(), 0, 0xFF));
				case 4 -> number(value) == null ? value : (short) range(number(value).longValue(), Short.MIN_VALUE, Short.MAX_VALUE);
				case 5 -> number(value) == null ? value : Unsigned.ushort((int) range(number(value).longValue(), 0, 0xFFFF));
				case 6 -> number(value) == null ? value : Math.toIntExact(number(value).longValue());
				case 7 -> number(value) == null ? value : Unsigned.uint(range(number(value).longValue(), 0, 0xFFFFFFFFL));
				case 8 -> number(value) == null ? value : number(value).longValue();
				case 9 -> number(value) == null ? value : Unsigned.ulong(range(number(value).longValue(), 0, Long.MAX_VALUE));
				case 10 -> number(value) == null ? value : number(value).floatValue();
				case 11 -> number(value) == null ? value : number(value).doubleValue();
				case 12 -> String.valueOf(value);
				default -> value;
			};
			return new Variant(converted);
		} catch (ArithmeticException outOfRange) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE,
					"OPCUA_VALUE_OUT_OF_RANGE", "value does not fit the variable's DataType", Map.of());
		}
	}

	private static Number number(Object value) {
		if (!(value instanceof Number number)) return null;
		if ((number instanceof Double || number instanceof Float) && number.doubleValue() != Math.rint(number.doubleValue())) return null;
		return number;
	}

	private static long range(long value, long min, long max) {
		if (value < min || value > max) throw new ArithmeticException("out of range");
		return value;
	}

	/** The variable's DataType, read once; {@code null} while the client is not connected. */
	private NodeId dataType() {
		NodeId known = dataType;
		if (known != null) return known;
		OpcUaClient client = connection == null ? null : connection.getOpcUaClient();
		if (client == null) return null;
		try {
			NodeId node = ExpandedNodeId.parse(endpoint.getNode()).toNodeIdOrThrow(client.getNamespaceTable());
			DataValue read = client.read(0, TimestampsToReturn.Neither, List.of(new ReadValueId(node,
					AttributeId.DataType.uid(), null, QualifiedName.NULL_VALUE))).getResults()[0];
			if (read.getValue() != null && read.getValue().getValue() instanceof NodeId type) dataType = type;
		} catch (Exception unknown) {
			return null;
		}
		return dataType;
	}
}
