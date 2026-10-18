/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.snmp.component;

import java.io.IOException;
import java.util.Map;

import org.apache.camel.Consumer;
import org.apache.camel.PollingConsumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultEndpoint;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

/** One SNMP object on one agent, read with GET and written with SET over UDP. */
public final class DmpSnmpEndpoint extends DefaultEndpoint {
	public static final long DEFAULT_TIMEOUT_MS = 1500;

	private final String host;
	private final int port;
	private final OID oid;
	private final int version;
	private final String community;
	private final long timeout;
	private final int retries;
	private Snmp session;
	/** The object's SMI type, learned from its first read: a SET must send the same type. */
	private volatile Variable syntax;

	DmpSnmpEndpoint(String uri, DmpSnmpComponent component, String host, int port, String oid, int version,
			String community, long timeout, int retries) {
		super(uri, component);
		this.host = host;
		this.port = port;
		this.oid = new OID(oid);
		this.version = version;
		this.community = community;
		this.timeout = timeout;
		this.retries = retries;
	}

	@Override public Producer createProducer() { return new DmpSnmpProducer(this); }

	@Override public Consumer createConsumer(Processor processor) {
		throw new UnsupportedOperationException("dmp-snmp is read by polling (GET); traps are not consumed");
	}

	@Override public PollingConsumer createPollingConsumer() { return new DmpSnmpPollingConsumer(this); }

	/** The object's value as JSON (see {@link SnmpValues#json}). */
	Object read() {
		Variable value = request(PDU.GET, new VariableBinding(oid));
		syntax = value;
		return SnmpValues.json(value);
	}

	/** Sets the object to a JSON value, as the object's own SMI type; the agent's error status raises. */
	void write(Object value) {
		Variable template = syntax;
		if (template == null) {
			template = request(PDU.GET, new VariableBinding(oid));
			syntax = template;
		}
		Variable variable;
		try {
			variable = SnmpValues.variable(value, template);
		} catch (IllegalArgumentException mismatch) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "SNMP_VALUE_MISMATCH",
					"value does not fit the object's SNMP type", Map.of("type", template.getSyntaxString(), "cause", String.valueOf(mismatch.getMessage())));
		}
		request(PDU.SET, new VariableBinding(oid, variable));
	}

	private Variable request(int type, VariableBinding binding) {
		PDU pdu = new PDU();
		pdu.setType(type);
		pdu.add(binding);
		ResponseEvent<UdpAddress> event;
		try {
			event = session().send(pdu, target());
		} catch (IOException failure) {
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT, "SNMP_TRANSPORT_FAILED",
					"the SNMP request could not be sent", Map.of("cause", String.valueOf(failure.getMessage())));
		}
		PDU response = event == null ? null : event.getResponse();
		if (response == null)
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.TIMEOUT, "SNMP_TIMEOUT",
					"the SNMP agent did not answer in time", Map.of("timeoutMillis", timeout));
		if (response.getErrorStatus() != PDU.noError)
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "SNMP_ERROR",
					"the SNMP agent rejected the request", Map.of("status", response.getErrorStatusText()));
		if (response.size() != 1)
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "SNMP_RESPONSE_INVALID",
					"the SNMP agent answered with " + response.size() + " bindings", Map.of());
		Variable value = response.get(0).getVariable();
		if (value == null || value.isException())
			throw new ConnectorInvocationException(ConnectorInvocationException.Category.REMOTE, "SNMP_NO_SUCH_OBJECT",
					"the SNMP agent has no such object", Map.of("oid", oid.toDottedString(),
							"status", value == null ? "null" : value.getSyntaxString()));
		return value;
	}

	private CommunityTarget<UdpAddress> target() {
		CommunityTarget<UdpAddress> target = new CommunityTarget<>((UdpAddress) GenericAddress.parse("udp:" + host + "/" + port),
				new OctetString(community));
		target.setVersion(version);
		target.setTimeout(timeout);
		target.setRetries(retries);
		return target;
	}

	private synchronized Snmp session() throws IOException {
		if (session == null) {
			DefaultUdpTransportMapping transport = new DefaultUdpTransportMapping();
			session = new Snmp(transport);
			transport.listen();
		}
		return session;
	}

	@Override
	protected synchronized void doStop() throws Exception {
		if (session != null) session.close();
		session = null;
		super.doStop();
	}
}
