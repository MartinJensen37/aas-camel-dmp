/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.opcua.component;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.milo.client.MiloClientConnection;
import org.apache.camel.support.DefaultConsumer;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.ExpandedNodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Observes one variable (OPC 10101 {@code observeproperty}) with a subscription of its own: the
 * endpoint's {@code requestedPublishingInterval} and {@code samplingInterval} apply to this
 * variable, and the monitored item keeps a queue of samples so a short state between two
 * publishes (e.g. a program's Running) is still delivered. camel-milo shares one subscription
 * per server connection, created with the first endpoint's publishing interval, and keeps only
 * the latest sample. Each message body is the sampled {@code DataValue}.
 *
 * <p>The subscription is made on the endpoint's shared connection and made again whenever that
 * connection's client changes (first connect, reconnect).
 */
final class DmpMiloSubscriptionConsumer extends DefaultConsumer {
	private static final Logger LOGGER = LoggerFactory.getLogger(DmpMiloSubscriptionConsumer.class);
	private static final long WATCH_PERIOD_MS = 500;
	private static final double DEFAULT_PUBLISHING_INTERVAL_MS = 1000;
	private static final UInteger QUEUE_SIZE = UInteger.valueOf(16);

	private final DmpMiloClientEndpoint endpoint;
	private MiloClientConnection connection;
	private ScheduledExecutorService watchdog;
	private ScheduledFuture<?> watch;
	private volatile OpcUaClient subscribedClient;
	private volatile OpcUaSubscription subscription;

	DmpMiloSubscriptionConsumer(DmpMiloClientEndpoint endpoint, Processor processor) {
		super(endpoint, processor);
		this.endpoint = endpoint;
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		connection = endpoint.createConnection();
		watchdog = endpoint.getCamelContext().getExecutorServiceManager()
				.newSingleThreadScheduledExecutor(this, "dmp-milo-subscription");
		watch = watchdog.scheduleWithFixedDelay(this::ensureSubscription, 0, WATCH_PERIOD_MS, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doStop() throws Exception {
		if (watch != null) watch.cancel(false);
		if (watchdog != null) endpoint.getCamelContext().getExecutorServiceManager().shutdownNow(watchdog);
		deleteSubscription();
		if (connection != null) endpoint.releaseConnection(connection);
		connection = null;
		super.doStop();
	}

	/** (Re)creates the subscription when the shared connection has a client this consumer is not subscribed with. */
	private void ensureSubscription() {
		try {
			OpcUaClient client = connection == null ? null : connection.getOpcUaClient();
			if (client == null || client == subscribedClient) return;
			deleteSubscription();
			Double publishing = endpoint.getConfiguration().getRequestedPublishingInterval();
			OpcUaSubscription created = new OpcUaSubscription(client,
					publishing == null ? DEFAULT_PUBLISHING_INTERVAL_MS : publishing);
			created.create();
			OpcUaMonitoredItem item = OpcUaMonitoredItem.newDataItem(
					ExpandedNodeId.parse(endpoint.getNode()).toNodeIdOrThrow(client.getNamespaceTable()));
			Double sampling = endpoint.getSamplingInterval();
			if (sampling != null) item.setSamplingInterval(sampling);
			item.setQueueSize(QUEUE_SIZE);
			item.setDiscardOldest(true);
			item.setDataValueListener((monitored, value) -> deliver(value));
			created.addMonitoredItem(item);
			created.synchronizeMonitoredItems();
			subscription = created;
			subscribedClient = client;
		} catch (Exception failure) {
			LOGGER.warn("OPC UA subscription to {} is not established yet: {}", endpoint.getNode(), failure.getMessage());
			deleteSubscription();
		}
	}

	private void deliver(Object value) {
		Exchange exchange = createExchange(true);
		exchange.getMessage().setBody(value);
		try {
			getProcessor().process(exchange);
		} catch (Exception failure) {
			exchange.setException(failure);
		} finally {
			if (exchange.getException() != null)
				getExceptionHandler().handleException("OPC UA sample processing failed", exchange, exchange.getException());
			releaseExchange(exchange, true);
		}
	}

	private void deleteSubscription() {
		OpcUaSubscription current = subscription;
		subscription = null;
		subscribedClient = null;
		if (current == null) return;
		try { current.delete(); }
		catch (Exception ignored) { /* the session may already be gone */ }
	}
}
