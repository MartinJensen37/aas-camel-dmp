/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.ServiceStatus;
import dk.aau.smartproductionlab.dmp.aas.event.AasEventHub;

/**
 * Runs a value-feed route only while event-sourced AAS Property routes listen on the hub. Stopping waits a grace
 * period, so replacing the last listening bundle does not restart the consumer.
 */
final class ValueFeedSwitch {
	private final CamelContext context;
	private final String routeId;
	private final Duration grace;
	private final ScheduledExecutorService executor;
	private ScheduledFuture<?> pendingStop;
	private volatile boolean wanted;

	ValueFeedSwitch(CamelContext context, String routeId, AasEventHub hub, Duration grace) {
		this.context = context;
		this.routeId = routeId;
		this.grace = grace;
		this.executor = context.getExecutorServiceManager().newSingleThreadScheduledExecutor(this, "dmp-value-feed");
		hub.onPresenceChanged(present -> executor.execute(() -> changed(present)));
	}

	private void changed(boolean present) {
		wanted = present;
		if (pendingStop != null) { pendingStop.cancel(false); pendingStop = null; }
		if (present) apply();
		else pendingStop = executor.schedule(this::apply, grace.toMillis(), TimeUnit.MILLISECONDS);
	}

	/** Brings the route to the latest wanted state; retries until the route exists or the context stops. */
	private void apply() {
		if (context.isStopping() || context.isStopped()) return;
		// Routes are added right after their builder is configured; until then there is nothing to switch.
		if (context.getRoute(routeId) == null) { executor.schedule(this::apply, 200, TimeUnit.MILLISECONDS); return; }
		try {
			ServiceStatus status = context.getRouteController().getRouteStatus(routeId);
			if (wanted && status != ServiceStatus.Started) context.getRouteController().startRoute(routeId);
			if (!wanted && status == ServiceStatus.Started) context.getRouteController().stopRoute(routeId);
		} catch (Exception failure) {
			executor.schedule(this::apply, 1, TimeUnit.SECONDS);
		}
	}
}
