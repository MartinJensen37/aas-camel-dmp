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
package org.eclipse.digitaltwin.basyx.dmp.aas.operation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.Processor;
import org.apache.camel.component.seda.SedaEndpoint;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.ConnectorInvocationException;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.TransformerInvocation;

/**
 * The status phase of an operation mapping (DMP PLAN P3) as route steps: the
 * bridge route takes a baseline, then loops — evaluate the status
 * transformation after each sample, wait for the next sample — until the
 * transformation reports {@code done} or {@code failed}, or the StatusTimeout
 * expires.
 *
 * <p>Three kinds of status source:
 * <ul>
 * <li><b>read</b> properties are read by a Content Enricher before the trigger
 * ({@code baseline}) and again when their polling interval is due;</li>
 * <li><b>observed</b> properties follow a subscription route that lives as
 * long as the mapping; their latest value is the baseline;</li>
 * <li><b>events</b> have no current value; only events after the baseline
 * count.</li>
 * </ul>
 * Subscription routes publish samples to one SEDA queue per mapping, which the
 * loop consumes with a Polling Consumer ({@code pollEnrich}); its timeout is
 * the poll slice, after which due reads are made. The Throttle in front of the
 * phase admits one invocation per action, so the queue has one consumer.
 */
final class StatusPhase {
	enum Acquisition { READ, OBSERVE, EVENT }

	/**
	 * One status source; {@code readUri} is the Read route of a READ source, null otherwise.
	 * {@code initialValue}: the subscription always delivers the current value first (an OPC UA
	 * monitored item), so the baseline waits for it.
	 */
	record Source(String sourceId, String dataSourceId, Acquisition acquisition, String readUri, long intervalMs,
			boolean initialValue) {
		Source(String sourceId, String dataSourceId, Acquisition acquisition, String readUri, long intervalMs) {
			this(sourceId, dataSourceId, acquisition, readUri, intervalMs, false);
		}
	}

	/** A subscribed value or event, as queued for the waiting invocation. */
	record Sample(String sourceId, Object value) { }

	/** The state of one invocation's status phase. */
	static final class Run {
		final Map<String, Object> baseline = new LinkedHashMap<>();
		final Map<String, Object> current = new LinkedHashMap<>();
		final long[] due;
		long begin;
		long deadline;
		boolean sampled;
		String state = RUNNING;
		String reason;

		Run(int sources) { due = new long[sources]; }

		boolean started() { return begin != 0; }
	}

	static final String RUNNING = "running", DONE = "done", FAILED = "failed";
	static final String RUN_PROPERTY = "dmp.status.run";
	private static final long MAX_POLL_SLICE_MS = 250;

	private final List<Source> sources;
	private final long timeoutMs;
	private final String queueUri;
	private final long pollSliceMs;
	private final Map<String, Object> observed = new ConcurrentHashMap<>();
	private volatile boolean closed;

	StatusPhase(List<Source> sources, long timeoutMs, String queueUri) {
		this.sources = List.copyOf(sources);
		this.timeoutMs = timeoutMs;
		this.queueUri = queueUri;
		this.pollSliceMs = this.sources.stream().filter(source -> source.acquisition() == Acquisition.READ)
				.mapToLong(Source::intervalMs).min().orElse(MAX_POLL_SLICE_MS);
	}

	List<Source> sources() { return sources; }

	String queueUri() { return queueUri; }

	/** How long one wait for a subscribed sample lasts before due reads and the deadline are checked. */
	long pollSliceMs() { return Math.max(1, Math.min(pollSliceMs, MAX_POLL_SLICE_MS)); }

	/** Ends running phases: their callers receive a transport error instead of waiting out the timeout. */
	void close() { closed = true; }

	/** Last step of a subscription route: keeps an observed value and turns the message into a sample. */
	Processor observer(Source source) {
		return exchange -> {
			Object value = exchange.getMessage().getBody();
			if (source.acquisition() == Acquisition.OBSERVE) {
				if (value == null) observed.remove(source.sourceId());
				else observed.put(source.sourceId(), value);
			}
			exchange.getMessage().setBody(new Sample(source.sourceId(), value));
		};
	}

	private static final String INITIAL_WAIT_PROPERTY = "dmp.status.initialWaitDeadline";

	/** True when a subscription guarantees an initial value the baseline must wait for. */
	boolean waitsForInitialValues() {
		return sources.stream().anyMatch(Source::initialValue);
	}

	/** Starts the wait for initial values (bounded by the StatusTimeout; nothing has been sent yet). */
	void openInitialWait(Exchange exchange) {
		exchange.setProperty(INITIAL_WAIT_PROPERTY, System.nanoTime() + timeoutMs * 1_000_000L);
	}

	/** Loop predicate: some subscription has not delivered its initial value yet. */
	Predicate awaitingInitialValues() {
		return exchange -> sources.stream().anyMatch(source -> source.initialValue()
				&& !observed.containsKey(source.sourceId()));
	}

	/** Inside the wait: a stopped mapping or an expired StatusTimeout ends it before any command. */
	void checkInitialWait(Exchange exchange) {
		if (closed) throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
				"DMP_STATUS_ABORTED", "status phase ended because the mapping was stopped", Map.of());
		Long deadline = exchange.getProperty(INITIAL_WAIT_PROPERTY, Long.class);
		if (deadline != null && System.nanoTime() >= deadline) throw new ConnectorInvocationException(
				ConnectorInvocationException.Category.TIMEOUT, "DMP_STATUS_TIMEOUT",
				"a status subscription delivered no initial value within the StatusTimeout", Map.of());
	}

	/** Opens the phase before the trigger: drops queued samples and takes the observed baseline. */
	void start(Exchange exchange) {
		exchange.getContext().getEndpoint(queueUri, SedaEndpoint.class).purgeQueue();
		Run run = new Run(sources.size());
		for (Source source : sources) if (source.acquisition() == Acquisition.OBSERVE) {
			Object value = observed.get(source.sourceId());
			run.baseline.put(source.sourceId(), value);
			run.current.put(source.sourceId(), value);
		}
		exchange.setProperty(RUN_PROPERTY, run);
	}

	/** Enricher strategy for a read source: before the trigger it is the baseline, afterwards a sample. */
	AggregationStrategy reading(int index) {
		Source source = sources.get(index);
		return (original, read) -> {
			Run run = run(original);
			Object value = read.getMessage().getBody();
			if (!run.started()) run.baseline.put(source.sourceId(), value);
			else run.sampled = true;
			run.current.put(source.sourceId(), value);
			run.due[index] = System.nanoTime() + source.intervalMs() * 1_000_000L;
			return original;
		};
	}

	/** Starts the clock once the command has been sent; the first check sees only the command's reply. */
	void triggered(Exchange exchange) {
		Run run = run(exchange);
		run.begin = System.nanoTime();
		run.deadline = run.begin + timeoutMs * 1_000_000L;
		for (int i = 0; i < sources.size(); i++)
			run.due[i] = sources.get(i).acquisition() == Acquisition.READ
					? run.begin + sources.get(i).intervalMs() * 1_000_000L : Long.MAX_VALUE;
		run.sampled = true;
	}

	Predicate running() { return exchange -> RUNNING.equals(run(exchange).state); }

	Predicate sampled() { return exchange -> run(exchange).sampled; }

	Predicate due(int index) { return exchange -> System.nanoTime() >= run(exchange).due[index]; }

	/** Body and invocation of one status check. */
	void prepareCheck(Exchange exchange, Object inputs, Object reply) {
		Run run = run(exchange);
		run.sampled = false;
		Map<String, Object> invocation = new LinkedHashMap<>();
		invocation.put("baseline", new LinkedHashMap<>(run.baseline));
		invocation.put("elapsed", (System.nanoTime() - run.begin) / 1e9);
		invocation.put("inputs", inputs);
		invocation.put("reply", reply);
		exchange.setProperty(TransformerInvocation.ARGUMENT_PROPERTY, invocation);
		exchange.setProperty(TransformerInvocation.STATUS_CHECK_PROPERTY, true);
		exchange.getMessage().setBody(new LinkedHashMap<>(run.current));
	}

	/** Reads the status check's answer: {@code "running"}, {@code "done"}, {@code "failed"} or {@code { state, reason }}. */
	void interpret(Exchange exchange) {
		Run run = run(exchange);
		exchange.removeProperty(TransformerInvocation.STATUS_CHECK_PROPERTY);
		exchange.removeProperty(TransformerInvocation.ARGUMENT_PROPERTY);
		Object answer = exchange.getMessage().getBody();
		Object state = answer instanceof Map<?, ?> table ? table.get("state") : answer;
		if (!(state instanceof String text) || !List.of(RUNNING, DONE, FAILED).contains(text))
			throw new OperationValidationException("DMP_RESPONSE_MAPPING_FAILED", 500,
					"Status transformation must answer running, done or failed", List.of());
		run.state = text;
		if (answer instanceof Map<?, ?> table && table.get("reason") != null) run.reason = String.valueOf(table.get("reason"));
	}

	/** Before each wait: a stopped mapping or an expired StatusTimeout ends the phase. */
	void checkDeadline(Exchange exchange) {
		if (closed) throw new ConnectorInvocationException(ConnectorInvocationException.Category.TRANSPORT,
				"DMP_STATUS_ABORTED", "status phase ended because the mapping was stopped", Map.of());
		if (System.nanoTime() >= run(exchange).deadline) throw new ConnectorInvocationException(
				ConnectorInvocationException.Category.TIMEOUT, "DMP_STATUS_TIMEOUT",
				"action did not finish within its StatusTimeout", Map.of());
	}

	/** Polling Consumer strategy: a queued sample, if one arrived within the poll slice. */
	AggregationStrategy arrival() {
		return (original, polled) -> {
			if (polled != null && polled.getMessage().getBody() instanceof Sample sample) {
				Run run = run(original);
				run.current.put(sample.sourceId(), sample.value());
				run.sampled = true;
			}
			return original;
		};
	}

	static Run run(Exchange exchange) {
		Run run = exchange.getProperty(RUN_PROPERTY, Run.class);
		if (run == null) throw new IllegalStateException("operation exchange has no status phase");
		return run;
	}
}
