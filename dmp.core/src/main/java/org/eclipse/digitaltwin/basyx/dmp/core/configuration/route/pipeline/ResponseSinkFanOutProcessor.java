package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.apache.camel.spi.ThreadPoolProfile;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Queues Lua-selected response values, grouping device-affordance members once per payload. */
public final class ResponseSinkFanOutProcessor implements Processor, ManagedRouteResource {
	public static final int DEFAULT_WORKERS = 2;
	public static final int DEFAULT_QUEUE_CAPACITY = 128;
	private static final Logger LOGGER = LoggerFactory.getLogger(ResponseSinkFanOutProcessor.class);
	private final ProducerTemplate producerTemplate;
	private final CamelContext camelContext;
	private final ExecutorService responseExecutor;
	private final AtomicLong rejectedWrites = new AtomicLong();
	private final AtomicLong failedWrites = new AtomicLong();
	private final int workerCount;
	private final int queueCapacity;
	private final String correlationId;
	private final SinkJobs jobs;
	private final Object lifecycleLock = new Object();
	private final AtomicInteger pendingWrites = new AtomicInteger();
	private volatile boolean acceptingWrites = true;
	private volatile boolean closed;

	public ResponseSinkFanOutProcessor(CamelContext context, String correlationId,
			List<Map<String, Object>> rawBindings) {
		this(context, correlationId, rawBindings, DEFAULT_WORKERS, DEFAULT_QUEUE_CAPACITY);
	}

	ResponseSinkFanOutProcessor(CamelContext context, String correlationId,
			List<Map<String, Object>> rawBindings, int workers, int queueCapacity) {
		this.camelContext = context;
		this.workerCount = workers;
		this.queueCapacity = queueCapacity;
		ThreadPoolProfile profile = new ThreadPoolProfile("response-sink-writes-" + correlationId);
		profile.setPoolSize(workers);
		profile.setMaxPoolSize(workers);
		profile.setKeepAliveTime(60L);
		profile.setTimeUnit(TimeUnit.SECONDS);
		profile.setMaxQueueSize(queueCapacity);
		profile.setRejectedPolicy(ThreadPoolRejectedPolicy.Abort);
		LOGGER.info("mapping {} response sink writes use bounded executor (workers={}, queueCapacity={}, rejection=Abort)",
				correlationId, workers, queueCapacity);
		this.correlationId = correlationId;
		this.jobs = new SinkJobs(correlationId, rawBindings);
		this.responseExecutor = context.getExecutorServiceManager().newThreadPool(
				this, "response-sink-writes-" + correlationId, profile);
		this.producerTemplate = context.createProducerTemplate();
	}

	@Override
	public void process(Exchange exchange) {
		for (DispatchJob job : prepareJobs(exchange)) enqueueJob(job);
	}

	public List<DispatchJob> prepareJobs(Exchange exchange) {
		return jobs.prepare(exchange.getMessage().getBody(), false);
	}

	/** Wire Tap's exchange copy is shallow; snapshot payload trees before scheduling. */
	public static void prepareDetachedExchange(Exchange exchange) {
		Object body = exchange.getMessage().getBody();
		exchange.getMessage().setBody(body instanceof DispatchJob job ? copyJob(job) : JsonCodec.copyValue(body));
		Map<String, Object> headers = new LinkedHashMap<>();
		exchange.getMessage().getHeaders().forEach((key, value) -> headers.put(key, JsonCodec.copyValue(value)));
		exchange.getMessage().setHeaders(headers);
		new LinkedHashMap<>(exchange.getProperties()).forEach((key, value) ->
				exchange.setProperty(key, JsonCodec.copyValue(value)));
	}

	private static DispatchJob copyJob(DispatchJob job) {
		return new DispatchJob(job.endpoint(), JsonCodec.copyValue(JsonCodec.toJsonCompatibleValue(job.body())),
				job.sinkLabel(), job.jsonPayload());
	}

	public void sendJob(Exchange exchange) {
		write((DispatchJob) exchange.getMessage().getBody());
	}

	public void recordRejection(Exchange exchange) {
		DispatchJob job = (DispatchJob) exchange.getMessage().getBody();
		long rejected = rejectedWrites.incrementAndGet();
		LOGGER.warn("mapping {} response sink group {} rejected by bounded executor; caller reply continues (rejected={})",
				correlationId, job.sinkLabel(), rejected);
	}

	public void enqueueJob(DispatchJob job) {
		DispatchJob detached = copyJob(job);
		FutureTask<Void> task = new FutureTask<>(() -> {
			write(detached);
			return null;
		}) {
			@Override protected void done() { pendingWrites.decrementAndGet(); }
		};
		synchronized (lifecycleLock) {
			if (!acceptingWrites) {
				reject(job);
				return;
			}
			pendingWrites.incrementAndGet();
			try {
				responseExecutor.execute(task);
			} catch (RejectedExecutionException e) {
				task.cancel(false);
				reject(job);
			}
		}
	}

	private void reject(DispatchJob job) {
		long rejected = rejectedWrites.incrementAndGet();
		LOGGER.warn("mapping {} response sink group {} rejected by bounded executor; caller reply continues (rejected={})",
				correlationId, job.sinkLabel(), rejected);
	}

	@Override
	public void bind(CamelContext context) {
		if (camelContext != context) {
			throw new IllegalArgumentException("response sink resource belongs to another Camel context");
		}
	}

	@Override
	public void close() throws Exception {
		drain(java.time.Duration.ofSeconds(6));
		producerTemplate.stop();
		camelContext.getExecutorServiceManager().shutdown(responseExecutor);
		closed = true;
	}

	@Override
	public void drain(java.time.Duration timeout) throws Exception {
		synchronized (lifecycleLock) {
			if (closed) return;
			acceptingWrites = false;
			responseExecutor.shutdown();
		}
		long nanos = timeout.toNanos();
		long deadline = System.nanoTime() + nanos;
		if (!responseExecutor.awaitTermination(Math.max(0, nanos * 5 / 6), TimeUnit.NANOSECONDS)) {
			for (Runnable queued : responseExecutor.shutdownNow()) {
				if (queued instanceof Future<?> future) future.cancel(false);
				rejectedWrites.incrementAndGet();
			}
			if (!responseExecutor.awaitTermination(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)) {
				throw new IllegalStateException("response writes did not drain before bundle retirement");
			}
		}
	}

	public int pendingWriteCount() { return pendingWrites.get(); }

	@Override public Map<String, Long> metrics() {
		return Map.of("pendingExtraWrites", (long) pendingWrites.get(), "rejectedExtraWrites", rejectedWrites.get(),
				"failedExtraWrites", failedWrites.get(), "extraWriteExecutors", closed ? 0L : 1L);
	}

	public java.util.concurrent.ExecutorService responseExecutor() {
		return responseExecutor;
	}

	private void write(DispatchJob job) {
		try {
			Object body = job.body();
			if (job.jsonPayload() && (body instanceof String || body == null)) {
				body = new ConnectorJsonPayload(body);
			} else if (body == null) {
				// Preserve an intentional wire null, not an absent transport body.
				body = JsonPointer.JSON_NULL;
			}
			if (job.jsonPayload()) {
				producerTemplate.sendBodyAndHeader(job.endpoint(), body, Exchange.CONTENT_TYPE, "application/json");
			} else {
				producerTemplate.sendBody(job.endpoint(), body);
			}
		} catch (RuntimeException e) {
			long failures = failedWrites.incrementAndGet();
			LOGGER.warn("mapping {} response sink group {} failed; caller reply is unchanged (failed={})",
					correlationId, job.sinkLabel(), failures);
		}
	}

	public long rejectedWriteCount() {
		return rejectedWrites.get();
	}

	public long failedWriteCount() {
		return failedWrites.get();
	}

	public int workerCount() { return workerCount; }

	public int queueCapacity() { return queueCapacity; }

	public record DispatchJob(String endpoint, Object body, String sinkLabel, boolean jsonPayload) { }

}
