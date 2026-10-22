/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import dk.aau.smartproductionlab.dmp.core.audit.AuditLog;
import dk.aau.smartproductionlab.dmp.core.audit.AuditPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes the AAS interaction log to InfluxDB (v2 write API, line protocol).
 * {@link #record} only offers the point to a bounded queue, so the data flow
 * never waits: when the queue is full the point is dropped and counted. One
 * writer thread sends batches; transient failures (unreachable, 5xx, 408, 429)
 * are retried with backoff while new points keep queueing, a rejected batch
 * (other 4xx) is dropped and counted.
 */
public final class InfluxAuditLog implements AuditLog, AutoCloseable {
	private static final Logger LOGGER = LoggerFactory.getLogger(InfluxAuditLog.class);
	private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);

	public record Settings(URI url, String token, String org, String bucket, int queueSize, int batchSize,
			Duration flushInterval) {
		public Settings {
			if (url == null || !url.isAbsolute() || url.getHost() == null || url.getUserInfo() != null
					|| !("http".equalsIgnoreCase(url.getScheme()) || "https".equalsIgnoreCase(url.getScheme())))
				throw new IllegalArgumentException("DMP_AUDIT_INFLUX_URL must be a credential-free HTTP(S) URL");
			for (String[] required : new String[][] { { "TOKEN", token }, { "ORG", org }, { "BUCKET", bucket } })
				if (required[1] == null || required[1].isBlank())
					throw new IllegalArgumentException("DMP_AUDIT_INFLUX_" + required[0] + " is required with DMP_AUDIT_INFLUX_URL");
			if (queueSize < 1 || queueSize > 1_000_000) throw new IllegalArgumentException("audit queue size must be 1..1000000");
			if (batchSize < 1 || batchSize > 10_000) throw new IllegalArgumentException("audit batch size must be 1..10000");
			if (flushInterval == null || flushInterval.toMillis() < 10 || flushInterval.toMillis() > 60_000)
				throw new IllegalArgumentException("audit flush interval must be 10..60000 ms");
		}

		/** The settings from DMP_AUDIT_INFLUX_* variables; empty when no URL is set. */
		public static Optional<Settings> fromEnvironment(Map<String, String> environment) {
			String url = environment.get("DMP_AUDIT_INFLUX_URL");
			if (url == null || url.isBlank()) return Optional.empty();
			URI uri;
			try { uri = URI.create(url.trim().replaceAll("/+$", "")); }
			catch (IllegalArgumentException invalid) { throw new IllegalArgumentException("DMP_AUDIT_INFLUX_URL must be a URL"); }
			return Optional.of(new Settings(uri, environment.get("DMP_AUDIT_INFLUX_TOKEN"),
					trimmed(environment.get("DMP_AUDIT_INFLUX_ORG")), trimmed(environment.get("DMP_AUDIT_INFLUX_BUCKET")),
					integer(environment, "DMP_AUDIT_INFLUX_QUEUE_SIZE", 10_000),
					integer(environment, "DMP_AUDIT_INFLUX_BATCH_SIZE", 500),
					Duration.ofMillis(integer(environment, "DMP_AUDIT_INFLUX_FLUSH_INTERVAL_MS", 1_000))));
		}

		@Override public String toString() {
			return "InfluxAuditLog.Settings[url=" + url + ", token=<redacted>, org=" + org + ", bucket=" + bucket
					+ ", queueSize=" + queueSize + ", batchSize=" + batchSize + "]";
		}

		private static String trimmed(String value) { return value == null ? null : value.trim(); }

		private static int integer(Map<String, String> environment, String name, int fallback) {
			String value = environment.get(name);
			return value == null || value.isBlank() ? fallback : Integer.parseInt(value.trim());
		}
	}

	private enum Outcome { WRITTEN, REJECTED, RETRY }

	private final Settings settings;
	private final URI writeUri;
	private final HttpClient client;
	private final BlockingQueue<AuditPoint> queue;
	private final Thread writer;
	private final AtomicLong written = new AtomicLong();
	private final AtomicLong dropped = new AtomicLong();
	private final AtomicLong rejected = new AtomicLong();
	private final AtomicReference<Instant> lastSuccess = new AtomicReference<>();
	private final AtomicReference<String> lastError = new AtomicReference<>("");
	private volatile boolean closed;

	public InfluxAuditLog(Settings settings) {
		this.settings = settings;
		this.writeUri = URI.create(settings.url() + "/api/v2/write?org=" + encode(settings.org()) + "&bucket="
				+ encode(settings.bucket()) + "&precision=ns");
		this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
		this.queue = new ArrayBlockingQueue<>(settings.queueSize());
		this.writer = new Thread(this::run, "dmp-audit-writer");
		writer.setDaemon(true);
		writer.start();
	}

	@Override
	public void record(AuditPoint point) {
		if (closed || point == null || !queue.offer(point)) dropped.incrementAndGet();
	}

	/** Counters for {@code /management/status}; never contains the token. */
	public Map<String, Object> status() {
		Map<String, Object> status = new LinkedHashMap<>();
		status.put("enabled", true);
		status.put("queued", queue.size());
		status.put("written", written.get());
		status.put("dropped", dropped.get());
		status.put("rejected", rejected.get());
		Instant success = lastSuccess.get();
		status.put("lastSuccess", success == null ? "" : success.toString());
		status.put("lastError", lastError.get());
		return status;
	}

	/** Stops accepting points, flushes what is queued for up to {@code timeout}, then stops the writer. */
	public void close(Duration timeout) {
		closed = true;
		try {
			writer.join(timeout.toMillis());
			if (writer.isAlive()) {
				writer.interrupt();
				writer.join(1_000);
			}
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
		dropped.addAndGet(queue.size());
		queue.clear();
	}

	@Override
	public void close() {
		close(Duration.ofSeconds(3));
	}

	private void run() {
		List<AuditPoint> batch = new ArrayList<>();
		long backoffMillis = 0;
		while (true) {
			try {
				if (batch.isEmpty()) {
					if (closed && queue.isEmpty()) return;
					AuditPoint first = queue.poll(settings.flushInterval().toMillis(), TimeUnit.MILLISECONDS);
					if (first == null) continue;
					batch.add(first);
					queue.drainTo(batch, settings.batchSize() - 1);
				}
				switch (send(batch)) {
				case WRITTEN -> { written.addAndGet(batch.size()); lastSuccess.set(Instant.now()); batch.clear(); backoffMillis = 0; }
				case REJECTED -> { rejected.addAndGet(batch.size()); batch.clear(); }
				case RETRY -> {
					if (closed) { dropped.addAndGet(batch.size()); batch.clear(); continue; }
					backoffMillis = backoffMillis == 0 ? 500 : Math.min(backoffMillis * 2, MAX_BACKOFF.toMillis());
					Thread.sleep(backoffMillis);
				}
				}
			} catch (InterruptedException interrupted) {
				dropped.addAndGet(batch.size());
				return;
			} catch (RuntimeException unexpected) {
				// Never let one bad batch stop the writer.
				rejected.addAndGet(batch.size());
				batch.clear();
				lastError.set("encoding-failed");
				LOGGER.warn("audit batch could not be encoded: {}", unexpected.getClass().getSimpleName());
			}
		}
	}

	private Outcome send(List<AuditPoint> batch) throws InterruptedException {
		StringBuilder body = new StringBuilder(batch.size() * 128);
		for (AuditPoint point : batch) LineProtocol.append(body, point);
		HttpRequest request = HttpRequest.newBuilder(writeUri).timeout(Duration.ofSeconds(10))
				.header("Authorization", "Token " + settings.token())
				.header("Content-Type", "text/plain; charset=utf-8")
				.POST(HttpRequest.BodyPublishers.ofString(body.toString(), StandardCharsets.UTF_8)).build();
		HttpResponse<Void> response;
		try { response = client.send(request, HttpResponse.BodyHandlers.discarding()); }
		catch (java.io.IOException unreachable) {
			noteError("influx-unreachable");
			return Outcome.RETRY;
		}
		int status = response.statusCode();
		if (status >= 200 && status < 300) return Outcome.WRITTEN;
		noteError("influx-http-" + status);
		return status >= 400 && status < 500 && status != 408 && status != 429 ? Outcome.REJECTED : Outcome.RETRY;
	}

	private void noteError(String category) {
		if (!category.equals(lastError.getAndSet(category)))
			LOGGER.warn("audit log write to InfluxDB failed: {}", category);
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8);
	}
}
