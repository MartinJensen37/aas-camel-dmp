package dk.aau.smartproductionlab.dmp.paho.mqtt5.component;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.component.paho.mqtt5.PahoMqtt5Constants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.Test;

/** Deterministic transport seam: exercises real Camel route installation, not live devices. */
class MqttV5ProducerLifecycleTest {
	@Test
	void failedSubscriptionCanRetryRegistrationButNeverPublishesFailedAttempt() throws Exception {
		try (var fixture = new Fixture()) {
			var producer = fixture.producer();
			fixture.transport.failStarts.set(1);
			assertThrows(Exception.class, () -> producer.process(fixture.request("authored")));
			assertEquals(0, fixture.transport.publications.get());
			assertEquals(0, fixture.replySubscriptionCount());
			assertEquals(0, fixture.endpoint.replies.pendingCount());
			var request = fixture.request("authored");
			producer.process(request);
			assertEquals("{\"State\":\"SUCCESS\"}", request.getMessage().getBody());
			assertEquals(1, fixture.transport.publications.get());
			assertEquals(2, fixture.transport.starts.get());
		}
	}

	@Test
	void concurrentInvocationWaitsForReadinessAndImmediateReplyCannotRaceRegistration() throws Exception {
		try (var fixture = new Fixture()) {
			fixture.transport.readyGate = new CountDownLatch(1);
			var producer = fixture.producer();
			var first = fixture.executor.submit(() -> {
				producer.process(fixture.request("first"));
				return null;
			});
			assertTrue(fixture.transport.startEntered.await(5, TimeUnit.SECONDS));
			assertEquals(0, fixture.transport.publications.get());
			var second = fixture.executor.submit(() -> {
				producer.process(fixture.request("second"));
				return null;
			});
			fixture.transport.readyGate.countDown();
			first.get(5, TimeUnit.SECONDS);
			second.get(5, TimeUnit.SECONDS);
			assertEquals(2, fixture.transport.publications.get());
			assertEquals(1, fixture.transport.starts.get());
			assertEquals(0, fixture.endpoint.replies.pendingCount());
		}
	}

	@Test
	void lastOwnerRemovesRouteAndStoppingOneOwnerDoesNotCancelTheOther() throws Exception {
		try (var fixture = new Fixture()) {
			fixture.transport.autoReply = false;
			fixture.transport.published = new CountDownLatch(2);
			var first = fixture.producer();
			var second = fixture.producer();
			var a = fixture.executor.submit(() -> { first.process(fixture.request("a")); return null; });
			var b = fixture.executor.submit(() -> { second.process(fixture.request("b")); return null; });
			assertTrue(fixture.transport.published.await(5, TimeUnit.SECONDS));
			assertEquals(1, fixture.replySubscriptionCount());
			assertEquals(1, fixture.transport.producersCreated.get());
			assertEquals(1, fixture.transport.activeClients.get());
			String oldRoute = fixture.replySubscriptionId();
			first.stop();
			assertThrows(java.util.concurrent.ExecutionException.class, () -> a.get(5, TimeUnit.SECONDS));
			assertFalse(b.isDone());
			assertEquals(1, fixture.endpoint.replies.pendingCount());
			assertEquals(1, fixture.replySubscriptionCount());
			assertEquals(1, fixture.transport.activeClients.get());
			assertEquals(0, fixture.transport.clientsStopped.get());
			fixture.transport.reply("a", "late");
			assertFalse(b.isDone());
			fixture.transport.reply("b", "{\"State\":\"SUCCESS\"}");
			b.get(5, TimeUnit.SECONDS);
			second.stop();
			assertEquals(0, fixture.replySubscriptionCount());
			assertTrue(fixture.transport.subscribers.isEmpty());
			int endpointCount = fixture.context.getEndpoints().size();
			fixture.transport.autoReply = true;
			var replacement = fixture.producer();
			replacement.process(fixture.request("new-generation"));
			assertEquals(3, fixture.transport.publications.get());
			assertNotEquals(oldRoute, fixture.replySubscriptionId());
			replacement.stop();
			assertEquals(0, fixture.replySubscriptionCount());
			assertEquals(endpointCount, fixture.context.getEndpoints().size());
			for (int generation = 0; generation < 3; generation++) {
				var readded = fixture.producer();
				readded.process(fixture.request("readded-" + generation));
				readded.stop();
				assertEquals(0, fixture.replySubscriptionCount());
				assertEquals(endpointCount, fixture.context.getEndpoints().size());
			}
		}
	}

	@Test
	void everyRequestCarriesAFreshCorrelation() throws Exception {
		try (var fixture = new Fixture()) {
			var producer = fixture.producer();
			var first = fixture.request("first");
			var second = fixture.request("second");
			producer.process(first);
			producer.process(second);
			assertEquals(2, fixture.transport.commands.size(), "two requests, two correlations");
			assertEquals("{\"State\":\"SUCCESS\"}", second.getMessage().getBody());
		}
	}

	@Test
	void publishFailureIsNotRetried() throws Exception {
		try (var fixture = new Fixture()) {
			var producer = fixture.producer();
			fixture.transport.failPublishes.set(1);
			assertThrows(Exception.class, () -> producer.process(fixture.request("failed-command")));
			assertEquals(1, fixture.transport.publications.get());
			assertEquals(0, fixture.endpoint.replies.pendingCount());
			producer.process(fixture.request("next-command"));
			assertEquals(2, fixture.transport.publications.get());
		}
	}

	@Test
	void timeoutDropsTheLateReplyAndDoesNotPublishAgain() throws Exception {
		try (var fixture = new Fixture()) {
			fixture.transport.autoReply = false;
			fixture.endpoint.setReplyTimeout(0);
			var producer = fixture.producer();
			var failure = assertThrows(dk.aau.smartproductionlab.dmp.core.configuration.route.core.ConnectorInvocationException.class,
					() -> producer.process(fixture.request("timed-out")));
			assertEquals("MQTT_REQUEST_TIMEOUT", failure.getConnectorCode());
			fixture.transport.reply("timed-out", "late");
			assertEquals(1, fixture.transport.publications.get());
			assertEquals(0, fixture.endpoint.replies.pendingCount());
		}
	}

	@Test
	void collidingEndpointHashesHaveDifferentOwnedRouteIds() throws Exception {
		try (var fixture = new Fixture()) {
			String options = "?brokerUrl=tcp://hash-collision:1883&clientId=collision&replyTopic=reply";
			var a = fixture.context.getEndpoint("paho-mqtt5-inout:Aa" + options, MqttV5Endpoint.class);
			var b = fixture.context.getEndpoint("paho-mqtt5-inout:BB" + options, MqttV5Endpoint.class);
			assertEquals(a.getEndpointKey().hashCode(), b.getEndpointKey().hashCode());
			Object ownerA = new Object();
			Object ownerB = new Object();
			try {
				a.acquireReplySubscription(ownerA, "probe:reply-a");
				b.acquireReplySubscription(ownerB, "probe:reply-b");
				assertEquals(2, fixture.replySubscriptionCount());
				a.releaseReplySubscription(ownerA);
				assertEquals(1, fixture.replySubscriptionCount());
				assertTrue(fixture.transport.subscribers.containsKey("reply-b"));
			} finally {
				a.releaseReplySubscription(ownerA);
				b.releaseReplySubscription(ownerB);
			}
			assertEquals(0, fixture.replySubscriptionCount());
		}
	}

	@Test
	void ownersShareOneUpstreamProducerAndClientUntilTheLastOwnerStops() throws Exception {
		try (var fixture = new Fixture()) {
			var first = fixture.producer();
			var second = fixture.producer();
			first.process(fixture.request("first-owner"));
			second.process(fixture.request("second-owner"));
			assertEquals(1, fixture.transport.producersCreated.get());
			assertEquals(1, fixture.transport.clientsStarted.get());
			assertEquals(1, fixture.transport.activeClients.get());
			first.stop();
			assertEquals(0, fixture.transport.clientsStopped.get());
			assertEquals(1, fixture.transport.activeClients.get());
			second.process(fixture.request("surviving-owner"));
			assertEquals(1, fixture.transport.producersCreated.get());
			assertEquals(3, fixture.transport.publications.get());
			second.stop();
			assertEquals(1, fixture.transport.clientsStopped.get());
			assertEquals(0, fixture.transport.activeClients.get());
		}
	}

	@Test
	void invocationPausedAcrossStopStartCannotPublishInTheNewGeneration() throws Exception {
		try (var fixture = new Fixture()) {
			var producer = fixture.producer();
			CountDownLatch preparing = new CountDownLatch(1);
			CountDownLatch resume = new CountDownLatch(1);
			var oldRequest = fixture.request("old-generation");
			oldRequest.getMessage().setBody(new Object() {
				@Override public String toString() {
					preparing.countDown();
					try {
						if (!resume.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("preparation timed out");
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new IllegalStateException(e);
					}
					return "{}";
				}
			});
			var oldInvocation = fixture.executor.submit(() -> { producer.process(oldRequest); return null; });
			try {
				assertTrue(preparing.await(5, TimeUnit.SECONDS));
				producer.stop();
				producer.start();
			} finally {
				resume.countDown();
			}
			var failure = assertThrows(java.util.concurrent.ExecutionException.class,
					() -> oldInvocation.get(5, TimeUnit.SECONDS));
			assertInstanceOf(IllegalStateException.class, failure.getCause());
			assertEquals(0, fixture.transport.publications.get());
			assertEquals(0, fixture.transport.producersCreated.get());
			assertEquals(0, fixture.endpoint.replies.pendingCount());
			producer.process(fixture.request("current-generation"));
			assertEquals(1, fixture.transport.publications.get());
			assertFalse(fixture.transport.commands.containsKey("old-generation"));
		}
	}

	@Test
	void failedLastOwnerCleanupHasNoGhostOwnerAndAcquisitionRetriesCleanup() throws Exception {
		try (var fixture = new Fixture()) {
			var first = fixture.producer();
			first.process(fixture.request("before-cleanup-failure"));
			String oldRoute = fixture.replySubscriptionId();
			Endpoint oldReplyEndpoint = fixture.endpoint.replyEndpoint();
			fixture.replyCleanupTarget = oldReplyEndpoint;
			fixture.failReplyCleanup.set(2);
			assertThrows(Exception.class, first::stop);
			assertEquals(1, fixture.replyCleanupAttempts.get());
			assertEquals(1, fixture.failReplyCleanup.get());
			assertEquals(0, fixture.replySubscriptionCount());
			assertSame(oldReplyEndpoint, fixture.context.hasEndpoint("probe:reply"));
			assertEquals(0, fixture.transport.activeClients.get(), "publication transport must release even if reply cleanup fails");
			var replacement = fixture.producer();
			assertThrows(Exception.class, () -> replacement.process(fixture.request("after-cleanup-failure")));
			assertEquals(2, fixture.replyCleanupAttempts.get());
			assertEquals(0, fixture.failReplyCleanup.get());
			assertEquals(0, fixture.replySubscriptionCount());
			assertSame(oldReplyEndpoint, fixture.context.hasEndpoint("probe:reply"));
			assertEquals(1, fixture.transport.publications.get(), "cleanup retry must not publish on failure");
			assertEquals(0, fixture.endpoint.replies.pendingCount());
			replacement.process(fixture.request("after-cleanup-failure"));
			assertEquals(3, fixture.replyCleanupAttempts.get());
			assertNotSame(oldReplyEndpoint, fixture.endpoint.replyEndpoint());
			assertNotEquals(oldRoute, fixture.replySubscriptionId());
			assertEquals(2, fixture.transport.publications.get());
			replacement.stop();
			assertEquals(0, fixture.replySubscriptionCount());
			assertEquals(0, fixture.context.getEndpoints().stream()
					.filter(endpoint -> endpoint.getEndpointUri().equals(oldReplyEndpoint.getEndpointUri())).count());
			assertNull(fixture.context.hasEndpoint("probe:reply"));
			assertTrue(fixture.transport.subscribers.isEmpty());
			assertEquals(0, fixture.transport.activeClients.get());
			assertEquals(2, fixture.transport.clientsStopped.get());
			assertEquals(0, fixture.endpoint.replies.pendingCount());
		}
	}

	private static final class Fixture implements AutoCloseable {
		final AtomicInteger failReplyCleanup = new AtomicInteger();
		final AtomicInteger replyCleanupAttempts = new AtomicInteger();
		volatile Endpoint replyCleanupTarget;
		final DefaultCamelContext context = new DefaultCamelContext() {
			@Override public void removeEndpoint(Endpoint endpoint) {
				if (endpoint == replyCleanupTarget) {
					replyCleanupAttempts.incrementAndGet();
					if (failReplyCleanup.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
						throw new IllegalStateException("reply endpoint cleanup failed");
					}
				}
				super.removeEndpoint(endpoint);
			}
		};
		final ProbeTransport transport = new ProbeTransport();
		final ExecutorService executor = Executors.newFixedThreadPool(2);
		final List<MqttV5Producer> producers = new ArrayList<>();
		final MqttV5Endpoint endpoint;

		Fixture() throws Exception {
			context.addComponent("probe", transport);
			context.start();
			endpoint = context.getEndpoint("paho-mqtt5-inout:command?brokerUrl=tcp://" + UUID.randomUUID()
					+ ":1883&clientId=test&replyTopic=reply&replyTimeout=2",
					MqttV5Endpoint.class);
		}

		MqttV5Producer producer() throws Exception {
			var producer = new MqttV5Producer(endpoint) {
				@Override protected String upstreamReplyUri() { return "probe:reply"; }
				@Override protected String upstreamPublishUri() { return "probe:command"; }
			};
			producer.start();
			producers.add(producer);
			return producer;
		}

		/** A request the probe can answer by {@code label}; the correlation itself is a fresh UUID. */
		Exchange request(String label) {
			var request = new DefaultExchange(context, ExchangePattern.InOut);
			request.getMessage().setBody(Map.of("value", 1, "label", label));
			return request;
		}

		/** Started reply consumers across every request/reply endpoint of the context. */
		long replySubscriptionCount() {
			return context.getEndpoints().stream().filter(MqttV5Endpoint.class::isInstance)
					.map(MqttV5Endpoint.class::cast).filter(MqttV5Endpoint::replySubscribed).count();
		}

		String replySubscriptionId() {
			return java.util.Objects.requireNonNull(endpoint.replySubscriptionId(), "no reply subscription");
		}

		@Override public void close() throws Exception {
			if (transport.readyGate != null) transport.readyGate.countDown();
			try {
				for (var producer : producers) producer.stop();
			} finally {
				executor.shutdownNow();
				context.stop();
			}
		}
	}

	private static final class ProbeTransport extends DefaultComponent {
		final AtomicInteger failStarts = new AtomicInteger();
		final AtomicInteger failPublishes = new AtomicInteger();
		final AtomicInteger starts = new AtomicInteger();
		final AtomicInteger publications = new AtomicInteger();
		final AtomicInteger producersCreated = new AtomicInteger();
		final AtomicInteger clientsStarted = new AtomicInteger();
		final AtomicInteger clientsStopped = new AtomicInteger();
		final AtomicInteger activeClients = new AtomicInteger();
		final CountDownLatch startEntered = new CountDownLatch(1);
		final Map<String, Processor> subscribers = new ConcurrentHashMap<>();
		final Map<String, MqttProperties> commands = new ConcurrentHashMap<>();
		/** Request label -> the correlation its command carried. */
		final Map<String, String> correlations = new ConcurrentHashMap<>();
		volatile CountDownLatch readyGate;
		volatile CountDownLatch published = new CountDownLatch(1);
		volatile boolean autoReply = true;

		@Override protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
			return new DefaultEndpoint(uri, this) {
				@Override public boolean isSingleton() { return true; }
				@Override public Consumer createConsumer(Processor processor) {
					return new DefaultConsumer(this, processor) {
						@Override protected void doStart() throws Exception {
							super.doStart();
							starts.incrementAndGet();
							startEntered.countDown();
							if (readyGate != null && !readyGate.await(5, TimeUnit.SECONDS)) {
								throw new IllegalStateException("readiness timed out");
							}
							if (failStarts.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
								throw new IllegalStateException("subscription failed");
							}
							subscribers.put(remaining, processor);
						}
						@Override protected void doStop() throws Exception {
							subscribers.remove(remaining, processor);
							super.doStop();
						}
						};
				}
				@Override public Producer createProducer() {
					producersCreated.incrementAndGet();
					return new DefaultProducer(this) {
						// One counted client per upstream producer, as in camel-paho-mqtt5.
						@Override protected void doStart() throws Exception {
							super.doStart();
							clientsStarted.incrementAndGet();
							activeClients.incrementAndGet();
						}
						@Override protected void doStop() throws Exception {
							clientsStopped.incrementAndGet();
							activeClients.decrementAndGet();
							super.doStop();
						}
						@Override public void process(Exchange exchange) throws Exception {
							assertTrue(subscribers.containsKey("reply"), "publication preceded subscription readiness");
							MqttProperties properties = exchange.getMessage().getHeader(
									PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES, MqttProperties.class);
							String correlation = new String(properties.getCorrelationData(), StandardCharsets.UTF_8);
							commands.put(correlation, properties);
							var label = java.util.regex.Pattern.compile("\"label\":\"([^\"]*)\"")
									.matcher(new String((byte[]) exchange.getMessage().getBody(), StandardCharsets.UTF_8));
							if (label.find()) correlations.put(label.group(1), correlation);
							publications.incrementAndGet();
							published.countDown();
							if (failPublishes.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
								throw new IllegalStateException("publication failed");
							}
							if (autoReply) {
								reply(correlation, "{\"State\":\"SUCCESS\"}");
							}
						}
					};
				}
			};
		}

		/** Answers the command of the request labelled {@code label} (or with that correlation). */
		void reply(String label, String payload) throws Exception {
			var reply = new DefaultExchange(getCamelContext());
			reply.getMessage().setBody(payload);
			reply.getMessage().setHeader(PahoMqtt5Constants.MQTT_TOPIC, "reply");
			reply.getMessage().setHeader(PahoMqtt5Constants.CAMEL_PAHO_MSG_PROPERTIES,
					commands.get(correlations.getOrDefault(label, label)));
			subscribers.get("reply").process(reply);
		}
	}
}
