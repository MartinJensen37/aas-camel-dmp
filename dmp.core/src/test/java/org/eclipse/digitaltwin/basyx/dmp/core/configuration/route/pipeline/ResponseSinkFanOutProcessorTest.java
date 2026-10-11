package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class ResponseSinkFanOutProcessorTest {
	@Test
	void saturatedDetachedWritesRejectWithoutBlockingTheCaller() throws Exception {
		var context = new DefaultCamelContext();
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		context.addRoutes(new RouteBuilder() {
			@Override
			public void configure() {
				from("direct:slow").process(exchange -> {
					entered.countDown();
					if (!release.await(2, TimeUnit.SECONDS)) {
						throw new IllegalStateException("test release timed out");
					}
				});
			}
		});
		context.start();
		try {
			var processor = new ResponseSinkFanOutProcessor(context, "bounded-test", List.of(
					Map.of("sinkId", "First", "endpoint", "direct:slow"),
					Map.of("sinkId", "Second", "endpoint", "direct:slow")), 1, 0);
			assertEquals(1, processor.workerCount());
			assertEquals(0, processor.queueCapacity());
			var exchange = new DefaultExchange(context);
			exchange.getMessage().setBody(Map.of("First", 1, "Second", 2));

			processor.process(exchange);
			assertTrue(entered.await(2, TimeUnit.SECONDS));
			assertEquals(1, processor.rejectedWriteCount());
			assertEquals(0, processor.failedWriteCount());
		} finally {
			release.countDown();
			context.stop();
		}
	}
}
