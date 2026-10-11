package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline.AffordanceValidation;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;
import org.apache.camel.spi.ThreadPoolProfile;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Queues Lua-selected response values, grouping device-affordance members once per payload. */
public final class ResponseSinkFanOutProcessor implements Processor {
	public static final int DEFAULT_WORKERS = 2;
	public static final int DEFAULT_QUEUE_CAPACITY = 128;
	private static final Logger LOGGER = LoggerFactory.getLogger(ResponseSinkFanOutProcessor.class);
	private final ProducerTemplate producerTemplate;
	private final ExecutorService responseExecutor;
	private final AtomicLong rejectedWrites = new AtomicLong();
	private final AtomicLong failedWrites = new AtomicLong();
	private final int workerCount;
	private final int queueCapacity;
	private final String correlationId;
	private final List<SinkBinding> bindings;
	private final Map<String, AffordanceValidation> memberValidators;
	private final Map<String, AffordanceValidation> payloadValidators;
	private final Map<String, AffordanceValidation> singlePayloadValidators;

	public ResponseSinkFanOutProcessor(CamelContext context, String correlationId,
			List<Map<String, Object>> rawBindings) {
		this(context, correlationId, rawBindings, DEFAULT_WORKERS, DEFAULT_QUEUE_CAPACITY);
	}

	ResponseSinkFanOutProcessor(CamelContext context, String correlationId,
			List<Map<String, Object>> rawBindings, int workers, int queueCapacity) {
		this.producerTemplate = context.createProducerTemplate();
		this.workerCount = workers;
		this.queueCapacity = queueCapacity;
		ThreadPoolProfile profile = new ThreadPoolProfile("response-sink-writes-" + correlationId);
		profile.setPoolSize(workers);
		profile.setMaxPoolSize(workers);
		profile.setKeepAliveTime(60L);
		profile.setTimeUnit(TimeUnit.SECONDS);
		profile.setMaxQueueSize(queueCapacity);
		profile.setRejectedPolicy(ThreadPoolRejectedPolicy.Abort);
		this.responseExecutor = context.getExecutorServiceManager().newThreadPool(
				this, "response-sink-writes-" + correlationId, profile);
		LOGGER.info("mapping {} response sink writes use bounded executor (workers={}, queueCapacity={}, rejection=Abort)",
				correlationId, workers, queueCapacity);
		this.correlationId = correlationId;
		List<SinkBinding> resolvedBindings = new ArrayList<>();
		this.memberValidators = new LinkedHashMap<>();
		this.payloadValidators = new LinkedHashMap<>();
		this.singlePayloadValidators = new LinkedHashMap<>();
		for (Map<String, Object> raw : rawBindings) {
			SinkBinding binding = SinkBinding.from(raw);
			resolvedBindings.add(binding);
			if (binding.schema != null) {
				memberValidators.put(binding.sinkId,
						new AffordanceValidation(binding.schema, "response sink " + binding.sinkId));
			}
			if (binding.payloadGroup != null && binding.payloadSchema != null) {
				payloadValidators.putIfAbsent(binding.payloadGroup,
						new AffordanceValidation(binding.payloadSchema,
								"response payload group " + binding.payloadGroup));
			}
			if (binding.payloadGroup == null && binding.payloadSchema != null) {
				singlePayloadValidators.put(binding.sinkId,
						new AffordanceValidation(binding.payloadSchema,
								"response sink payload " + binding.sinkId));
			}
		}
		this.bindings = List.copyOf(resolvedBindings);
	}

	@Override
	public void process(Exchange exchange) {
		for (DispatchJob job : prepareJobs(exchange)) enqueueJob(job);
	}

	public List<DispatchJob> prepareJobs(Exchange exchange) {
		Object parsed = exchange.getMessage().getBody();
		if (!(parsed instanceof Map<?, ?> rawValues)) {
			throw new IllegalArgumentException("response Lua returned no SinkId map");
		}
		Map<String, Object> values = new LinkedHashMap<>();
		rawValues.forEach((key, value) -> values.put(String.valueOf(key), value));
		Map<String, Group> groups = new LinkedHashMap<>();
		List<DispatchJob> jobs = new ArrayList<>();
		java.util.Set<String> incompleteGroups = new java.util.HashSet<>();
		for (SinkBinding binding : bindings) {
			if (!values.containsKey(binding.sinkId)) {
				LOGGER.warn("mapping {} response Lua omitted SinkId {}; skipping response sink",
						correlationId, binding.sinkId);
				if (binding.payloadGroup != null) {
					incompleteGroups.add(binding.payloadGroup);
				}
				continue;
			}
			String groupKey = binding.payloadGroup != null
					? "payload:" + binding.payloadGroup : "sink:" + binding.sinkId;
			groups.computeIfAbsent(groupKey, ignored -> new Group(binding.payloadGroup, binding.jsonPayload))
					.add(binding, values.get(binding.sinkId));
		}
		for (Group group : groups.values()) {
			if (group.payloadGroup != null && incompleteGroups.contains(group.payloadGroup)) {
				LOGGER.warn("mapping {} response sink group {} is incomplete; skipping group",
					correlationId, group.payloadGroup);
				continue;
			}
			DispatchJob job = dispatch(group);
			if (job != null) jobs.add(job);
		}
		return List.copyOf(jobs);
	}

	private DispatchJob dispatch(Group group) {
		SinkBinding first = group.bindings.get(0);
		Object payload;
		boolean assembleMembers = group.bindings.size() > 1 || first.valuePath != null;
		try {
			if (assembleMembers) {
				Map<String, Object> keyed = new LinkedHashMap<>();
				Map<String, String> paths = new LinkedHashMap<>();
				for (SinkBinding binding : group.bindings) {
					keyed.put(binding.sinkId, group.values.get(binding.sinkId));
					if (binding.valuePath != null) paths.put(binding.sinkId, binding.valuePath);
					validateMember(binding, group.values.get(binding.sinkId));
				}
				payload = PayloadAssembler.assemble(keyed,
						group.bindings.stream().map(b -> b.sinkId).toList(), paths);
				AffordanceValidation validator = group.payloadGroup == null
						? null : payloadValidators.get(group.payloadGroup);
				if (validator != null) validator.outgoing(payload);
			} else {
				payload = group.values.get(first.sinkId);
				validateMember(first, payload);
				AffordanceValidation validator = first.payloadGroup == null
						? singlePayloadValidators.get(first.sinkId)
						: payloadValidators.get(first.payloadGroup);
				if (validator != null) validator.outgoing(payload);
			}
		} catch (RuntimeException e) {
			LOGGER.warn("mapping {} response sink group {} rejected before send: {}",
				correlationId, group.payloadGroup == null ? first.sinkId : group.payloadGroup,
					e.getMessage());
			return null;
		}

		String endpoint = first.endpoint;
		String sinkLabel = group.payloadGroup == null ? first.sinkId : group.payloadGroup;
		return new DispatchJob(endpoint, payload, sinkLabel, group.jsonPayload);
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
		try {
			responseExecutor.execute(() -> {
				write(job);
			});
		} catch (RejectedExecutionException e) {
			long rejected = rejectedWrites.incrementAndGet();
			LOGGER.warn("mapping {} response sink group {} rejected by bounded executor; caller reply continues (rejected={})",
					correlationId, job.sinkLabel(), rejected);
		}
	}

	public java.util.concurrent.ExecutorService responseExecutor() {
		return responseExecutor;
	}

	private void write(DispatchJob job) {
		try {
			Object body = job.jsonPayload() && job.body() instanceof String
					? new ConnectorJsonPayload(job.body()) : job.body();
			if (job.jsonPayload()) {
				producerTemplate.sendBodyAndHeader(job.endpoint(), body, Exchange.CONTENT_TYPE, "application/json");
			} else {
				producerTemplate.sendBody(job.endpoint(), body);
			}
		} catch (RuntimeException e) {
			long failures = failedWrites.incrementAndGet();
			LOGGER.warn("mapping {} response sink group {} failed ({}): {}",
					correlationId, job.sinkLabel(), job.endpoint(), e.getMessage());
			LOGGER.debug("response sink failure count for mapping {} is {}", correlationId, failures);
		}
	}

	private static boolean isJsonSchema(Map<String, Object> schema) {
		if (schema == null || schema.isEmpty()) return false;
		return schema.containsKey("type") || schema.containsKey("properties") || schema.containsKey("items")
				|| schema.containsKey("const") || schema.containsKey("enum") || schema.containsKey("oneOf")
				|| schema.containsKey("anyOf") || schema.containsKey("allOf");
	}

	public long rejectedWriteCount() {
		return rejectedWrites.get();
	}

	public long failedWriteCount() {
		return failedWrites.get();
	}

	public int workerCount() { return workerCount; }

	public int queueCapacity() { return queueCapacity; }

	private void validateMember(SinkBinding binding, Object value) {
		AffordanceValidation validator = memberValidators.get(binding.sinkId);
		if (validator != null) validator.outgoing(value);
	}

	private static final class Group {
		private final String payloadGroup;
		private final boolean jsonPayload;
		private final List<SinkBinding> bindings = new ArrayList<>();
		private final Map<String, Object> values = new LinkedHashMap<>();

		private Group(String payloadGroup, boolean jsonPayload) {
			this.payloadGroup = payloadGroup;
			this.jsonPayload = jsonPayload;
		}

		private void add(SinkBinding binding, Object value) {
			bindings.add(binding);
			values.put(binding.sinkId, value == null ? JsonPointer.JSON_NULL : value);
		}
	}

	public record DispatchJob(String endpoint, Object body, String sinkLabel, boolean jsonPayload) { }

	private record SinkBinding(String sinkId, String endpoint, String payloadGroup,
			String valuePath, Map<String, Object> payloadSchema, Map<String, Object> schema,
			boolean jsonPayload) {
		@SuppressWarnings("unchecked")
		private static SinkBinding from(Map<String, Object> raw) {
			Map<String, Object> payloadSchema = asSchema(raw.get("payloadSchema"));
			Map<String, Object> schema = asSchema(raw.get("schema"));
			boolean jsonPayload = raw.containsKey("jsonPayload")
					? Boolean.TRUE.equals(raw.get("jsonPayload"))
					: isJsonSchema(payloadSchema) || isJsonSchema(schema);
			return new SinkBinding(string(raw.get("sinkId")), string(raw.get("endpoint")),
				string(raw.get("payloadGroup")), string(raw.get("valuePath")),
						payloadSchema, schema, jsonPayload);
		}

		private static String string(Object value) {
			return value == null ? null : String.valueOf(value);
		}

		@SuppressWarnings("unchecked")
		private static Map<String, Object> asSchema(Object value) {
			if (!(value instanceof Map<?, ?> map)) return null;
			Map<String, Object> result = new LinkedHashMap<>();
			map.forEach((key, schemaValue) -> result.put(String.valueOf(key), schemaValue));
			return result;
		}
	}
}
