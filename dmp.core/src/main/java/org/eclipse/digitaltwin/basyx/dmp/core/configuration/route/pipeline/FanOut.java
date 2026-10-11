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
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.pipeline;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.model.ProcessorDefinition;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.DataSinkConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.EndpointIdentityProvider;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RouteConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.RoutesConfiguration;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonPointer;
import org.eclipse.digitaltwin.basyx.dmp.core.util.ConnectorJsonPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fan-out: the mirror image of the fan-in half (SPEC §6.4).
 *
 * <p>Both halves follow the same shape and share one vocabulary:
 *
 * <pre>
 *   fan-in    N source contributions ─▶ ONE {@link KeyedSnapshot} keyed by SourceId
 *             (disassembled at the source, gated on completeness)
 *   transform ONE evaluation of the shared snapshot
 *   fan-out   the shared result      ─▶ ONE {@link KeyedSnapshot} keyed by SinkId
 *             (parsed once)            ─▶ one payload per device affordance
 * </pre>
 *
 * <p>The invariants are explicit here, not inherited from a framework detail:
 *
 * <ol>
 * <li>the transform result is parsed <b>once</b> into a single immutable
 * {@link KeyedSnapshot};</li>
 * <li>each sink contribution is derived from that snapshot, never from a
 * sibling's output;</li>
 * <li>members of a device affordance are staged in the current exchange and
 * validated together before its single send.</li>
 * </ol>
 *
 * <p>Sinks are emitted in configured order through one sequential Split — no
 * {@code multicast} (whose contract is "the <i>same</i> message to N endpoints",
 * not "N differently shaped messages") and no per-sink {@code direct:} branch
 * routes, so route count stays a function of mappings rather than of sinks.
 */
public final class FanOut {

	private static final Logger LOGGER = LoggerFactory.getLogger(FanOut.class);

	/** Exchange property carrying the one shared, immutable result snapshot. */
	private static final String RESULT_PROPERTY = "dmp.sharedResult";
	/** Exchange property carrying the shared result serialised for a sink. */
	private static final String SINK_PAYLOADS_PROPERTY = "dmp.sharedPayload";
	/** Exchange property carrying the sinks that failed, if any. */
	private static final String SINK_FAILURES_PROPERTY = "dmp.sinkFailures";
	private static final String GROUP_PAYLOADS_PROPERTY = "dmp.groupPayloads";
	private static final String GROUP_FAILURES_PROPERTY = "dmp.groupFailures";
	private static final String MEMBER_VALUE_PROPERTY = "dmp.memberValue";
	private static final String BRANCH_BASE_PROPERTIES = "dmp.branchBaseProperties";
	private static final String BRANCH_BASE_HEADERS = "dmp.branchBaseHeaders";
	private static final String DISPATCH_JOBS_PROPERTY = "dmp.dispatchJobs";
	private static final String DISPATCH_OUTCOMES_PROPERTY = "dmp.dispatchOutcomes";
	private static final String CURRENT_DISPATCH_JOB = "dmp.currentDispatchJob";
	private static final String DISPATCH_ENDPOINT_HEADER = "dmp.dispatchEndpoint";

	private FanOut() {
	}

	/**
	 * Appends transform + fan-out to {@code route}.
	 *
	 * @param transformerEndpoints
	 *            route-level transformers (run once)
	 * @param perSinkTransformers
	 *            optional heterogeneous {@code datasinkMappingConfiguration}
	 *            sequences keyed by sink ENDPOINT URI; when every sink maps to
	 *            the same sequence it is hoisted to run once instead.
	 */
	public static void apply(ProcessorDefinition<?> route, RoutesConfiguration routesConfiguration,
			RouteConfiguration routeConfig, String[] transformerEndpoints, String[] sinkEndpoints,
			Map<String, String[]> perSinkTransformers, String routeId) {
		apply(route, routesConfiguration, routeConfig, transformerEndpoints, sinkEndpoints,
				perSinkTransformers, routeId, AffordanceValidation.configuredMode());
	}

	static void apply(ProcessorDefinition<?> route, RoutesConfiguration routesConfiguration,
			RouteConfiguration routeConfig, String[] transformerEndpoints, String[] sinkEndpoints,
			Map<String, String[]> perSinkTransformers, String routeId, AffordanceValidation.Mode mode) {
		List<String> sinkIds = routeConfig.getDatasinks();
		String[] configIds = new String[sinkEndpoints.length];
		for (int i = 0; i < sinkEndpoints.length; i++) {
			configIds[i] = i < sinkIds.size() ? sinkIds.get(i)
					: sinkIdForEndpoint(routesConfiguration, routeConfig, sinkEndpoints[i]);
		}
		if (perSinkTransformers != null && !perSinkTransformers.isEmpty()) {
			String[] hoisted = hoistCommonSequence(perSinkTransformers, configIds,
					sinkEndpoints);
			if (hoisted != null) {
				perSinkTransformers = null; // fully covered; one shared sequence below
				transformerEndpoints = hoisted;
			} else {
				// heterogeneous per-sink sequences stay per-sink (override
				// semantics); no route-level pass.
				transformerEndpoints = new String[0];
			}
		}

		if (transformerEndpoints != null && transformerEndpoints.length > 0) {
			route.process(exchange -> LOGGER.debug("route {}: body before transformer: {}",
					routeId, exchange.getMessage().getBody(String.class)));
			route.to(transformerEndpoints);
		}

		if (sinkEndpoints.length == 0) {
			LOGGER.warn("route {}: no sink endpoints", routeId);
			return;
		}

		String[] groupKeys = new String[sinkEndpoints.length];
		Map<String, Integer> groupLast = new LinkedHashMap<>();
		Map<String, String> groupEndpoints = new LinkedHashMap<>();
		Map<String, Object> groupSchemas = new LinkedHashMap<>();
		Map<String, AffordanceValidation> payloadValidators = new LinkedHashMap<>();
		Map<String, AffordanceValidation> memberValidators = new LinkedHashMap<>();
		for (int i = 0; i < sinkEndpoints.length; i++) {
			DataSinkConfiguration config = routesConfiguration.getDatasinks().get(configIds[i]);
			memberValidators.put(configIds[i], new AffordanceValidation(
					config == null ? null : config.getSchema(), "sink " + configIds[i], mode));
			if (config != null && !config.handlesValuePathInternally()
					&& config.getPayloadGroup() != null && !config.getPayloadGroup().isBlank()) {
				String group = config.getPayloadGroup();
				groupKeys[i] = group;
				String previousEndpoint = groupEndpoints.putIfAbsent(group, sinkEndpoints[i]);
				if (previousEndpoint != null
						&& !sameDestination(previousEndpoint, sinkEndpoints[i])) {
					throw new IllegalArgumentException("payloadGroup " + group + " has different endpoint URIs");
				}
				if (groupSchemas.containsKey(group) && !Objects.equals(groupSchemas.get(group), config.getPayloadSchema())) {
					throw new IllegalArgumentException("payloadGroup " + group + " has different payload schemas");
				}
				groupSchemas.put(group, config.getPayloadSchema());
				groupLast.put(group, i);
				payloadValidators.putIfAbsent(group, new AffordanceValidation(config.getPayloadSchema(),
						"payloadGroup " + group, mode));
			}
		}

		// invariant 1: the shared result is parsed ONCE into one immutable
		// snapshot; every sink below starts from that same value
		route.process(exchange -> {
			KeyedSnapshot result = KeyedSnapshot.parse(exchange.getMessage().getBody());
			exchange.setProperty(RESULT_PROPERTY, result);
			exchange.setProperty(SINK_PAYLOADS_PROPERTY, JsonCodec.copyValue(result.root()));
			exchange.setProperty(SINK_FAILURES_PROPERTY, new java.util.ArrayList<String>());
			exchange.setProperty(GROUP_PAYLOADS_PROPERTY, new LinkedHashMap<String, Object>());
			exchange.setProperty(GROUP_FAILURES_PROPERTY, new java.util.HashSet<String>());
			exchange.setProperty(DISPATCH_JOBS_PROPERTY, new ArrayList<DispatchJob>());
			exchange.setProperty(BRANCH_BASE_PROPERTIES,
					copyProperties(exchange.getProperties()));
			exchange.setProperty(BRANCH_BASE_HEADERS, copyMap(exchange.getMessage().getHeaders()));
			LOGGER.debug("route {}: body after transformer: {}",
					routeId, exchange.getMessage().getBody());
		});

		// invariant 2 + 3: each sink's body is derived from the shared value
		// (never from a sibling's output) and no per-branch message copying is
		// relied upon. Sinks are emitted in configured order from one route, so
		// route count is a function of mappings, not of sinks.
		final Map<String, String[]> sinkTransformers = perSinkTransformers;
		for (int i = 0; i < sinkEndpoints.length; i++) {
			final String configId = configIds[i];
			final String group = groupKeys[i];
			final boolean sendGroup = group != null && groupLast.get(group) == i;
			final String[] extra = sinkTransformers == null ? null
					: sinkTransformers.containsKey(configId) ? sinkTransformers.get(configId)
					: sinkTransformers.get(sinkEndpoints[i]);
			// start every sink from the shared result; an unmapped sink is
			// already final, a mapped one still needs its own transformer chain
			route.process(exchange -> {
				restoreBranchState(exchange);
				KeyedSnapshot result = snapshotOf(exchange);
				if (extra == null || extra.length == 0) {
					exchange.setProperty(MEMBER_VALUE_PROPERTY, selectedValue(result, configId, routesConfiguration));
					exchange.getMessage().setBody(assemble(result, configId, routesConfiguration));
				} else {
					exchange.getMessage().setBody(JsonCodec.copyValue(result.root()));
				}
			});
			if (extra != null && extra.length > 0) {
				// per-sink override semantics: this chain runs on its sink's
				// copy only, and its output is what gets assembled below
				route.to(extra);
				route.process(exchange -> {
					KeyedSnapshot transformed = KeyedSnapshot.parse(exchange.getMessage().getBody());
					exchange.setProperty(MEMBER_VALUE_PROPERTY, selectedValue(transformed, configId, routesConfiguration));
					exchange.getMessage().setBody(assemble(transformed, configId, routesConfiguration));
				});
			}
			// every sink is attempted; failures are collected and reported once
			// at the end rather than aborting the remaining sinks
			final String endpoint = group == null ? sinkEndpoints[i] : groupEndpoints.get(group);
			org.apache.camel.model.TryDefinition attempt = route.doTry();
			attempt.process(e -> {
						Object payload = e.getMessage().getBody();
						DataSinkConfiguration config = routesConfiguration.getDatasinks().get(configId);
						if (config != null && !config.handlesValuePathInternally() && config.getValuePath() != null
								&& !config.getValuePath().isBlank()) {
							Object member = JsonPointer.extract(payload, config.getValuePath())
									.orElseThrow(() -> new IllegalArgumentException("Sink " + configId
											+ ": valuePath does not resolve in assembled body"));
							memberValidators.get(configId).outgoing(member == JsonPointer.JSON_NULL ? null : member);
						} else {
							memberValidators.get(configId).outgoing(e.getProperty(MEMBER_VALUE_PROPERTY));
						}
						if (group != null) {
							if (sendGroup && groupFailures(e).contains(group)) {
								throw new IllegalArgumentException("payloadGroup " + group + " has a failed member");
							}
							Map<String, Object> staged = groupPayloads(e);
							staged.put(group, staged.containsKey(group)
									? merge(staged.get(group), payload) : payload);
							if (sendGroup) {
								Object merged = staged.remove(group);
								payloadValidators.get(group).outgoing(merged);
								e.getMessage().setBody(merged);
							}
						}
					});
			if (group == null || sendGroup) {
				boolean jsonPayload = declaresJsonPayload(routesConfiguration, configId);
				attempt.process(e -> {
					dispatchJobs(e).add(new DispatchJob(routeId, configId, endpoint,
							e.getMessage().getBody(), jsonPayload));
				});
			}
			attempt.doCatch(Exception.class)
					.process(e -> {
						if (group != null) {
							groupFailures(e).add(group);
						}
						sinkFailures(e).add(endpoint);
						LOGGER.warn("route {}: sink {} ({}) failed",
								routeId, configId, endpoint, e);
					})
					.end();
		}

		var split = route.split(org.apache.camel.builder.Builder.exchangeProperty(DISPATCH_JOBS_PROPERTY));
		split.aggregationStrategy(new DispatchOutcomeAggregationStrategy());
		split.onPrepare(FanOut::restoreBranchMetadata);
		split.process(exchange -> {
			DispatchJob job = (DispatchJob) exchange.getMessage().getBody();
			stripFanOutState(exchange);
			exchange.setProperty(CURRENT_DISPATCH_JOB, job);
			exchange.getMessage().setHeader(DISPATCH_ENDPOINT_HEADER, job.endpoint());
			if (job.jsonPayload()) exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/json");
			else exchange.getMessage().removeHeader(Exchange.CONTENT_TYPE);
			Object payload = job.payload();
			if (job.jsonPayload() && payload instanceof String) {
				payload = new ConnectorJsonPayload(payload);
			}
			exchange.getMessage().setBody(payload);
		});
		var send = split.doTry();
		var endpointChoice = send.choice();
		for (String dispatchEndpoint : Arrays.stream(sinkEndpoints).distinct().toList()) {
			endpointChoice.when(exchange -> dispatchEndpoint.equals(
					exchange.getMessage().getHeader(DISPATCH_ENDPOINT_HEADER, String.class)))
					.to(dispatchEndpoint);
		}
		endpointChoice.otherwise().process(exchange -> {
			throw new IllegalStateException("fan-out job references an unconfigured sink endpoint");
		});
		endpointChoice.end();
		send.process(exchange -> exchange.getMessage().setBody(
				new DispatchOutcome((DispatchJob) exchange.getProperty(CURRENT_DISPATCH_JOB), null)));
		send.doCatch(Exception.class)
				.process(exchange -> exchange.getMessage().setBody(new DispatchOutcome(
						(DispatchJob) exchange.getProperty(CURRENT_DISPATCH_JOB),
						exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class))))
				.end();
		split.end();
		route.process(FanOut::collectDispatchOutcomes);
		route.process(exchange -> {
			restoreBranchState(exchange);
			exchange.removeProperty(BRANCH_BASE_PROPERTIES);
			exchange.removeProperty(BRANCH_BASE_HEADERS);
			reportFailures(exchange);
		});
	}

	private static final class DispatchOutcomeAggregationStrategy implements AggregationStrategy {
		@Override
		@SuppressWarnings("unchecked")
		public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
			Exchange aggregate = oldExchange == null ? newExchange : oldExchange;
			List<DispatchOutcome> outcomes = aggregate.getProperty(DISPATCH_OUTCOMES_PROPERTY, List.class);
			if (outcomes == null) {
				outcomes = new ArrayList<>();
				aggregate.setProperty(DISPATCH_OUTCOMES_PROPERTY, outcomes);
			}
			outcomes.add(newExchange.getMessage().getBody(DispatchOutcome.class));
			return aggregate;
		}
	}

	private record DispatchJob(String routeId, String configId, String endpoint, Object payload,
			boolean jsonPayload) { }

	private record DispatchOutcome(DispatchJob job, Exception failure) { }

	@SuppressWarnings("unchecked")
	private static List<DispatchJob> dispatchJobs(Exchange exchange) {
		return (List<DispatchJob>) exchange.getProperty(DISPATCH_JOBS_PROPERTY);
	}

	private static boolean declaresJsonPayload(RoutesConfiguration configuration, String configId) {
		DataSinkConfiguration sink = configuration.getDatasinks().get(configId);
		return sink != null && sink.requiresJsonWireEncoding();
	}

	private static void collectDispatchOutcomes(Exchange exchange) {
		List<DispatchOutcome> outcomes = exchange.getProperty(DISPATCH_OUTCOMES_PROPERTY, List.class);
		if (outcomes == null) return;
		for (DispatchOutcome outcome : outcomes) {
			if (outcome.failure() == null) continue;
			sinkFailures(exchange).add(outcome.job().endpoint());
			LOGGER.warn("route {}: sink {} ({}) failed",
					outcome.job().routeId(), outcome.job().configId(), outcome.job().endpoint(), outcome.failure());
		}
	}

	@SuppressWarnings("unchecked")
	private static void restoreBranchState(org.apache.camel.Exchange exchange) {
		restoreBranchMetadata(exchange);
		exchange.getMessage().setBody(JsonCodec.copyValue(snapshotOf(exchange).root()));
	}

	@SuppressWarnings("unchecked")
	private static void restoreBranchMetadata(org.apache.camel.Exchange exchange) {
		Map<String, Object> properties = (Map<String, Object>) exchange.getProperty(BRANCH_BASE_PROPERTIES);
		Map<String, Object> headers = (Map<String, Object>) exchange.getProperty(BRANCH_BASE_HEADERS);
		if (properties == null || headers == null) return;
		exchange.getProperties().clear();
		exchange.getProperties().putAll(copyProperties(properties));
		exchange.setProperty(BRANCH_BASE_PROPERTIES, properties);
		exchange.setProperty(BRANCH_BASE_HEADERS, headers);
		exchange.getMessage().setHeaders(copyMap(headers));
	}

	private static void stripFanOutState(Exchange exchange) {
		exchange.removeProperty(RESULT_PROPERTY);
		exchange.removeProperty(SINK_PAYLOADS_PROPERTY);
		exchange.removeProperty(SINK_FAILURES_PROPERTY);
		exchange.removeProperty(GROUP_PAYLOADS_PROPERTY);
		exchange.removeProperty(GROUP_FAILURES_PROPERTY);
		exchange.removeProperty(DISPATCH_JOBS_PROPERTY);
		exchange.removeProperty(BRANCH_BASE_PROPERTIES);
		exchange.removeProperty(BRANCH_BASE_HEADERS);
	}

	private static Map<String, Object> copyProperties(Map<String, Object> properties) {
		Map<String, Object> copy = new LinkedHashMap<>();
		properties.forEach((key, value) -> copy.put(key,
				isFanOutState(key) ? value : copyMetadata(value)));
		return copy;
	}

	private static boolean isFanOutState(String key) {
		return RESULT_PROPERTY.equals(key) || SINK_PAYLOADS_PROPERTY.equals(key)
				|| SINK_FAILURES_PROPERTY.equals(key) || GROUP_PAYLOADS_PROPERTY.equals(key)
				|| GROUP_FAILURES_PROPERTY.equals(key) || DISPATCH_JOBS_PROPERTY.equals(key)
				|| BRANCH_BASE_PROPERTIES.equals(key)
				|| BRANCH_BASE_HEADERS.equals(key);
	}

	private static Map<String, Object> copyMap(Map<String, Object> values) {
		Map<String, Object> copy = new LinkedHashMap<>();
		values.forEach((key, value) -> copy.put(key, copyMetadata(value)));
		return copy;
	}

	private static Object copyMetadata(Object value) {
		if (value instanceof Map<?, ?> map) {
			Map<String, Object> copy = new LinkedHashMap<>();
			map.forEach((key, member) -> copy.put(String.valueOf(key), copyMetadata(member)));
			return copy;
		}
		if (value instanceof List<?> list) {
			List<Object> copy = new ArrayList<>(list.size());
			list.forEach(member -> copy.add(copyMetadata(member)));
			return copy;
		}
		if (value instanceof Set<?> set) {
			Set<Object> copy = new LinkedHashSet<>();
			set.forEach(member -> copy.add(copyMetadata(member)));
			return copy;
		}
		if (value != null && value.getClass().isArray()) {
			int length = java.lang.reflect.Array.getLength(value);
			Object copy = java.lang.reflect.Array.newInstance(value.getClass().getComponentType(), length);
			for (int i = 0; i < length; i++) {
				java.lang.reflect.Array.set(copy, i, copyMetadata(java.lang.reflect.Array.get(value, i)));
			}
			return copy;
		}
		return value;
	}

	private static Object selectedValue(KeyedSnapshot result, String configId,
			RoutesConfiguration routesConfiguration) {
		DataSinkConfiguration config = routesConfiguration.getDatasinks().get(configId);
		if (config != null && !config.handlesValuePathInternally()
				&& config.getSinkId() != null && !config.getSinkId().isBlank()
				&& result.has(config.getSinkId())) {
			return result.value(config.getSinkId());
		}
		return result.root();
	}

	static boolean sameDestination(String first, String second) {
		if (Objects.equals(first, second)) return true;
		if (first == null || second == null) return false;
		List<EndpointIdentityProvider> providers = java.util.ServiceLoader
				.load(EndpointIdentityProvider.class, Thread.currentThread().getContextClassLoader())
				.stream().map(java.util.ServiceLoader.Provider::get).toList();
		EndpointIdentityProvider left = providers.stream().filter(provider -> provider.supports(first))
				.findFirst().orElse(null);
		EndpointIdentityProvider right = providers.stream().filter(provider -> provider.supports(second))
				.findFirst().orElse(null);
		return left != null && right != null && left.getClass().equals(right.getClass())
				&& left.identity(first).equals(right.identity(second));
	}

	@SuppressWarnings("unchecked")
	private static java.util.Set<String> groupFailures(org.apache.camel.Exchange exchange) {
		return (java.util.Set<String>) exchange.getProperty(GROUP_FAILURES_PROPERTY);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> groupPayloads(org.apache.camel.Exchange exchange) {
		return (Map<String, Object>) exchange.getProperty(GROUP_PAYLOADS_PROPERTY);
	}

	/** Merge disjoint nested paths, including array indices; conflicting leaves fail loudly. */
	private static Object merge(Object left, Object right) {
		if (left instanceof Map<?, ?> a && right instanceof Map<?, ?> b) {
			Map<String, Object> result = new LinkedHashMap<>();
			a.forEach((key, value) -> result.put(String.valueOf(key), value));
			b.forEach((key, value) -> {
				String name = String.valueOf(key);
				result.put(name, result.containsKey(name) ? merge(result.get(name), value) : value);
			});
			return result;
		}
		if (left instanceof List<?> a && right instanceof List<?> b) {
			List<Object> result = new ArrayList<>(a);
			while (result.size() < b.size()) {
				result.add(null);
			}
			for (int i = 0; i < b.size(); i++) {
				Object rightValue = b.get(i);
				if (i >= a.size()) result.set(i, rightValue);
				else result.set(i, merge(a.get(i), rightValue));
			}
			return result;
		}
		if (!Objects.equals(left, right)) {
			throw new IllegalArgumentException("Conflicting sink values in a payloadGroup: " + left + " vs " + right);
		}
		return left;
	}

	private static Object sharedValue(KeyedSnapshot result) {
		return JsonCodec.copyValue(result.root());
	}

	private static KeyedSnapshot snapshotOf(org.apache.camel.Exchange exchange) {
		return exchange.getProperty(RESULT_PROPERTY, KeyedSnapshot.class);
	}

	@SuppressWarnings("unchecked")
	private static List<String> sinkFailures(org.apache.camel.Exchange exchange) {
		return (List<String>) exchange.getProperty(SINK_FAILURES_PROPERTY);
	}

	/**
	 * Makes a partial fan-out visible: every sink was attempted, and a sink that
	 * failed must not pass as a successful write. Throwing here (after the fact)
	 * keeps both properties — all sinks attempted, and the failure reported.
	 * Change this to a debug log to make fan-out best-effort.
	 */
	private static void reportFailures(org.apache.camel.Exchange exchange) {
		List<String> failures = sinkFailures(exchange);
		if (failures.isEmpty()) {
			return;
		}
		throw new IllegalStateException("fan-out could not deliver to sinks: " + failures);
	}

	/** The same shaping step, over an arbitrary (possibly transformed) value. */
	private static Object assemble(KeyedSnapshot result, String dataSinkId,
			RoutesConfiguration routesConfiguration) {
		DataSinkConfiguration config = dataSinkId == null
				? null
				: routesConfiguration.getDatasinks().get(dataSinkId);
		if (config == null) return JsonCodec.copyValue(result.root());
		String sinkId = config.getSinkId();
		String valuePath = config.getValuePath();

		Object value = result.root();
		if (sinkId != null && !sinkId.isBlank()) {
			if (!result.has(sinkId)) {
				throw new IllegalArgumentException("Sink '" + dataSinkId
						+ "': SinkId '" + sinkId + "' not present in the transform result");
			}
			value = result.value(sinkId);
			if (value == null) value = JsonPointer.JSON_NULL;
		}
		if (config.handlesValuePathInternally()) return JsonCodec.copyValue(value);

		if (valuePath == null || valuePath.isBlank()) {
			return JsonCodec.copyValue(value);
		}
		return assembleAt(JsonPointer.parse(valuePath).tokens(), value);
	}

	/**
	 * The common transformer sequence when every sink endpoint maps to the
	 * identical non-null sequence; {@code String[0]} when they are all empty;
	 * {@code null} when the mapping is heterogeneous or does not cover all
	 * sinks.
	 */
	private static String[] hoistCommonSequence(Map<String, String[]> dataSinkMapping,
			String[] ids, String[] endpoints) {
		String[] keys = Arrays.stream(ids).allMatch(dataSinkMapping::containsKey) ? ids : endpoints;
		if (keys.length != dataSinkMapping.size()
				|| !Arrays.stream(keys).allMatch(dataSinkMapping::containsKey)) {
			return null;
		}
		String[] first = dataSinkMapping.get(keys[0]);
		boolean identical = Arrays.stream(keys)
				.allMatch(endpoint -> Arrays.equals(first, dataSinkMapping.get(endpoint)));
		return identical ? (first == null ? new String[0] : first) : null;
	}

	private static Object assembleAt(List<String> tokens, Object value) {
		return assembleAt(tokens, 0, value);
	}

	private static Object assembleAt(List<String> tokens, int offset, Object value) {
		if (offset == tokens.size()) {
			return value;
		}
		String token = tokens.get(offset);
		if (token.startsWith("[") && token.endsWith("]")) {
			int index = Integer.parseInt(token.substring(1, token.length() - 1));
			if (index < 0) {
				throw new IllegalArgumentException("negative JSON array index in sink path: " + token);
			}
			java.util.List<Object> array = new java.util.ArrayList<>();
			while (array.size() <= index) {
				array.add(null);
			}
			array.set(index, assembleAt(tokens, offset + 1, value));
			return array;
		}
		java.util.Map<String, Object> object = new java.util.LinkedHashMap<>();
		object.put(token, assembleAt(tokens, offset + 1, value));
		return object;
	}

	/**
	 * Reverse-looks-up the sink config id for a resolved endpoint URI (used in
	 * the sink-mapped multicast path whose keys are endpoint URIs).
	 */
	private static String sinkIdForEndpoint(RoutesConfiguration routesConfiguration,
			RouteConfiguration routeConfig, String endpointUri) {
		for (String id : routeConfig.getDatasinks()) {
			DataSinkConfiguration config = routesConfiguration.getDatasinks().get(id);
			if (config != null && endpointUri.equals(config.getConnectionURI())) {
				return id;
			}
		}
		return null;
	}
}
