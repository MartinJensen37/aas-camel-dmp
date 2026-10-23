/*******************************************************************************
 * Copyright (C) 2024 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.core.configuration.route.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Dmp authors, jungjan
 */
public class RouteConfiguration {

	/** Trigger data of a mapping's response half (DMP extensions ResponseSources/-Transformation/-Sinks). */
	public static final String RESPONSE_TRANSFORMER = "responseTransformer";
	public static final String RESPONSE_SOURCE_BINDINGS = "responseSourceBindings";
	public static final String RESPONSE_SINK_BINDINGS = "responseSinkBindings";
	private String trigger;
	private String routeId;
	private List<String> datasources = new ArrayList<>();
	private List<String> transformers = new ArrayList<>();
	private List<String> datasinks = new ArrayList<>();

	private Map<String, Object> triggerData = new HashMap<>();

	public RouteConfiguration() {
	}

	/**
	 * @param trigger
	 *            the route creator's trigger
	 * @param datasources
	 *            the datasources feeding the route (fan-in)
	 * @param transformers
	 *            the transformers applied once per evaluation, in order
	 * @param datasinks
	 *            the datasinks the result is dispatched to, in order
	 */
	public RouteConfiguration(String trigger, List<String> datasources, List<String> transformers, List<String> datasinks) {
		this.trigger = trigger;
		this.datasources = datasources;
		this.transformers = transformers;
		this.datasinks = datasinks;
	}

	public RouteConfiguration(RouteConfiguration configuration) {
		this(configuration.getRouteTrigger(), configuration.getDatasources(), configuration.getTransformers(), configuration.getDatasinks());
		setRouteId(configuration.getRouteId());
		this.triggerData = configuration.triggerData;
	}

	/** What the route creator needs beyond sources, transformers and sinks, as the compiler set it. */
	public Map<String, Object> getTriggerData() {
		return triggerData;
	}

	/** The response half's Lua transformer id (ResponseTransformation), or null. */
	public String getResponseTransformer() {
		Object value = triggerData == null ? null : triggerData.get(RESPONSE_TRANSFORMER);
		return value == null ? null : String.valueOf(value);
	}

	/** ResponseSources: each {@code sourceId} with what it projects ({@code sinkId} and {@code valuePath}). */
	public List<Map<String, Object>> getResponseSourceBindings() {
		return mapList(RESPONSE_SOURCE_BINDINGS);
	}

	/** ResponseSinks: each {@code sinkId} with its {@code dataSinkId} and payload binding. */
	public List<Map<String, Object>> getResponseSinkBindings() {
		return mapList(RESPONSE_SINK_BINDINGS);
	}

	private List<Map<String, Object>> mapList(String key) {
		Object value = triggerData == null ? null : triggerData.get(key);
		if (!(value instanceof List<?> list)) return List.of();
		List<Map<String, Object>> out = new java.util.ArrayList<>();
		for (Object item : list) {
			if (item instanceof Map<?, ?> map) {
				Map<String, Object> entry = new java.util.LinkedHashMap<>();
				map.forEach((k, v) -> entry.put(String.valueOf(k), v));
				out.add(java.util.Collections.unmodifiableMap(entry));
			}
		}
		return List.copyOf(out);
	}

	public void setTriggerData(Map<String, Object> triggerData) {
		this.triggerData = triggerData;
	}

	public List<String> getDatasinks() {
		return datasinks;
	}

	public void setDatasinks(List<String> datasinks) {
		this.datasinks = datasinks;
	}

	public String getRouteId() {
		return routeId;
	}

	public void setRouteId(String routeId) {
		this.routeId = routeId;
	}

	public String getRouteTrigger() {
		return trigger;
	}

	public void setRouteTrigger(String trigger) {
		this.trigger = trigger;
	}

	public List<String> getTransformers() {
		return transformers;
	}

	public void setTransformers(List<String> transformers) {
		this.transformers = transformers;
	}

	/**
	 * The datasource ids feeding this route (fan-in, SPEC §6). A route may
	 * aggregate multiple sources into one exchange before the transform.
	 */
	public List<String> getDatasources() {
		return datasources;
	}

	public void setDatasources(List<String> datasources) {
		this.datasources = datasources;
	}

}
