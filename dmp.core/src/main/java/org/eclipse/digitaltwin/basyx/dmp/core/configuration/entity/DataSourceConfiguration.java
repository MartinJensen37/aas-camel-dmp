/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity;

import java.util.Map;

/**
 * A generic class of Data source configurations
 * @author haque
 *
 */
public abstract class DataSourceConfiguration extends RouteEntity {
	private String serverUrl;
	private int serverPort;
	private String sourceId;
	private String valuePath;
	/**
	 * How this source delivers data, classified by the management node from
	 * the AID (SPEC §6.1, ADR-018): {@code "async"} = push/subscription,
	 * {@code "sync"} = request/response polled per cycle. Absent → async
	 * (the historical event-route default).
	 */
	private String delivery;
	/**
	 * Poll cadence of a SYNC source in milliseconds (decision 2026-09-15):
	 * the sync source is read at this interval by the mapping route and its
	 * value feeds the same latest-value cache as the async sources. A typed
	 * AAS Property source must never rely on an implicit cadence: the
	 * management node emits a positive effective interval for polling mode.
	 */
	private long pollingIntervalMs;
	/** Typed connector kind for sources whose transport body needs adaptation. */
	private String sourceKind;
	/** Resolved source mode, such as {@code poll} or {@code event}. */
	private String mode;
	/** Connector-specific filter options; never interpreted as a Camel URI. */
	private Map<String, Object> eventFilter;

	public DataSourceConfiguration() {
		super();
	}
	
	public DataSourceConfiguration(String uniqueId, String serverUrl, int serverPort) {
		super(uniqueId);
		this.serverUrl = serverUrl;
		this.serverPort = serverPort;
	}

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

	/**
	 * The AIMC SourceId this source contributes to the {@code sources} map
	 * (SPEC §6.4). Mirrors the sink-side {@code valuePath}: the key under
	 * which this source's payload is placed for the Lua transform.
	 */
	public String getSourceId() {
		return sourceId;
	}

	public void setSourceId(String sourceId) {
		this.sourceId = sourceId;
	}

	/**
	 * Source-side fine resolution (ADR-014 / SPEC §6.3): a JSON Pointer
	 * (e.g. {@code $.State}) applied to this source's canonical payload at the
	 * DMP boundary. When set, only the extracted leaf is delivered under the
	 * {@code SourceId} (disassembly); absent → the whole payload (coarse,
	 * current behaviour). The management node pins it per fine source; the DMP
	 * only applies what it was given.
	 */
	public String getValuePath() {
		return valuePath;
	}

	public void setValuePath(String valuePath) {
		this.valuePath = valuePath;
	}

	/**
	 * The source's delivery role ("async"/"sync", ADR-018); absent → async.
	 */
	public String getDelivery() {
		return delivery == null || delivery.isBlank() ? "async" : delivery;
	}

	public void setDelivery(String delivery) {
		this.delivery = delivery;
	}

	public long getPollingIntervalMs() {
		return pollingIntervalMs;
	}

	public void setPollingIntervalMs(long pollingIntervalMs) {
		this.pollingIntervalMs = pollingIntervalMs;
	}

	public String getSourceKind() {
		return sourceKind;
	}

	public void setSourceKind(String sourceKind) {
		this.sourceKind = sourceKind;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public Map<String, Object> getEventFilter() {
		return eventFilter;
	}

	public void setEventFilter(Map<String, Object> eventFilter) {
		this.eventFilter = eventFilter;
	}
}
