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

/**
 * A generic class of Data Sink Configuration
 * @author haque
 *
 */
public abstract class DataSinkConfiguration extends RouteEntity {
	private String sinkId;
	private String valuePath;

	public DataSinkConfiguration() {
		super();
	}
	
	public DataSinkConfiguration(String uniqueId) {
		super(uniqueId);
	}

	/**
	 * The AIMC SinkId this sink writes out of the transform's return object
	 * (SPEC §6.4). Mirrors the source-side {@code sourceId}: the key whose
	 * value this sink consumes. Concrete sinks (e.g. AAS) may expose this as
	 * {@code valuePath}.
	 */
	public String getSinkId() {
		return sinkId;
	}

	public void setSinkId(String sinkId) {
		this.sinkId = sinkId;
	}

	/**
	 * Sink-side fine resolution / assembly (ADR-014 / SPEC §6.5.4): a JSON
	 * Pointer (e.g. {@code $.SetPoint}) at which the {@code SinkId} value is
	 * placed in the outgoing payload after the transform. Absent → coarse
	 * (write the whole {@code SinkId} value as-is). The management node pins it
	 * per fine sink; the DMP only applies what it was given.
	 */
	public String getValuePath() {
		return valuePath;
	}

	public void setValuePath(String valuePath) {
		this.valuePath = valuePath;
	}

	/**
	 * Whether this sink producer applies its {@code valuePath} internally (e.g.
	 * the AAS sink extracts {@code node.get(valuePath)} itself) and therefore
	 * must NOT additionally be wrapped by the generic route-level assembly
	 * processor.
	 */
	public boolean handlesValuePathInternally() {
		return false;
	}
}
