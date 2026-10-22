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
package dk.aau.smartproductionlab.dmp.core.configuration.entity;

/**
 * A generic class of Data transformer configuration
 * @author haque
 *
 */
public abstract class DataTransformerConfiguration extends RouteEntity {

	/**
	 * Exchange property: a SourceId-keyed map a transformer receives beside its sources. Set
	 * for an Operation's response transformation: the values its request transformation received.
	 */
	public static final String REQUEST_SOURCES = "dmp.transformer.requestSources";

	public DataTransformerConfiguration() {
		super();
	}
	
	public DataTransformerConfiguration(String uniqueId) {
		super(uniqueId);
	}

	/**
	 * The Lua envelope key (the AIMC SourceId the blob's {@code sources}
	 * table is keyed by) — only the Lua transformer carries it; the base
	 * contract returns {@code null}.
	 */
	public String getSourceKeys() {
		return null;
	}
}
