/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core;

/**
 * Exchange properties of a transformer call that receives a second argument,
 * {@code aimc_main(sources, invocation)}: the status and response
 * transformations of an operation mapping.
 */
public final class TransformerInvocation {
	/** The {@code invocation} argument; without it the transformer is called with {@code sources} only. */
	public static final String ARGUMENT_PROPERTY = "dmp.transformer.invocation";
	/**
	 * Marks a status check: its result may be a string ({@code "running"},
	 * {@code "done"}, {@code "failed"}) instead of a SinkId table.
	 */
	public static final String STATUS_CHECK_PROPERTY = "dmp.transformer.statusCheck";

	private TransformerInvocation() { }
}
