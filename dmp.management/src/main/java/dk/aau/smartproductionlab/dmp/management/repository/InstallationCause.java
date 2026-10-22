/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package dk.aau.smartproductionlab.dmp.management.repository;

import java.util.Locale;

/**
 * Why a bundle failed to start, as one of a fixed set of causes. The status reports the cause, never the
 * exception text: that can name endpoint URIs, and an OPC UA URI carries its credentials. The full chain
 * is in the log.
 */
public final class InstallationCause {
	public static final String MULTIPLE_CONSUMERS_UNSUPPORTED = "multiple-consumers-unsupported";
	public static final String UNKNOWN_HOST = "unknown-host";
	public static final String CONNECTION_REFUSED = "connection-refused";
	public static final String TIMEOUT = "timeout";
	public static final String AUTHENTICATION_FAILED = "authentication-failed";
	public static final String ENDPOINT_INVALID = "endpoint-invalid";
	public static final String UNKNOWN = "unknown";

	private InstallationCause() { }

	/** The first recognised cause along the exception chain, or {@link #UNKNOWN}. */
	public static String of(Throwable failure) {
		java.util.Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
		for (Throwable current = failure; current != null && seen.add(current); current = current.getCause()) {
			String message = String.valueOf(current.getMessage()).toLowerCase(Locale.ROOT);
			if (message.contains("multiple consumers for the same endpoint")) return MULTIPLE_CONSUMERS_UNSUPPORTED;
			if (current instanceof java.net.UnknownHostException) return UNKNOWN_HOST;
			if (current instanceof java.net.ConnectException || message.contains("connection refused")) return CONNECTION_REFUSED;
			if (current instanceof java.net.SocketTimeoutException || current instanceof java.util.concurrent.TimeoutException)
				return TIMEOUT;
			if (message.contains("bad_useraccessdenied") || message.contains("bad_identitytoken")
					|| message.contains("not authorized") || message.contains("unauthorized")) return AUTHENTICATION_FAILED;
			if (current instanceof org.apache.camel.ResolveEndpointFailedException) return ENDPOINT_INVALID;
		}
		return UNKNOWN;
	}
}
