/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package org.eclipse.digitaltwin.basyx.dmp.core.security;

import java.util.Objects;
import java.util.UUID;

import org.apache.camel.CamelContext;
import org.eclipse.digitaltwin.basyx.dmp.core.configuration.entity.ManagedRouteResource;

/**
 * Credentials kept out of endpoint URIs. They are bound in the Camel registry under an opaque name while the
 * owning bundle lives, and an endpoint URI only names them: {@code userName=#bean:<name>.user&password=#bean:<name>.password}.
 * Camel resolves the references when it creates the endpoint, so route definitions, endpoint registries and
 * diagnostics see the names, never the secret.
 */
public final class BoundCredential implements ManagedRouteResource {
	private final String name = "dmp-credential-" + UUID.randomUUID();
	private final String userName;
	private final String password;
	private CamelContext boundContext;
	private int owners;

	public BoundCredential(String userName, String password) {
		this.userName = Objects.requireNonNull(userName, "userName");
		this.password = password == null ? "" : password;
	}

	/** The opaque registry name; not a secret. */
	public String name() { return name; }
	public String userNameReference() { return "#bean:" + name + ".user"; }
	public String passwordReference() { return "#bean:" + name + ".password"; }

	/** The bound user name, for components that cannot take a reference. */
	public String userName() { return userName; }
	public String password() { return password; }

	@Override public synchronized void bind(CamelContext context) {
		if (boundContext != null) {
			if (boundContext != context) throw new IllegalStateException("credential is bound in another CamelContext");
			owners++;
			return;
		}
		context.getRegistry().bind(name + ".user", userName);
		context.getRegistry().bind(name + ".password", password);
		boundContext = context;
		owners = 1;
	}

	@Override public synchronized void close() {
		if (boundContext == null) return;
		if (--owners > 0) return;
		boundContext.getRegistry().unbind(name + ".user");
		boundContext.getRegistry().unbind(name + ".password");
		boundContext = null;
	}

	@Override public String toString() { return "BoundCredential[" + name + ", <redacted>]"; }
}
