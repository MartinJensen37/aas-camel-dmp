/*******************************************************************************
 * Copyright (C) 2026 the aas-camel-dmp Authors
 *
 * SPDX-License-Identifier: MIT
 ******************************************************************************/
package dk.aau.smartproductionlab.dmp.aas.event;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;

/**
 * {@code aas-event:<base64url submodel ID>.<base64url idShortPath>}: a consumer of repository events for one
 * element, fed by the control-plane event consumer through this context's {@link AasEventHub}.
 */
public final class AasEventComponent extends DefaultComponent {
	public static final String SCHEME = "aas-event";
	private final AasEventHub hub = new AasEventHub();

	public AasEventHub hub() { return hub; }

	/** The hub of a context's {@code aas-event} component, creating the component when needed. */
	public static AasEventHub hub(CamelContext context) {
		return context.getComponent(SCHEME, AasEventComponent.class).hub();
	}

	public static String uri(String submodelId, String idShortPath) {
		return SCHEME + ":" + encode(submodelId) + "." + encode(idShortPath);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
		int dot = remaining.indexOf('.');
		if (dot <= 0 || dot == remaining.length() - 1) throw new IllegalArgumentException("aas-event endpoint must name a submodel and an element");
		return new AasEventEndpoint(uri, this, decode(remaining.substring(0, dot)), decode(remaining.substring(dot + 1)));
	}

	private static String encode(String value) {
		return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String decode(String value) {
		return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
	}
}
