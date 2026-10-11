package org.eclipse.digitaltwin.basyx.dmp.paho.mqtt5.component;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.eclipse.digitaltwin.basyx.dmp.core.configuration.route.core.EndpointIdentityProvider;

/** Groups per-sink MQTT endpoints by broker/topic/options, excluding only clientId. */
public final class MqttEndpointIdentityProvider implements EndpointIdentityProvider {
	@Override
	public boolean supports(String endpointUri) {
		return endpointUri != null && (endpointUri.startsWith("paho:")
				|| endpointUri.startsWith("paho-mqtt5-inout:"));
	}

	@Override
	public String identity(String endpointUri) {
		int query = endpointUri.indexOf('?');
		if (query < 0) return endpointUri;
		String[] sharedOptions = endpointUri.substring(query + 1).split("&");
		return endpointUri.substring(0, query) + "?" + Arrays.stream(sharedOptions)
				.filter(option -> !option.startsWith("clientId="))
				.sorted().collect(Collectors.joining("&"));
	}
}
