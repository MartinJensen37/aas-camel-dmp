/*******************************************************************************
 * Copyright (C) 2026 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.databridge.core.configuration.parser;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import org.eclipse.digitaltwin.basyx.databridge.core.configuration.route.core.RouteConfiguration;

/**
 * Gson deserializer for {@link RouteConfiguration} that accepts both the
 * singular {@code "datasource"} (string) and the plural {@code "datasources"}
 * (array) JSON keys, mapping either onto the single {@code datasources} list
 * (SPEC §6.4). This preserves backward compatibility with existing
 * {@code routes.json} files that use the singular key.
 */
public class RouteConfigurationDeserializer implements JsonDeserializer<RouteConfiguration> {

	@Override
	public RouteConfiguration deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
		JsonObject obj = json.getAsJsonObject();

		RouteConfiguration config = new RouteConfiguration();
		if (obj.has("trigger")) {
			config.setRouteTrigger(obj.get("trigger").getAsString());
		}
		if (obj.has("routeId")) {
			config.setRouteId(obj.get("routeId").getAsString());
		}
		if (obj.has("transformers")) {
			config.setTransformers(readStringList(obj.get("transformers")));
		}
		if (obj.has("datasinks")) {
			config.setDatasinks(readStringList(obj.get("datasinks")));
		}
		if (obj.has("datasources")) {
			config.setDatasources(readStringList(obj.get("datasources")));
		} else if (obj.has("datasource")) {
			// legacy singular key
			config.setDatasources(java.util.Collections.singletonList(obj.get("datasource").getAsString()));
		}
		if (obj.has("datasinkMappingConfiguration")) {
			config.setDatasinkMappingConfiguration(context.deserialize(obj.get("datasinkMappingConfiguration"), java.util.Map.class));
		}
		if (obj.has("triggerData")) {
			config.setTriggerData(context.deserialize(obj.get("triggerData"), java.util.Map.class));
		}
		return config;
	}

	private List<String> readStringList(JsonElement element) {
		List<String> result = new ArrayList<>();
		if (element.isJsonArray()) {
			for (JsonElement e : element.getAsJsonArray()) {
				result.add(e.getAsString());
			}
		} else if (element.isJsonPrimitive()) {
			result.add(element.getAsString());
		}
		return result;
	}
}