/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * Turns an authored model capture into what aas-model publishes: {@code {aas_id}} and {@code {aas_id_short}}
 * resolved ({@code id_injector}), and every Operation given its {@code invocationDelegation} URL
 * ({@code invocation_authoring}): {@code http://dmp:8080/operations/<aas idShort>/<skill>}, the skill being
 * the idShort of the collection holding the Operation.
 */
public final class PublishedModels {
	public static final String INVOCATION_ORIGIN = "http://dmp:8080";

	private PublishedModels() { }

	public static String publish(String submodelJson, String aasId, String aasIdShort) {
		JsonElement model = JsonParser.parseString(submodelJson);
		return resolve(model, aasId, aasIdShort, null).toString();
	}

	private static JsonElement resolve(JsonElement value, String aasId, String aasIdShort, String parentIdShort) {
		if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString())
			return new JsonPrimitive(value.getAsString().replace("{aas_id}", aasId).replace("{aas_id_short}", aasIdShort));
		if (value.isJsonArray()) {
			JsonArray array = new JsonArray();
			for (JsonElement item : value.getAsJsonArray()) array.add(resolve(item, aasId, aasIdShort, parentIdShort));
			return array;
		}
		if (!value.isJsonObject()) return value;
		JsonObject object = value.getAsJsonObject();
		String idShort = object.has("idShort") && object.get("idShort").isJsonPrimitive() ? object.get("idShort").getAsString() : parentIdShort;
		JsonObject result = new JsonObject();
		for (var entry : object.entrySet()) result.add(entry.getKey(), resolve(entry.getValue(), aasId, aasIdShort, idShort));
		if ("Operation".equals(text(object, "modelType"))) delegate(result, aasIdShort, parentIdShort);
		return result;
	}

	private static void delegate(JsonObject operation, String aasIdShort, String skill) {
		JsonArray qualifiers = operation.has("qualifiers") ? operation.getAsJsonArray("qualifiers") : new JsonArray();
		for (JsonElement qualifier : qualifiers) {
			JsonObject object = qualifier.getAsJsonObject();
			if (!"invocationDelegation".equals(text(object, "type"))) continue;
			object.addProperty("value", object.get("value").getAsString().replace("{aas_id_short}", segment(aasIdShort))
					.replace("{skill}", segment(String.valueOf(skill))));
			return;
		}
		JsonObject qualifier = new JsonObject();
		qualifier.addProperty("kind", "ConceptQualifier");
		qualifier.addProperty("type", "invocationDelegation");
		qualifier.addProperty("valueType", "xs:string");
		qualifier.addProperty("value", INVOCATION_ORIGIN + "/operations/" + segment(aasIdShort) + "/" + segment(String.valueOf(skill)));
		qualifiers.add(qualifier);
		operation.add("qualifiers", qualifiers);
	}

	private static String segment(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}

	private static String text(JsonObject object, String name) {
		JsonElement value = object.get(name);
		return value == null || !value.isJsonPrimitive() ? null : value.getAsString();
	}
}
