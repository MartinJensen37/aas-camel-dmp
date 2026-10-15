/* Copyright (C) 2026 the aas-camel-dmp Authors
 * SPDX-License-Identifier: MIT */
package org.eclipse.digitaltwin.basyx.dmp.management.repository;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.eclipse.digitaltwin.basyx.dmp.core.util.JsonCodec;
import org.eclipse.digitaltwin.aas4j.v3.model.Property;
import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;
import org.eclipse.digitaltwin.basyx.dmp.aas.model.Aas;

/**
 * Safe identity extraction for repository invalidation events; raw event bodies are never logged.
 *
 * <p>Only the identity ({@link #type()}, {@link #resourceId()}, {@link #elementPath()}) is read up front, because
 * most events are dropped on it. Everything else is derived on first use and then kept: for an MQTT element event
 * the payload is not even parsed until needed. {@link #propertyShape()} is the updated Property without its value,
 * or null when the event is not a Property update; {@link #semanticIds()} are the semanticId key values of a
 * submodel carried in the event, or null when it carries none; {@link #canonical()} is the event in the BaSyx
 * Kafka shape ({@code type}, {@code id}, {@code smElementPath}, {@code smElement}, {@code submodel}).
 */
public final class AasControlEvent {
	private final String type, resourceId, elementPath;
	private final boolean fullRescan;
	private final Lazy<JsonObject> event;
	private final Lazy<Property> propertyShape;
	private final Lazy<Set<String>> semanticIds;
	private final Lazy<String> canonical;

	private AasControlEvent(String type, String resourceId, String elementPath, boolean fullRescan, Supplier<JsonObject> event) {
		this.type = type;
		this.resourceId = resourceId;
		this.elementPath = elementPath;
		this.fullRescan = fullRescan;
		this.event = new Lazy<>(event);
		this.propertyShape = new Lazy<>(() -> propertyShape(this.event.get()));
		this.semanticIds = new Lazy<>(() -> this.event.get() == null ? null : semanticIds(this.event.get().get("submodel")));
		this.canonical = new Lazy<>(() -> this.event.get() == null ? null : this.event.get().toString());
	}

	public String type() { return type; }
	public String resourceId() { return resourceId; }
	public String elementPath() { return elementPath; }
	public boolean fullRescan() { return fullRescan; }
	public Property propertyShape() { return fullRescan ? null : propertyShape.get(); }
	public Set<String> semanticIds() { return fullRescan ? null : semanticIds.get(); }
	public String canonical() { return fullRescan ? null : canonical.get(); }

	@Override public String toString() {
		return "AasControlEvent[type=" + type + ", resourceId=" + resourceId + ", elementPath=" + elementPath + ", fullRescan=" + fullRescan + "]";
	}

	/** A BaSyx Kafka event, possibly wrapped. */
	public static AasControlEvent parse(Object body) {
		try {
			JsonElement json;
			if (body instanceof byte[] bytes) json = JsonParser.parseString(new String(bytes, StandardCharsets.UTF_8));
			else if (body instanceof String text) json = JsonParser.parseString(text);
			else json = JsonParser.parseString(JsonCodec.toJson(body));
			JsonObject event = eventObject(json, 0);
			if (event == null) return rescan("unknown");
			return of(text(event, "type"), first(event, "submodelId", "aasId", "id"), text(event, "smElementPath"), () -> event);
		} catch (RuntimeException malformed) {
			return rescan("malformed");
		}
	}

	/**
	 * A BaSyx submodel-repository MQTT event. Its topic carries the event kind and, for element events, the
	 * submodel ID and element path ({@code sm-repository/<repo>/submodels/<base64url id>/submodelElements/<path>/updated});
	 * its payload is the submodel or element. A value write ({@code .../submodelElements/<path>/value/updated}, BaSyx
	 * since 2026-07) reads as an {@code SME_UPDATED} of that element, like the plain {@code /updated} older versions send.
	 * An element payload is parsed only when something asks for it.
	 */
	public static AasControlEvent parseMqtt(String topic, byte[] payload) {
		try {
			String[] levels = topic.split("/");
			if (levels.length < 4 || !"sm-repository".equals(levels[0]) || !"submodels".equals(levels[2])) return rescan("unknown");
			String action = levels[levels.length - 1];
			if (levels.length == 4) {
				// Submodel events name the submodel only in the payload.
				String type = "SM_" + action.toUpperCase(java.util.Locale.ROOT);
				JsonElement body = payload == null || payload.length == 0 ? null
						: JsonParser.parseString(new String(payload, StandardCharsets.UTF_8));
				if (body == null || !body.isJsonObject()) return rescan(type);
				String id = text(body.getAsJsonObject(), "id");
				return of(type, id, null, () -> {
					JsonObject event = new JsonObject();
					event.addProperty("type", type);
					event.addProperty("id", id);
					event.add("submodel", body);
					return event;
				});
			}
			if (levels.length < 6 || !"submodelElements".equals(levels[4])) return rescan("unknown");
			String id = new String(Base64.getUrlDecoder().decode(levels[3]), StandardCharsets.UTF_8);
			if (levels.length == 6 && "patched".equals(action)) {
				// A bulk element patch: which elements changed is only in the payload.
				return of("SME_PATCHED", id, null, () -> event("SME_PATCHED", id, null, null));
			}
			boolean attachment = levels.length >= 8 && "attachment".equals(levels[levels.length - 2]);
			boolean value = levels.length >= 8 && "value".equals(levels[levels.length - 2]) && "updated".equals(action);
			String path = String.join("/", java.util.Arrays.copyOfRange(levels, 5, levels.length - (attachment || value ? 2 : 1)));
			String type = attachment || value ? "SME_UPDATED" : "SME_" + action.toUpperCase(java.util.Locale.ROOT);
			return of(type, id, path, () -> event(type, id, path,
					attachment || payload == null || payload.length == 0 ? null : JsonParser.parseString(new String(payload, StandardCharsets.UTF_8))));
		} catch (RuntimeException malformed) {
			return rescan("malformed");
		}
	}

	private static JsonObject event(String type, String id, String path, JsonElement element) {
		JsonObject event = new JsonObject();
		event.addProperty("type", type);
		event.addProperty("id", id);
		if (path != null) event.addProperty("smElementPath", path);
		if (element != null) event.add("smElement", element);
		return event;
	}

	private static AasControlEvent of(String type, String id, String path, Supplier<JsonObject> event) {
		if (!switch (type == null ? "" : type) {
		case "SM_CREATED", "SM_UPDATED", "SM_DELETED", "SME_CREATED", "SME_UPDATED", "SME_DELETED", "SME_PATCHED",
				"AAS_CREATED", "AAS_UPDATED", "AAS_DELETED" -> true;
		default -> false;
		}) return rescan(type == null ? "unknown" : type);
		return id == null || id.isBlank() ? new AasControlEvent(type, null, path, true, () -> null)
				: new AasControlEvent(type, id, path, false, event);
	}

	private static AasControlEvent rescan(String type) {
		return new AasControlEvent(type, null, null, true, () -> null);
	}

	private static Set<String> semanticIds(JsonElement submodel) {
		if (submodel == null || !submodel.isJsonObject()) return null;
		Set<String> ids = new LinkedHashSet<>();
		JsonElement semantic = submodel.getAsJsonObject().get("semanticId");
		if (semantic != null && semantic.isJsonObject() && semantic.getAsJsonObject().get("keys") instanceof JsonArray keys)
			for (JsonElement key : keys) if (key.isJsonObject() && text(key.getAsJsonObject(), "value") != null)
				ids.add(text(key.getAsJsonObject(), "value"));
		return Set.copyOf(ids);
	}

	private static Property propertyShape(JsonObject event) {
		if (event == null || !"SME_UPDATED".equals(text(event, "type"))) return null;
		JsonElement raw = event.get("smElement");
		if (raw == null || !raw.isJsonObject() || !raw.getAsJsonObject().has("value")
				|| !"Property".equals(text(raw.getAsJsonObject(), "modelType"))) return null;
		try {
			return Aas.read(raw.toString(), SubmodelElement.class) instanceof Property property ? Aas.withoutValue(property) : null;
		} catch (RuntimeException malformed) { return null; }
	}

	private static JsonObject eventObject(JsonElement value, int depth) {
		if (depth > 4 || value == null || !value.isJsonObject()) return null;
		JsonObject object = value.getAsJsonObject();
		if (object.has("type")) return object;
		for (String wrapper : new String[] { "event", "payload", "data", "notification" }) {
			if (object.has(wrapper)) {
				JsonObject nested = eventObject(object.get(wrapper), depth + 1);
				if (nested != null) return nested;
			}
		}
		return null;
	}

	private static String first(JsonObject event, String... names) {
		for (String name : names) {
			String value = text(event, name);
			if (value != null && !value.isBlank()) return value;
		}
		return null;
	}

	private static String text(JsonObject object, String name) {
		JsonElement value = object.get(name);
		return value == null || value.isJsonNull() || !value.isJsonPrimitive() ? null : value.getAsString();
	}

	/** Computed once, on first use; a payload that fails to parse reads as absent. */
	private static final class Lazy<T> implements Supplier<T> {
		private Supplier<T> source;
		private T value;
		Lazy(Supplier<T> source) { this.source = source; }
		@Override public synchronized T get() {
			if (source != null) {
				try { value = source.get(); }
				catch (RuntimeException malformed) { value = null; }
				source = null;
			}
			return value;
		}
	}
}
