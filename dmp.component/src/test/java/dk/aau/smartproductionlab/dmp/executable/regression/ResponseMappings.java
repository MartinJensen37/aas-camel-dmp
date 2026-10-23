package dk.aau.smartproductionlab.dmp.executable.regression;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Test models (AAS JSON): the response mapping an Operation with outputs needs. The
 * caller's outputs are only what the ResponseTransformation makes of the ResponseSources;
 * nothing is matched by name, so a fixture Operation that answers declares its outputs and
 * a ResponseSink selecting it.
 */
public final class ResponseMappings {

	private ResponseMappings() {
	}

	/**
	 * Appends ResponseSources ({@code sourceId}: the output of the action the mapping's first
	 * Sink addresses), the response {@code lua} and a ResponseSink ({@code callerSinkId})
	 * selecting the mapping's Operation source. Call it after the Sinks are final.
	 */
	public static void respond(JsonObject mapping, String sourceId, String callerSinkId, String lua) {
		JsonArray action = keys(child(mapping, "Sinks").getAsJsonArray("value").get(0).getAsJsonObject(), "Sink");
		// The sink names the action's input (or a member of it); the reply is the action's output.
		int affordance = 0;
		while (affordance < action.size() && !"actions".equals(action.get(affordance).getAsJsonObject().get("value").getAsString())) affordance++;
		while (action.size() > affordance + 2) action.remove(action.size() - 1);
		action.add(key("SubmodelElementCollection", "output"));
		JsonArray operation = keys(child(mapping, "Sources").getAsJsonArray("value").get(0).getAsJsonObject(), "Source");
		none(mapping);
		JsonArray fields = mapping.getAsJsonArray("value");
		fields.add(list("ResponseSources", endpoint("Source", sourceId, action)));
		fields.add(JsonParser.parseString("{\"modelType\":\"Blob\",\"idShort\":\"ResponseTransformation\","
				+ "\"contentType\":\"text/plain\",\"value\":\""
				+ Base64.getEncoder().encodeToString(lua.getBytes(StandardCharsets.UTF_8)) + "\"}"));
		fields.add(list("ResponseSinks", endpoint("Sink", callerSinkId, operation)));
	}

	/** Removes the response mapping: the Operation then answers with no outputs. */
	public static void none(JsonObject mapping) {
		JsonArray fields = mapping.getAsJsonArray("value");
		for (int i = fields.size() - 1; i >= 0; i--)
			if (fields.get(i).getAsJsonObject().get("idShort").getAsString().startsWith("Response")) fields.remove(i);
	}

	/** Declares {@code idShorts} as xs:string output variables of a CCI Operation element. */
	public static void outputs(JsonObject operation, String... idShorts) {
		JsonArray variables = new JsonArray();
		for (String idShort : idShorts) variables.add(JsonParser.parseString("{\"value\":{\"modelType\":\"Property\","
				+ "\"idShort\":\"" + idShort + "\",\"valueType\":\"xs:string\"}}"));
		operation.add("outputVariables", variables);
	}

	/** The {@code skill}'s Operation in a CCI submodel. */
	public static JsonObject operation(JsonObject cci, String skill) {
		return child(child(child(cci, "Skills"), skill), "operation");
	}

	private static JsonArray keys(JsonObject endpoint, String role) {
		return child(endpoint, role).getAsJsonObject("value").getAsJsonArray("keys").deepCopy();
	}

	private static JsonObject key(String type, String value) {
		JsonObject key = new JsonObject();
		key.addProperty("type", type);
		key.addProperty("value", value);
		return key;
	}

	private static JsonObject endpoint(String role, String id, JsonArray keys) {
		JsonObject reference = new JsonObject();
		reference.addProperty("type", "ModelReference");
		reference.add("keys", keys);
		JsonObject element = JsonParser.parseString("{\"modelType\":\"ReferenceElement\",\"idShort\":\"" + role + "\"}")
				.getAsJsonObject();
		element.add("value", reference);
		JsonObject endpoint = JsonParser.parseString("{\"modelType\":\"SubmodelElementCollection\",\"value\":[]}")
				.getAsJsonObject();
		endpoint.getAsJsonArray("value").add(element);
		endpoint.getAsJsonArray("value").add(JsonParser.parseString("{\"modelType\":\"Property\",\"idShort\":\"" + role
				+ "Id\",\"valueType\":\"xs:string\",\"value\":\"" + id + "\"}"));
		return endpoint;
	}

	private static JsonObject list(String idShort, JsonObject member) {
		JsonObject list = JsonParser.parseString("{\"modelType\":\"SubmodelElementList\",\"idShort\":\"" + idShort
				+ "\",\"typeValueListElement\":\"SubmodelElementCollection\",\"value\":[]}").getAsJsonObject();
		list.getAsJsonArray("value").add(member);
		return list;
	}

	private static JsonObject child(JsonObject parent, String name) {
		for (var entry : parent.getAsJsonArray(parent.has("submodelElements") ? "submodelElements" : "value")) {
			JsonObject child = entry.getAsJsonObject();
			if (child.has("idShort") && name.equals(child.get("idShort").getAsString())) return child;
		}
		throw new IllegalStateException("missing fixture element " + name);
	}
}
