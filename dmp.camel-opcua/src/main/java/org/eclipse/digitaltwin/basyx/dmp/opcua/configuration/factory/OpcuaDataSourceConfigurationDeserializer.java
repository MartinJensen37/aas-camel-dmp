package org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.factory;

import com.google.gson.*;
import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSourceConfiguration;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class OpcuaDataSourceConfigurationDeserializer implements JsonDeserializer<OpcuaDataSourceConfiguration> {
  
    @Override
    public OpcuaDataSourceConfiguration deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject object = jsonElement.getAsJsonObject();
        Map<String, String> ctx = new HashMap<>();

        if (object.has("requestedPublishingInterval"))
            ctx.put("requestedPublishingInterval", object.get("requestedPublishingInterval").getAsString());

        if (object.has("parameters")) {
            for (Map.Entry<String, JsonElement> parameter : object.getAsJsonObject("parameters").entrySet()) {
                if (!parameter.getValue().isJsonNull()) {
                    ctx.putIfAbsent(parameter.getKey(), parameter.getValue().getAsString());
                }
            }
        }

        // the execution-policy fields ride the BASE configuration (sourceId,
        // delivery, declared polling cadence) — without them the route cache
        // keys by the raw endpoint id and the Lua sources lookups miss
        OpcuaDataSourceConfiguration config = new OpcuaDataSourceConfiguration(
                object.get("uniqueId").getAsString(),
                object.get("serverUrl").getAsString(),
                object.get("serverPort").getAsInt(),
                object.get("pathToService").getAsString(),
                object.get("nodeInformation").getAsString(),
                (!object.has("username") || object.get("username").isJsonNull()) ? null :
                        object.get("username").getAsString(),
                (!object.has("password") || object.get("password").isJsonNull()) ? null :
                        object.get("password").getAsString(),
                ctx
        );
        if (object.has("sourceId") && !object.get("sourceId").isJsonNull()) {
            config.setSourceId(object.get("sourceId").getAsString());
        }
        if (object.has("delivery") && !object.get("delivery").isJsonNull()) {
            config.setDelivery(object.get("delivery").getAsString());
        }
        if (object.has("pollingIntervalMs") && !object.get("pollingIntervalMs").isJsonNull()) {
            config.setPollingIntervalMs(object.get("pollingIntervalMs").getAsLong());
        }
        if (object.has("valuePath") && !object.get("valuePath").isJsonNull()) {
            config.setValuePath(object.get("valuePath").getAsString());
        }
        if (object.has("sourceKind") && !object.get("sourceKind").isJsonNull()) {
            config.setSourceKind(object.get("sourceKind").getAsString());
        }
        if (object.has("mode") && !object.get("mode").isJsonNull()) {
            config.setMode(object.get("mode").getAsString());
        }
        if (object.has("eventFilter") && !object.get("eventFilter").isJsonNull()) {
            config.setEventFilter(jsonDeserializationContext.deserialize(object.get("eventFilter"), Map.class));
        }
        return config;
    }
}
