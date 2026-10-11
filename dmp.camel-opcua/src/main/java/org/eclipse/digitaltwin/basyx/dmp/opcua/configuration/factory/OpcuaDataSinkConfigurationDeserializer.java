/*******************************************************************************
 * Copyright (C) 2024 the Eclipse BaSyx Authors
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
package org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.factory;

import com.google.gson.*;

import org.eclipse.digitaltwin.basyx.dmp.opcua.configuration.OpcuaDataSinkConfiguration;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of OpcUa Producer configuration deserializer
 *
 * @author zielstor
 *
 */
public class OpcuaDataSinkConfigurationDeserializer implements JsonDeserializer<OpcuaDataSinkConfiguration> {
    @Override
    public OpcuaDataSinkConfiguration deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        JsonObject object = jsonElement.getAsJsonObject();
        Map<String, String> ctx = new HashMap<>();

        if (object.has("requestedPublishingInterval"))
            ctx.put("requestedPublishingInterval", object.get("requestedPublishingInterval").getAsString());

        if (object.has("parameters")) {
            object.get("parameters").getAsJsonObject()
                    .entrySet()
                    .parallelStream()
                    .filter(o -> !o.getValue().isJsonNull() && !ctx.containsKey(o.getValue().getAsString()))
                    .forEach(o -> ctx.put(o.getKey(), o.getValue().getAsString()));
        }

        // the descriptor emits method-call sinks under `configuration` —
        // merge it (existing parameters win)
        if (object.has("configuration") && object.get("configuration").isJsonObject()) {
            object.get("configuration").getAsJsonObject()
                    .entrySet()
                    .forEach(o -> ctx.putIfAbsent(o.getKey(), o.getValue().getAsString()));
        }

        OpcuaDataSinkConfiguration sink = new OpcuaDataSinkConfiguration(
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
        if (object.has("sinkId") && !object.get("sinkId").isJsonNull()) {
            sink.setSinkId(object.get("sinkId").getAsString());
        }
        if (object.has("valuePath") && !object.get("valuePath").isJsonNull()) {
            sink.setValuePath(object.get("valuePath").getAsString());
        }
        if (object.has("schema") && !object.get("schema").isJsonNull()) {
            sink.setSchema(jsonDeserializationContext.deserialize(object.get("schema"), Map.class));
        }
        if (object.has("payloadSchema") && !object.get("payloadSchema").isJsonNull()) {
            sink.setPayloadSchema(jsonDeserializationContext.deserialize(object.get("payloadSchema"), Map.class));
        }
        if (object.has("payloadGroup") && !object.get("payloadGroup").isJsonNull()) {
            sink.setPayloadGroup(object.get("payloadGroup").getAsString());
        }
        return sink;
    }
}
