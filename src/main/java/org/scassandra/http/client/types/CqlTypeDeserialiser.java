package org.scassandra.http.client.types;

import com.google.gson.*;

import java.lang.reflect.Type;

public class CqlTypeDeserialiser implements JsonDeserializer<CqlType> {
    @Override
    public CqlType deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        return null;
    }
}
