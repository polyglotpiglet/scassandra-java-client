package org.scassandra.http.client.types;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;

public class GsonCqlTypeSerialiser implements JsonSerializer<CqlType> {
    @Override
    public JsonElement serialize(CqlType src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.serialise());
    }
}
