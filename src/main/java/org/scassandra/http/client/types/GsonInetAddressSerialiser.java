package org.scassandra.http.client.types;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.scassandra.cql.CqlType;

import java.lang.reflect.Type;
import java.net.InetAddress;

public class GsonInetAddressSerialiser implements JsonSerializer<InetAddress> {
    @Override
    public JsonElement serialize(InetAddress src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(src.getHostAddress());
    }
}
