package org.scassandra.http.client.types;

import com.google.gson.*;
import org.scassandra.cql.CqlType;
import org.scassandra.cql.CqlTypeFactory;

import java.lang.reflect.Type;

public class GsonCqlTypeDeserialiser implements JsonDeserializer<CqlType> {

    private CqlTypeFactory cqlTypeFactory = new CqlTypeFactory();

    @Override
    public CqlType deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        return cqlTypeFactory.buildType(json.getAsString());
    }
}
