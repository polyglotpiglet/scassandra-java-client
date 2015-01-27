package org.scassandra.http.client.types;

import org.scassandra.http.client.ColumnTypes;

import java.util.HashMap;
import java.util.Map;

public class PrimitiveType extends CqlType {

    private static final Map<String, PrimitiveType> mapping = new HashMap<String, PrimitiveType>();

    public static final PrimitiveType VARCHAR = registerPrimitive("varchar");
    public static final PrimitiveType TEXT = registerPrimitive("text");
    public static final PrimitiveType ASCII = registerPrimitive("ascii");
    public static final PrimitiveType DOUBLE = registerPrimitive("double");
    public static final PrimitiveType FLOAT = registerPrimitive("float");
    public static final PrimitiveType INET = registerPrimitive("inet");
    public static final PrimitiveType INT = registerPrimitive("int");
    public static final PrimitiveType BIG_INT = registerPrimitive("bigint");
    public static final PrimitiveType TIMESTAMP = registerPrimitive("timestamp");
    public static final PrimitiveType TIMEUUID = registerPrimitive("timeuuid");
    public static final PrimitiveType UUID = registerPrimitive("uuid");
    public static final PrimitiveType VAR_INT = registerPrimitive("varint");
    public static final PrimitiveType BLOB = registerPrimitive("blob");
    public static final PrimitiveType BOOLEAN = registerPrimitive("boolean");
    public static final PrimitiveType COUNTER = registerPrimitive("counter");
    public static final PrimitiveType DECIMAL = registerPrimitive("decimal");

    private final String columnType;

    private static PrimitiveType registerPrimitive(String type) {
        PrimitiveType primitiveType = new PrimitiveType(type);
        mapping.put(type, primitiveType);
        return primitiveType;
    }

    public static PrimitiveType fromName(String name) {
        return mapping.get(name);
    }

    private PrimitiveType(String columnType) {
        this.columnType = columnType;
    }

    @Override
    public String serialise() {
        return columnType;
    }

    @Override
    public String toString() {
        return serialise();
    }
}
