package org.scassandra.http.client.types;

import org.scassandra.http.client.ColumnTypes;

import java.util.HashMap;
import java.util.Map;

public class PrimitiveType extends CqlType {

    private static final Map<String, PrimitiveType> mapping = new HashMap<String, PrimitiveType>();

    public static final PrimitiveType VARCHAR = registerPrimitive(ColumnTypes.Varchar);
    public static final PrimitiveType TEXT = registerPrimitive(ColumnTypes.Text);
    public static final PrimitiveType ASCII = registerPrimitive(ColumnTypes.Ascii);
    public static final PrimitiveType DOUBLE = registerPrimitive(ColumnTypes.Double);
    public static final PrimitiveType FLOAT = registerPrimitive(ColumnTypes.Float);
    public static final PrimitiveType INET = registerPrimitive(ColumnTypes.Inet);
    public static final PrimitiveType INT = registerPrimitive(ColumnTypes.Int);
    public static final PrimitiveType BIG_INT = registerPrimitive(ColumnTypes.Bigint);
    public static final PrimitiveType TIMESTAMP = registerPrimitive(ColumnTypes.Timestamp);
    public static final PrimitiveType TIMEUUID = registerPrimitive(ColumnTypes.Timeuuid);
    public static final PrimitiveType UUID = registerPrimitive(ColumnTypes.Uuid);
    public static final PrimitiveType VAR_INT = registerPrimitive(ColumnTypes.Varint);
    public static final PrimitiveType BLOB = registerPrimitive(ColumnTypes.Blob);
    public static final PrimitiveType BOOLEAN = registerPrimitive(ColumnTypes.Boolean);
    public static final PrimitiveType COUNTER = registerPrimitive(ColumnTypes.Counter);
    public static final PrimitiveType DECIMAL = registerPrimitive(ColumnTypes.Decimal);



    private static PrimitiveType registerPrimitive(ColumnTypes type) {
        PrimitiveType primitiveType = new PrimitiveType(type);
        mapping.put(type.toString().toLowerCase(), primitiveType);
        return primitiveType;
    }

    public static PrimitiveType fromName(String name) {
        return mapping.get(name);
    }


    private ColumnTypes columnType;

    private PrimitiveType(ColumnTypes columnType) {
        this.columnType = columnType;
    }

    @Override
    public String serialise() {
        return columnType.toString().toLowerCase();
    }

    @Override
    public String toString() {
        return serialise();
    }
}
