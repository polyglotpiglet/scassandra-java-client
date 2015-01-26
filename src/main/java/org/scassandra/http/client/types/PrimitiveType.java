package org.scassandra.http.client.types;

import org.scassandra.http.client.ColumnTypes;

public class PrimitiveType extends CqlType {

    public static final PrimitiveType VARCHAR = new PrimitiveType(ColumnTypes.Varchar);
    public static final PrimitiveType ASCII = new PrimitiveType(ColumnTypes.Ascii);

    private ColumnTypes columnType;

    private PrimitiveType(ColumnTypes columnType) {
        this.columnType = columnType;
    }

    @Override
    public String serialise() {
        return columnType.toString().toLowerCase();
    }
}
