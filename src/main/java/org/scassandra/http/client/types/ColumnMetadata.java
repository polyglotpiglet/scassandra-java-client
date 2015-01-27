package org.scassandra.http.client.types;

import org.scassandra.cql.CqlType;

public class ColumnMetadata {
    private final String name;
    private final CqlType type;

    public ColumnMetadata(String name, CqlType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public CqlType getType() {
        return type;
    }
}
