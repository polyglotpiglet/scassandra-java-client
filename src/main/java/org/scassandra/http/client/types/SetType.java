package org.scassandra.http.client.types;

public class SetType extends CqlType {
    private final CqlType type;

    public SetType(CqlType type) {
        this.type = type;
    }

    @Override
    public String serialise() {
        return String.format("set<%s>", type.serialise());
    }
}
