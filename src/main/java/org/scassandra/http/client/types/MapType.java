package org.scassandra.http.client.types;

public class MapType extends CqlType {
    private CqlType keyType;
    private CqlType mapType;

    public MapType(CqlType keyType, CqlType mapType) {
        this.keyType = keyType;
        this.mapType = mapType;
    }

    @Override
    public String serialise() {
        return String.format("map<%s,%s>", keyType.serialise(), mapType.serialise());
    }
}
