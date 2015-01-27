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

    @Override
    public String toString() {
        return this.serialise();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapType mapType1 = (MapType) o;

        if (keyType != null ? !keyType.equals(mapType1.keyType) : mapType1.keyType != null) return false;
        if (mapType != null ? !mapType.equals(mapType1.mapType) : mapType1.mapType != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = keyType != null ? keyType.hashCode() : 0;
        result = 31 * result + (mapType != null ? mapType.hashCode() : 0);
        return result;
    }
}
