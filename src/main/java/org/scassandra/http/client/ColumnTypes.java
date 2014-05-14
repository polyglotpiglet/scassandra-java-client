package org.scassandra.http.client;

import com.google.gson.annotations.SerializedName;

public enum ColumnTypes {

    @SerializedName("ascii")
    Ascii,

    @SerializedName("bigint")
    Bigint,

    @SerializedName("blob")
    Blob,

    @SerializedName("boolean")
    Boolean,

    @SerializedName("counter")
    Counter,

    @SerializedName("decimal")
    Decimal,

    @SerializedName("double")
    Double,

    @SerializedName("float")
    Float,

    @SerializedName("int")
    Int,

    @SerializedName("timestamp")
    Timestamp,

    @SerializedName("uuid")
    Uuid,

    @SerializedName("varchar")
    Varchar,

    @SerializedName("varint")
    Varint,

    @SerializedName("timeuuid")
    Timeuuid,

    @SerializedName("inet")
    Inet,

    @SerializedName("text")
    Text,

    @SerializedName("set")
    Set,
}
