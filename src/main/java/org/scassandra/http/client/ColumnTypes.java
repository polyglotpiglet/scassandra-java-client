/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

    @SerializedName("set<varchar>")
    VarcharSet,

    @SerializedName("set<ascii>")
    AsciiSet,

    @SerializedName("set<text>")
    TextSet,

    @SerializedName("set<bigint>")
    BigintSet,

    @SerializedName("set<blob>")
    BlobSet,

    @SerializedName("set<boolean>")
    BooleanSet,

    @SerializedName("set<decimal>")
    DecimalSet,

    @SerializedName("set<double>")
    DoubleSet,

    @SerializedName("set<float>")
    FloatSet,

    @SerializedName("set<inet>")
    InetSet,

    @SerializedName("set<int>")
    IntSet,

    @SerializedName("set<timestamp>")
    TimestampSet,

    @SerializedName("set<timeuuid>")
    TimeuuidSet,

    @SerializedName("set<uuid>")
    UuidSet,

    @SerializedName("set<varint>")
    VarintSet,

    @SerializedName("list<varchar>")
    VarcharList,

    @SerializedName("list<ascii>")
    AsciiList,

    @SerializedName("list<text>")
    TextList,

    @SerializedName("list<bigint>")
    BigintList,

    @SerializedName("list<blob>")
    BlobList,

    @SerializedName("list<boolean>")
    BooleanList,

    @SerializedName("list<decimal>")
    DecimalList,

    @SerializedName("list<double>")
    DoubleList,

    @SerializedName("list<float>")
    FloatList,

    @SerializedName("list<inet>")
    InetList,

    @SerializedName("list<int>")
    IntList,

    @SerializedName("list<timestamp>")
    TimestampList,

    @SerializedName("list<timeuuid>")
    TimeuuidList,

    @SerializedName("list<uuid>")
    UuidList,

    @SerializedName("list<varint>")
    VarintList,

    @SerializedName("map<varchar,varchar>")
    VarcharVarcharMap,

    @SerializedName("map<varchar,text>")
    VarcharTextMap,

    @SerializedName("map<varchar,ascii>")
    VarcharAsciiMap,

    @SerializedName("map<text,varchar>")
    TextVarcharMap,

    @SerializedName("map<text,text>")
    TextTextMap,

    @SerializedName("map<text,ascii>")
    TextAsciiMap,

    @SerializedName("map<ascii,varchar>")
    AsciiVarcharMap,

    @SerializedName("map<ascii,text>")
    AsciiTextMap,

    @SerializedName("map<ascii,ascii>")
    AsciiAsciiMap
    ;

}
