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

    @SerializedName("list<varchar>")
    VarcharList,

    @SerializedName("list<ascii>")
    AsciiList,

    @SerializedName("list<text>")
    TextList;

}
