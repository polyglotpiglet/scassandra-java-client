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

import java.math.BigInteger;

public enum ColumnTypes {

    @SerializedName("ascii")
    Ascii {
        @Override
        public boolean equals(Object expected, Object actual) {
            if (expected == null) return actual == null;
            return expected.equals(actual);
        }
    },

    @SerializedName("bigint")
    Bigint {
        @Override
        public boolean equals(Object expected, Object actual) {

            if (expected == null) return actual == null;
            if (actual == null) return expected == null;

            if (expected instanceof Integer) {
                return expected.equals(Integer.parseInt(actual.toString()));
            } else if (expected instanceof Long) {
                return expected.equals(Long.parseLong(actual.toString()));
            } else if (expected instanceof BigInteger) {
                return expected.equals(new BigInteger(actual.toString()));
            } else if (expected instanceof String) {
                return compareStringInteger(expected, actual, this);
            } else {
                throw throwInvalidType(expected, actual, this);
            }
        }

    },

    @SerializedName("blob")
    Blob,

    @SerializedName("boolean")
    Boolean {
        @Override
        public boolean equals(Object expected, Object actual) {
            if (expected == null) throw throwNullError(actual, this);

            if (expected instanceof Boolean) {
                return expected.equals(actual);
            } else {
                throw throwInvalidType(expected, actual, this);
            }
        }
    },

    @SerializedName("counter")
    Counter,

    @SerializedName("decimal")
    Decimal,

    @SerializedName("double")
    Double,

    @SerializedName("float")
    Float,

    @SerializedName("int")
    Int {
        @Override
        public boolean equals(Object expected, Object actual) {

            if (expected == null) return actual == null;
            if (actual == null) return expected == null;

            if (expected instanceof Integer) {
                return expected.equals(Integer.parseInt(actual.toString()));
            } else if (expected instanceof String) {
                try {
                    return Integer.valueOf((String) expected).equals(Integer.parseInt(actual.toString()));
                } catch (NumberFormatException e) {
                    throw throwInvalidType(expected, actual, this);
                }
            } else {
                throw throwInvalidType(expected, actual, this);
            }
        }
    },

    @SerializedName("timestamp")
    Timestamp,

    @SerializedName("uuid")
    Uuid,

    @SerializedName("varchar")
    Varchar {
        @Override
        public boolean equals(Object expected, Object actual) {
            return Ascii.equals(expected, actual);
        }
    },

    @SerializedName("varint")
    Varint {
        @Override
        public boolean equals(Object expected, Object actual) {
            if (expected == null) return actual == null;
            if (actual == null) return expected == null;


            if (expected instanceof BigInteger) {
                return expected.equals(new BigInteger(actual.toString()));
            } else if (expected instanceof String) {
                return compareStringInteger(expected, actual, this);
            } else {
                throw throwInvalidType(expected, actual, this);
            }
        }
    },

    @SerializedName("timeuuid")
    Timeuuid,

    @SerializedName("inet")
    Inet,

    @SerializedName("text")
    Text {
        @Override
        public boolean equals(Object expected, Object actual) {
            return Ascii.equals(expected, actual);
        }
    },

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
    AsciiAsciiMap;

    public boolean equals(Object expected, Object actual) {
        return false;
    }

    private static IllegalArgumentException throwInvalidType(Object expected, Object actual, ColumnTypes instance) {
        return new IllegalArgumentException(String.format("Invalid expected value (%s,%s) for variable of types %s, the value was %s for valid types see: %s",
                expected,
                expected.getClass().getSimpleName(),
                instance.name(),
                actual,
                "http://www.scassandra.org/java-client/column-types/"
        ));
    }
    private static IllegalArgumentException throwNullError(Object actual, ColumnTypes instance) {
        return new IllegalArgumentException(String.format("Invalid expected value (null) for variable of types %s, the value was %s for valid types see: %s",
                instance.name(),
                actual,
                "http://www.scassandra.org/java-client/column-types/"
        ));
    }

    private static boolean compareStringInteger(Object expected, Object actual, ColumnTypes instance) {
        try {
            return new BigInteger((String) expected).equals(new BigInteger(actual.toString()));
        } catch (NumberFormatException e) {
            throw throwInvalidType(expected, actual, instance);
        }
    }
}
