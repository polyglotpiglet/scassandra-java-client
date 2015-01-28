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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.binary.Hex;
import org.scassandra.cql.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.*;
import static org.scassandra.cql.MapType.*;
import static org.scassandra.cql.ListType.*;

/**
 * This won't be an enum in version 1.0 where we'll make a braking change to represent
 * types using classes like Jackson
 */
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
            return equalsForLongType(expected, actual, this);
        }
    },
    @SerializedName("blob")
    Blob {
        @Override
        public boolean equals(Object expected, Object actual) {
            if (expected == null) {
                throw throwNullError(actual, this);
            }
            if (expected instanceof String) {
                return expected.equals(actual);
            } else if (expected instanceof ByteBuffer) {
                ByteBuffer bb = (ByteBuffer) expected;
                byte[] b = new byte[bb.remaining()];
                bb.get(b);
                String encodedExpected = Hex.encodeHexString(b);
                String actualWithout0x = actual.toString().replaceFirst("0x", "");
                return encodedExpected.equals(actualWithout0x);
            } else {
                throw throwInvalidType(expected, actual, this);
            }
        }
    },

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
    Counter {
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsForLongType(expected, actual, this);
        }

        @Override
        public CqlType getType() {
            return COUNTER;
        }
    },

    @SerializedName("decimal")
    Decimal {
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsDecimalType(expected, actual, this);

        }
    },

    @SerializedName("double")
    Double {
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsDecimalType(expected, actual, this);

        }
    },

    @SerializedName("float")
    Float {
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsDecimalType(expected, actual, this);

        }
    },

    @SerializedName("int")
    Int {
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsForLongType(expected, actual, this);
        }

    },

    @SerializedName("timestamp")
    Timestamp {
        // Gson converts JsNumbers to Doubles :(

        @Override
        public boolean equals(Object expected, Object actual) {
            Long typedActualValue = ((Double) actual).longValue();

            if (expected == null) return actual == null;
            if (actual == null) return expected == null;

            if (expected instanceof Long) {
                return expected.equals(typedActualValue);
            } else if (expected instanceof Date) {
                return ((Date) expected).getTime() == typedActualValue;
            }

            throw throwInvalidType(expected, actual, this);

        }
    },

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

            Long typedActual = ((Double) actual).longValue();

            if (expected instanceof BigInteger) {
                return expected.equals(new BigInteger(typedActual.toString()));
            } else if (expected instanceof String) {
                try {
                    return new BigInteger((String) expected).equals(new BigInteger(typedActual.toString()));
                } catch (NumberFormatException e) {
                    throw throwInvalidType(expected, actual, this);
                }
            } else {
                throw throwInvalidType(expected, actual, this);
            }
        }
    },

    @SerializedName("timeuuid")
    Timeuuid {
        // comes back from the server as a string
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsForUUID(expected, actual, this);
        }

    },

    @SerializedName("uuid")
    Uuid {
        // comes back from the server as a string
        @Override
        public boolean equals(Object expected, Object actual) {
            return equalsForUUID(expected, actual, this);
        }

    },

    @SerializedName("inet")
    Inet {
        // comes from the server as a string
        @Override
        public boolean equals(Object expected, Object actual) {
            if (expected == null) return actual == null;
            if (actual == null) return expected == null;

            if (expected instanceof String) {
                try {
                    return expected.equals(actual);
                } catch (Exception e) {
                    throw throwInvalidType(expected, actual, this);
                }
            } else if (expected instanceof InetAddress) {
                return ((InetAddress) expected).getHostAddress().equals(actual);
            }

            throw throwInvalidType(expected, actual, this);
        }
    },

    @SerializedName("text")
    Text {
        @Override
        public boolean equals(Object expected, Object actual) {
            return Ascii.equals(expected, actual);
        }
    },

    @SerializedName("set<varchar>")
    VarcharSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Varchar);
        }

        @Override
        public CqlType getType() {
            return set(VARCHAR);
        }
    },


    @SerializedName("set<ascii>")
    AsciiSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Ascii);
        }

        @Override
        public CqlType getType() {
            return set(ASCII);
        }
    },

    @SerializedName("set<text>")
    TextSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Text);
        }

        @Override
        public CqlType getType() {
            return set(TEXT);
        }
    },

    @SerializedName("set<bigint>")
    BigintSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Bigint);
        }

        @Override
        public CqlType getType() {
            return set(BIG_INT);
        }
    },

    @SerializedName("set<blob>")
    BlobSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Blob);
        }

        @Override
        public CqlType getType() {
            return set(BLOB);
        }
    },

    @SerializedName("set<boolean>")
    BooleanSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Boolean);
        }

        @Override
        public CqlType getType() {
            return set(BOOLEAN);
        }
    },

    @SerializedName("set<decimal>")
    DecimalSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Decimal);
        }

        @Override
        public CqlType getType() {
            return set(DECIMAL);
        }
    },

    @SerializedName("set<double>")
    DoubleSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Double);
        }

        @Override
        public CqlType getType() {
            return set(DOUBLE);
        }
    },

    @SerializedName("set<float>")
    FloatSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Float);
        }

        @Override
        public CqlType getType() {
            return set(FLOAT);
        }
    },

    @SerializedName("set<inet>")
    InetSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Inet);
        }

        @Override
        public CqlType getType() {
            return set(INET);
        }
    },

    @SerializedName("set<int>")
    IntSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Int);
        }

        @Override
        public CqlType getType() {
            return set(INT);
        }
    },

    @SerializedName("set<timestamp>")
    TimestampSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Timestamp);
        }

        @Override
        public CqlType getType() {
            return set(TIMESTAMP);
        }
    },

    @SerializedName("set<timeuuid>")
    TimeuuidSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Timeuuid);
        }

        @Override
        public CqlType getType() {
            return set(TIMEUUID);
        }
    },

    @SerializedName("set<uuid>")
    UuidSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Uuid);
        }

        @Override
        public CqlType getType() {
            return set(UUID);
        }
    },

    @SerializedName("set<varint>")
    VarintSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Varint);
        }

        @Override
        public CqlType getType() {
            return set(VAR_INT);
        }
    },

    @SerializedName("list<varchar>")
    VarcharList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Varchar);
        }

        @Override
        public CqlType getType() {
            return list(PrimitiveType.VARCHAR);
        }
    },

    @SerializedName("list<ascii>")
    AsciiList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Ascii);
        }

        @Override
        public CqlType getType() {
            return list(ASCII);
        }
    },

    @SerializedName("list<text>")
    TextList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Text);
        }
        @Override
        public CqlType getType() {
            return list(TEXT);
        }
    },

    @SerializedName("list<bigint>")
    BigintList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Bigint);
        }
        @Override
        public CqlType getType() {
            return list(BIG_INT);
        }
    },

    @SerializedName("list<blob>")
    BlobList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Blob);
        }
        @Override
        public CqlType getType() {
            return list(BLOB);
        }
    },

    @SerializedName("list<boolean>")
    BooleanList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Boolean);
        }
        @Override
        public CqlType getType() {
            return list(BOOLEAN);
        }
    },

    @SerializedName("list<decimal>")
    DecimalList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Decimal);
        }
        @Override
        public CqlType getType() {
            return list(DECIMAL);
        }
    },

    @SerializedName("list<double>")
    DoubleList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Double);
        }
        @Override
        public CqlType getType() {
            return list(DOUBLE);
        }
    },

    @SerializedName("list<float>")
    FloatList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Float);
        }
        @Override
        public CqlType getType() {
            return list(FLOAT);
        }
    },

    @SerializedName("list<inet>")
    InetList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Inet);
        }
        @Override
        public CqlType getType() {
            return list(INET);
        }
    },

    @SerializedName("list<int>")
    IntList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Int);
        }
        @Override
        public CqlType getType() {
            return list(INT);
        }
    },

    @SerializedName("list<timestamp>")
    TimestampList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Timestamp);
        }
        @Override
        public CqlType getType() {
            return list(TIMESTAMP);
        }
    },

    @SerializedName("list<timeuuid>")
    TimeuuidList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Timeuuid);
        }
        @Override
        public CqlType getType() {
            return list(TIMEUUID);
        }
    },

    @SerializedName("list<uuid>")
    UuidList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Uuid);
        }
        @Override
        public CqlType getType() {
            return list(UUID);
        }
    },

    @SerializedName("list<varint>")
    VarintList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Varint);
        }
        @Override
        public CqlType getType() {
            return list(VAR_INT);
        }
    },

    @SerializedName("map<varchar,varchar>")
    VarcharVarcharMap {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Varchar, Varchar);
        }

        @Override
        public CqlType getType() {
            return map(PrimitiveType.VARCHAR, PrimitiveType.VARCHAR);
        }
    },

    @SerializedName("map<varchar,text>")
    VarcharTextMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Varchar, Text);
        }

        @Override
        public CqlType getType() {
            return map(PrimitiveType.VARCHAR, PrimitiveType.TEXT);
        }
    },

    @SerializedName("map<varchar,ascii>")
    VarcharAsciiMap {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Varchar, Ascii);
        }

        @Override
        public CqlType getType() {
            return map(PrimitiveType.VARCHAR, PrimitiveType.ASCII);
        }
    },

    @SerializedName("map<text,varchar>")
    TextVarcharMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Text, Varchar);
        }
        @Override
        public CqlType getType() {
            return map(PrimitiveType.TEXT, PrimitiveType.VARCHAR);
        }
    },

    @SerializedName("map<text,text>")
    TextTextMap {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Text, Text);
        }
        @Override
        public CqlType getType() {
            return map(PrimitiveType.TEXT, PrimitiveType.TEXT);
        }
    },

    @SerializedName("map<text,ascii>")
    TextAsciiMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Text, Ascii);
        }
        @Override
        public CqlType getType() {
            return map(PrimitiveType.TEXT, PrimitiveType.ASCII);
        }
    },

    @SerializedName("map<ascii,varchar>")
    AsciiVarcharMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Ascii, Varchar);
        }
        @Override
        public CqlType getType() {
            return map(PrimitiveType.ASCII, PrimitiveType.VARCHAR);
        }
    },

    @SerializedName("map<ascii,text>")
    AsciiTextMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Ascii, Text);
        }
        @Override
        public CqlType getType() {
            return map(PrimitiveType.ASCII, PrimitiveType.TEXT);
        }
    },

    @SerializedName("map<ascii,ascii>")
    AsciiAsciiMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Ascii, Ascii);
        }
        @Override
        public CqlType getType() {
            return map(PrimitiveType.ASCII, PrimitiveType.ASCII);
        }
    }
    ;


    abstract public boolean equals(Object expected, Object actual);

    public CqlType getType() {
        return PrimitiveType.fromName(this.name().toLowerCase());
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

    private static boolean compareStringInteger(Object expected, Long actual, ColumnTypes instance) {
        try {
            return new BigInteger((String) expected).equals(new BigInteger(actual.toString()));
        } catch (NumberFormatException e) {
            throw throwInvalidType(expected, actual, instance);
        }
    }


    private static boolean equalsForLongType(Object expected, Object actual, ColumnTypes columnTypes) {

        if (expected == null) return actual == null;
        if (actual == null) return false;

        Long typedActual = ((Double) actual).longValue();

        if (expected instanceof Integer) {
            return ((Integer) expected).longValue() == typedActual;
        } else if (expected instanceof Long) {
            return expected == typedActual;
        } else if (expected instanceof BigInteger) {
            return expected.equals(new BigInteger(typedActual.toString()));
        } else if (expected instanceof String) {
            return compareStringInteger(expected, typedActual, columnTypes);
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

    private static boolean equalsDecimalType(Object expected, Object actual, ColumnTypes columnTypes) {
        if (expected == null) {
            throw throwNullError(actual, columnTypes);
        } else if (actual == null) {
            return false;
        }

        if (expected instanceof String) {
            try {
                return (new BigDecimal(expected.toString()).compareTo(new BigDecimal(actual.toString())) == 0);
            } catch (NumberFormatException e) {
                throw throwInvalidType(expected, actual, columnTypes);
            }
        } else if (expected instanceof BigDecimal) {
            return ((BigDecimal) expected).compareTo(new BigDecimal(actual.toString())) == 0;
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

    private static boolean equalsForUUID(Object expected, Object actual, ColumnTypes columnTypes) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof String) {
            try {
                return java.util.UUID.fromString(expected.toString()).equals(java.util.UUID.fromString(actual.toString()));
            } catch (Exception e) {
                throw throwInvalidType(expected, actual, columnTypes);
            }
        } else if (expected instanceof UUID) {
            return expected.toString().equals(actual);
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

    private static boolean compareSet(Object expected, Object actual, ColumnTypes columnTypes, final ColumnTypes setType) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof Set) {
            final Set<?> typedExpected = (Set<?>) expected;
            final List<?> actualList = (List<?>) actual;

            return typedExpected.size() == actualList.size() && 
                    Iterables.all(typedExpected, new Predicate<Object>() {
                        @Override
                        public boolean apply(final Object eachExpected) {
                            return Iterables.any(actualList, new Predicate<Object>() {
                        @Override
                        public boolean apply(Object eachActual) {
                            return setType.equals(eachExpected, eachActual);
                        }
                    });
                }
            });

        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }

    private static boolean compareList(Object expected, Object actual, ColumnTypes columnTypes, final ColumnTypes listType) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof List) {
            final List<?> typedExpected = (List<?>) expected;
            final List<?> actualList = (List<?>) actual;

            if (typedExpected.size() != actualList.size()) return false;

            for (int i = 0; i < actualList.size(); i++) {
                if (!listType.equals(typedExpected.get(i), actualList.get(i))) return false;
            }
            return true;
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }
    
    private static boolean compareMap(Object expected, Object actual, ColumnTypes columnTypes, final ColumnTypes keyType, final ColumnTypes valueType) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        if (expected instanceof Map) {
            final Map<?,?> typedExpected = (Map<?, ?>) expected;
            final Map<?,?> actualMap = (Map<?, ?>) actual;

            if (typedExpected.size() != actualMap.size()) return false;

            for (final Map.Entry<?, ?> eachExpected : typedExpected.entrySet()) {
                boolean match = Iterables.any(actualMap.keySet(), new Predicate<Object>() {
                    @Override
                    public boolean apply(Object eachActualKey) {
                        Object eachActual = actualMap.get(eachActualKey);
                        return keyType.equals(eachExpected.getKey(), eachActualKey) && valueType.equals(eachExpected.getValue(), eachActual);
                    }
                });
                
                if (!match) return false;
            }
            return true;
        } else {
            throw throwInvalidType(expected, actual, columnTypes);
        }
    }
}
