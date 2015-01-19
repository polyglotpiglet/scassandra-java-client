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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

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
    },


    @SerializedName("set<ascii>")
    AsciiSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Ascii);
        }
    },

    @SerializedName("set<text>")
    TextSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Text);
        }
    },

    @SerializedName("set<bigint>")
    BigintSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Bigint);
        }
    },

    @SerializedName("set<blob>")
    BlobSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Blob);
        }
    },

    @SerializedName("set<boolean>")
    BooleanSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Boolean);
        }
    },

    @SerializedName("set<decimal>")
    DecimalSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Decimal);
        }
    },

    @SerializedName("set<double>")
    DoubleSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Double);
        }
    },

    @SerializedName("set<float>")
    FloatSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Float);
        }
    },

    @SerializedName("set<inet>")
    InetSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Inet);
        }
    },

    @SerializedName("set<int>")
    IntSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Int);
        }
    },

    @SerializedName("set<timestamp>")
    TimestampSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Timestamp);
        }
    },

    @SerializedName("set<timeuuid>")
    TimeuuidSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Timeuuid);
        }
    },

    @SerializedName("set<uuid>")
    UuidSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Uuid);
        }
    },

    @SerializedName("set<varint>")
    VarintSet {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareSet(expected, actual, this, Varint);
        }
    },

    @SerializedName("list<varchar>")
    VarcharList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Varchar);
        }
    },

    @SerializedName("list<ascii>")
    AsciiList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Ascii);
        }
    },

    @SerializedName("list<text>")
    TextList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Text);
        }
    },

    @SerializedName("list<bigint>")
    BigintList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Bigint);
        }
    },

    @SerializedName("list<blob>")
    BlobList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Blob);
        }
    },

    @SerializedName("list<boolean>")
    BooleanList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Boolean);
        }
    },

    @SerializedName("list<decimal>")
    DecimalList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Decimal);
        }
    },

    @SerializedName("list<double>")
    DoubleList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Double);
        }
    },

    @SerializedName("list<float>")
    FloatList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Float);
        }
    },

    @SerializedName("list<inet>")
    InetList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Inet);
        }
    },

    @SerializedName("list<int>")
    IntList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Int);
        }
    },

    @SerializedName("list<timestamp>")
    TimestampList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Timestamp);
        }
    },

    @SerializedName("list<timeuuid>")
    TimeuuidList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Timeuuid);
        }
    },

    @SerializedName("list<uuid>")
    UuidList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Uuid);
        }
    },

    @SerializedName("list<varint>")
    VarintList {
        // comes back as a List
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareList(expected, actual, this, Varint);
        }
    },

    @SerializedName("map<varchar,varchar>")
    VarcharVarcharMap {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Varchar, Varchar);
        }
    },

    @SerializedName("map<varchar,text>")
    VarcharTextMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Varchar, Text);
        }
    },

    @SerializedName("map<varchar,ascii>")
    VarcharAsciiMap {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Varchar, Ascii);
        }
    },

    @SerializedName("map<text,varchar>")
    TextVarcharMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Text, Varchar);
        }
    },

    @SerializedName("map<text,text>")
    TextTextMap {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Text, Text);
        }
    },

    @SerializedName("map<text,ascii>")
    TextAsciiMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Text, Ascii);
        }
    },

    @SerializedName("map<ascii,varchar>")
    AsciiVarcharMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Ascii, Varchar);
        }
    },

    @SerializedName("map<ascii,text>")
    AsciiTextMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Ascii, Text);
        }
    },

    @SerializedName("map<ascii,ascii>")
    AsciiAsciiMap  {
        @Override
        public boolean equals(Object expected, Object actual) {
            return compareMap(expected, actual, this, Ascii, Ascii);
        }
    },

    @SerializedName("map<bigint,varchar>")
    BigintVarcharMap,
    
    @SerializedName("map<bigint,text>")
    BigintTextMap,
    
    @SerializedName("map<bigint,ascii>")
    BigintAsciiMap
    
    ;

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
                return UUID.fromString(expected.toString()).equals(UUID.fromString(actual.toString()));
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
