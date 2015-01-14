package org.scassandra.http.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.scassandra.http.client.ColumnTypes.*;
import static org.scassandra.http.client.ColumnTypesTest.Result.*;

@RunWith(Parameterized.class)
public class ColumnTypesTest {

    enum Result {
        MATCH,
        NO_MATCH,
        ILLEGAL_ARGUMENT
    }

    @Parameterized.Parameters(name = "Type: {0} expected: {1}, actual {2}, should be equal: {3}")
    public static Collection<Object[]> data() throws UnknownHostException {
        return Arrays.asList(new Object[][]{
                {Ascii, "one", "one", MATCH},
                {Ascii, "one", "two", NO_MATCH},
                {Ascii, "one", null, NO_MATCH},
                {Ascii, null, "two", NO_MATCH},
                {Ascii, "one", 5, NO_MATCH},
                {Ascii, "one", UUID.randomUUID(), NO_MATCH},

                {Text, "one", "one", MATCH},
                {Text, "one", "two", NO_MATCH},
                {Text, "one", null, NO_MATCH},
                {Text, null, "two", NO_MATCH},

                {Varchar, "one", "one", MATCH},
                {Varchar, "one", "two", NO_MATCH},
                {Varchar, "one", null, NO_MATCH},
                {Varchar, null, "two", NO_MATCH},

                {Bigint, 1, 1d, MATCH},
                {Bigint, 1l, 1d, MATCH},
                {Bigint, new BigInteger("1"), 1d, MATCH},

                {Bigint, null, 1d, NO_MATCH},
                {Bigint, "hello", 1d, ILLEGAL_ARGUMENT},
                
                {Counter, 1, 1d, MATCH},
                {Counter, 1l, 1d, MATCH},
                {Counter, new BigInteger("1"), 1d, MATCH},

                {Counter, new BigInteger("1"), null, NO_MATCH},
                {Counter, null, new BigInteger("1"), NO_MATCH},
                {Counter, "hello", 1d, ILLEGAL_ARGUMENT},

                {Int, 1, 1d, MATCH},
                {Int, "1", 1d, MATCH},
                {Int, new BigInteger("1"), 1d, MATCH},

                {Int, null, 1, NO_MATCH},
                {Int, "hello", 1d, ILLEGAL_ARGUMENT},

                {Varint, "1", 1d, MATCH},
                {Varint, new BigInteger("1"), 1d, MATCH},

                {Varint, "1", null, NO_MATCH},
                {Varint, null, 1d, NO_MATCH},
                {Varint, 1, 1d, ILLEGAL_ARGUMENT},
                {Varint, 1, 1d, ILLEGAL_ARGUMENT},
                {Varint, 1l, 1d, ILLEGAL_ARGUMENT},
                {Varint, "hello", 1d, ILLEGAL_ARGUMENT},

                {Boolean, true, true, MATCH},
                {Boolean, false, false, MATCH},

                {Boolean, true, false, NO_MATCH},
                {Boolean, null, false, ILLEGAL_ARGUMENT},
                {Boolean, true, false, NO_MATCH},
                {Boolean, 1, false, ILLEGAL_ARGUMENT},
                {Boolean, "true", false, ILLEGAL_ARGUMENT},
                {Boolean, new BigDecimal("1.2"), false, ILLEGAL_ARGUMENT},

                {Blob, "0x0012345435345345435435", "0x0012345435345345435435", MATCH},
                {Blob, ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "0x" + Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), MATCH},

                {Blob, ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), "0x" + Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}), NO_MATCH},
                {Blob, "0x0012345435345345435435", "0x0012345435345345435433", NO_MATCH},
                {Blob, 1, "0x0012345435345345435435", ILLEGAL_ARGUMENT},
                {Blob, new BigInteger("1"), "0x0012345435345345435435", ILLEGAL_ARGUMENT},
                {Blob, new BigDecimal("1"), "0x0012345435345345435435", ILLEGAL_ARGUMENT},
                {Blob, null, "0x0012345435345345435433", ILLEGAL_ARGUMENT},

                {Decimal, "1", "1", MATCH},
                {Decimal, "1.0000", "1", MATCH},
                {Decimal, new BigDecimal("1.0000"), "1", MATCH},

                {Decimal, "1", null, NO_MATCH},
                {Decimal, "1", "2", NO_MATCH},
                {Decimal, new BigInteger("1"), "1.234", ILLEGAL_ARGUMENT},
                {Decimal, null, "1", ILLEGAL_ARGUMENT},
                {Decimal, 1, "1", ILLEGAL_ARGUMENT},
                {Decimal, 1, 1, ILLEGAL_ARGUMENT},
                {Decimal, 1l, 1, ILLEGAL_ARGUMENT},
                {Decimal, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {Decimal, "hello", 1, ILLEGAL_ARGUMENT},
                
                {Float, "1", "1", MATCH},
                {Float, "1.0000", "1", MATCH},

                {Float, "1", null, NO_MATCH},
                {Float, "1", "2", NO_MATCH},
                {Float, new BigInteger("1"), "1.234", ILLEGAL_ARGUMENT},
                {Float, null, "1", ILLEGAL_ARGUMENT},
                {Float, 1, "1", ILLEGAL_ARGUMENT},
                {Float, 1, 1, ILLEGAL_ARGUMENT},
                {Float, 1l, 1, ILLEGAL_ARGUMENT},
                {Float, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {Float, "hello", 1, ILLEGAL_ARGUMENT},
                
                {Double, "1", "1", MATCH},
                {Double, "1.0000", "1", MATCH},

                {Double, "1", null, NO_MATCH},
                {Double, "1", "2", NO_MATCH},
                {Double, new BigInteger("1"), "1.234", ILLEGAL_ARGUMENT},
                {Double, null, "1", ILLEGAL_ARGUMENT},
                {Double, 1, "1", ILLEGAL_ARGUMENT},
                {Double, 1, 1, ILLEGAL_ARGUMENT},
                {Double, 1l, 1, ILLEGAL_ARGUMENT},
                {Double, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {Double, "hello", 1, ILLEGAL_ARGUMENT},

                {Timestamp, 1l, 1d, MATCH},
                {Timestamp, new Date(1l), 1d, MATCH},
                {Timestamp, null, 1d, NO_MATCH},

                {Timestamp, 1l, 12d, NO_MATCH},
                {Timestamp, 1, 1d, ILLEGAL_ARGUMENT},
                {Timestamp, "1", 1d, ILLEGAL_ARGUMENT},
                {Timestamp, new BigInteger("1"), 1d, ILLEGAL_ARGUMENT},
                {Timestamp, "hello", 1d, ILLEGAL_ARGUMENT},

                {Timeuuid, "59ad61d0-c540-11e2-881e-b9e6057626c4", "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},
                {Timeuuid, UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4"), "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},

                {Timeuuid, UUID.randomUUID(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {Timeuuid, UUID.randomUUID().toString(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {Timeuuid, null, "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {Timeuuid, "59ad61d0-c540-11e2-881e-b9e6057626c4", null,  NO_MATCH},

                {Timeuuid, new Date(1l),  "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Timeuuid, 1l, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Timeuuid, 1, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Timeuuid, "1", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Timeuuid, new BigInteger("1"), "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Timeuuid, "hello", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                
                {Uuid, "59ad61d0-c540-11e2-881e-b9e6057626c4", "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},
                {Uuid, UUID.fromString("59ad61d0-c540-11e2-881e-b9e6057626c4"), "59ad61d0-c540-11e2-881e-b9e6057626c4", MATCH},

                {Uuid, UUID.randomUUID(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {Uuid, UUID.randomUUID().toString(), "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {Uuid, null, "59ad61d0-c540-11e2-881e-b9e6057626c4", NO_MATCH},
                {Uuid, "59ad61d0-c540-11e2-881e-b9e6057626c4", null,  NO_MATCH},

                {Uuid, new Date(1l),  "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Uuid, 1l, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Uuid, 1, "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Uuid, "1", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Uuid, new BigInteger("1"), "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},
                {Uuid, "hello", "59ad61d0-c540-11e2-881e-b9e6057626c4", ILLEGAL_ARGUMENT},

                {Inet, InetAddress.getLocalHost(), InetAddress.getLocalHost().getHostAddress(), MATCH},
                {Inet, InetAddress.getLocalHost().getHostAddress(), InetAddress.getLocalHost().getHostAddress(), MATCH},
                
                {Inet, InetAddress.getLocalHost(), "192.168.56.56", NO_MATCH},
                {Inet, InetAddress.getLocalHost().getHostAddress(), "192.168.56.56", NO_MATCH},

                {Inet, null, InetAddress.getLocalHost().getHostAddress(), NO_MATCH},
                {Inet, InetAddress.getLocalHost().getHostAddress(), null,  NO_MATCH},

                {Inet, new Date(1l),  InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {Inet, 1l, InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {Inet, 1, InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                {Inet, new BigInteger("1"), InetAddress.getLocalHost().getHostAddress(), ILLEGAL_ARGUMENT},
                
                {TextSet, Sets.newHashSet("one"), Lists.newArrayList("one"), MATCH},
                {TextSet, Sets.newHashSet("one"), Lists.newArrayList("two"), NO_MATCH},
                {TextSet, Sets.newHashSet("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {TextSet, null, Lists.newArrayList("one", "two"), NO_MATCH},
                {TextSet, Sets.newHashSet("one"), null, NO_MATCH},

                {TextSet, new Date(1l),  Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {TextSet, 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {TextSet, 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {TextSet, new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},

                {AsciiSet, Sets.newHashSet("one"), Lists.newArrayList("one"), MATCH},
                {AsciiSet, Sets.newHashSet("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {AsciiSet, null, Lists.newArrayList("one", "two"), NO_MATCH},
                {AsciiSet, Sets.newHashSet("one"), null, NO_MATCH},

                {AsciiSet, new Date(1l),  Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {AsciiSet, 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {AsciiSet, 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {AsciiSet, new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                
                {VarcharSet, Sets.newHashSet("one"), Lists.newArrayList("one"), MATCH},
                {VarcharSet, Sets.newHashSet("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {VarcharSet, null, Lists.newArrayList("one", "two"), NO_MATCH},
                {VarcharSet, Sets.newHashSet("one"), null, NO_MATCH},

                {VarcharSet, new Date(1l),  Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {VarcharSet, 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {VarcharSet, 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {VarcharSet, new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},

                {UuidSet, Sets.newHashSet(UUID.randomUUID()), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), NO_MATCH},
                {UuidSet, Sets.newHashSet(UUID.randomUUID().toString()), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), NO_MATCH},
                {UuidSet, Sets.newHashSet((UUID) null), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), NO_MATCH},
                {UuidSet, Sets.newHashSet("59ad61d0-c540-11e2-881e-b9e6057626c4"), Lists.newArrayList((UUID) null),  NO_MATCH},

                {UuidSet, new Date(1l),  Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {UuidSet, 1l, Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {UuidSet, 1, Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {UuidSet, "1", Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {UuidSet, new BigInteger("1"), Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},
                {UuidSet, "hello", Lists.newArrayList("59ad61d0-c540-11e2-881e-b9e6057626c4"), ILLEGAL_ARGUMENT},

                {TextList, Lists.newArrayList("one"), Lists.newArrayList("one"), MATCH},
                {TextList, Lists.newArrayList("one"), Lists.newArrayList("two"), NO_MATCH},
                {TextList, Lists.newArrayList("one", "two"), Lists.newArrayList("one", "two"), MATCH},
                {TextList, Lists.newArrayList("one", "two"), Lists.newArrayList("two", "one"), NO_MATCH},
                {TextList, Lists.newArrayList("one"), Lists.newArrayList("one", "two"), NO_MATCH},
                {TextList, null, Lists.newArrayList("one", "two"), NO_MATCH},
                {TextList, Lists.newArrayList("one"), null, NO_MATCH},

                {TextList, new Date(1l),  Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {TextList, 1l, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {TextList, 1, Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
                {TextList, new BigInteger("1"), Lists.newArrayList("one"), ILLEGAL_ARGUMENT},
        });
    }

    private ColumnTypes type;
    private Object expected;
    private Object actual;
    private Result match;

    public ColumnTypesTest(ColumnTypes type, Object expected, Object actual, Result match) {
        this.type = type;
        this.expected = expected;
        this.actual = actual;
        this.match = match;
    }

    @Test
    public void test() throws Exception {
        switch(match) {
            case MATCH : {
                assertTrue(type.equals(expected, actual));
                break;
            }
            case NO_MATCH : {
                boolean equals = type.equals(expected, actual);
                assertFalse("Expected no match but got: " + equals, equals);
                break;
            }
            case ILLEGAL_ARGUMENT : {
                try {
                    boolean equals = type.equals(expected, actual);
                    fail("Expected Illegal Argument Exception, actual: " + equals);
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains(type.name()));
                }
                break;
            }
        }
    }
}