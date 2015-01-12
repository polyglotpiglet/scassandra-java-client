package org.scassandra.http.client;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
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
    public static Collection<Object[]> data() {
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

                {Bigint, 1, 1, MATCH},
                {Bigint, 1l, 1l, MATCH},
                {Bigint, 1l, "1", MATCH},
                {Bigint, "1", "1", MATCH},
                {Bigint, new BigInteger("1"), new BigInteger("1"), MATCH},
                {Bigint, new BigInteger("1"), 1, MATCH},

                {Bigint, new BigInteger("1"), null, NO_MATCH},
                {Bigint, null, new BigInteger("1"), NO_MATCH},
                {Bigint, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {Bigint, "hello", 1, ILLEGAL_ARGUMENT},

                {Int, 1, 1, MATCH},
                {Int, "1", "1", MATCH},
                {Int, "1", 1, MATCH},
                {Int, 1, "1", MATCH},

                {Int, 1, null, NO_MATCH},
                {Int, null, 1, NO_MATCH},
                {Int, new BigInteger("1"), 1, ILLEGAL_ARGUMENT},
                {Int, 1l, 1, ILLEGAL_ARGUMENT},
                {Int, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {Int, "hello", 1, ILLEGAL_ARGUMENT},

                {Varint, "1", "1", MATCH},
                {Varint, new BigInteger("1"), new BigInteger("1"), MATCH},
                {Varint, new BigInteger("1"), 1, MATCH},
                {Varint, "1", 1, MATCH},

                {Varint, "1", null, NO_MATCH},
                {Varint, null, "1", NO_MATCH},
                {Varint, 1, "1", ILLEGAL_ARGUMENT},
                {Varint, 1, 1, ILLEGAL_ARGUMENT},
                {Varint, 1l, 1, ILLEGAL_ARGUMENT},
                {Varint, "hello", new BigInteger("1"), ILLEGAL_ARGUMENT},
                {Varint, "hello", 1, ILLEGAL_ARGUMENT},

                {Boolean, true, true, MATCH},
                {Boolean, false, false, MATCH},

                {Boolean, true, false, NO_MATCH},
                {Boolean, null, false, ILLEGAL_ARGUMENT},
                {Boolean, true, false, NO_MATCH},
                {Boolean, 1, false, ILLEGAL_ARGUMENT},
                {Boolean, "true", false, ILLEGAL_ARGUMENT},
                {Boolean, new BigDecimal("1.2"), false, ILLEGAL_ARGUMENT},
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
                assertFalse(type.equals(expected, actual));
                break;
            }
            case ILLEGAL_ARGUMENT : {
                try {
                    type.equals(expected, actual);
                    fail("Expected Illegal Argument Exception");
                } catch (IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains(type.name()));
                }
                break;
            }
        }
    }
}