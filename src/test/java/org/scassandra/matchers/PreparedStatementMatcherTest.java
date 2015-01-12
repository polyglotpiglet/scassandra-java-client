package org.scassandra.matchers;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.scassandra.http.client.PreparedStatementExecution;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreparedStatementMatcherTest {

    //todo blob

    @Test
    public void matchWithJustQuery() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertTrue(matched);
    }

    @Test
    public void mismatchingConsistency() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .withConsistency("QUORUM")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertFalse(matched);
    }

    @Test
    public void mismatchingQueryText() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("some query")
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("some different query")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertFalse(matched);
    }

    @Test
    public void mismatchingStringVariables() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "three")
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "not three!!")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchingStringVariables() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "three")
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("one", "two", "three")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertTrue(matched);
    }

    @Test
    public void machingNumbers() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(new Byte("1"), new Short("2"), 3, 4L)
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(new Byte("1"), new Short("2"), 3, 4L)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingDoubles() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(new Byte("1"), new Short("2"), 3, 4L)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0, 2.0, 3.0, 4.0)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingFloats() throws Exception {
        //given
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(new Byte("1"), new Short("2"), 3, 4L)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0f, 2.0f, 3.0f, 4.0f)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingBlobsWithByteBuffer() throws Exception {
        //given
        byte[] byteArray = new byte[] {1,2,3,4,5,6,7,8,9,10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("0x0102030405060708090a")
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(byteBuffer)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingBlobsWhenNotInActualReturnsFalse() throws Exception {
        //given
        byte[] byteArray = new byte[] {1,2,3,4,5,6,7,8,9,10};
        ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(byteBuffer)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertFalse(matched);
    }

    @Test
    public void inetMatching() throws Exception {
        //given
        InetAddress inetAddress = InetAddress.getLocalHost();
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(inetAddress.getHostAddress())
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(inetAddress)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void decimalMatchingAsDouble() throws Exception {
        //given
        BigDecimal decimal = new BigDecimal(90);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(90.0)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(decimal)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void decimalMatchingAsBigDecimal() throws Exception {
        //given
        BigDecimal decimal = new BigDecimal(90);
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(decimal)
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(decimal)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }

    @Test
    public void numbersMatchingUUIDsWithUUIDClass() throws Exception {
        //given
        UUID uuid = UUID.randomUUID();
        UUID theSame = UUID.fromString(uuid.toString());
        PreparedStatementExecution expected = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(uuid)
                .build();
        PreparedStatementExecution actual = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(theSame)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expected);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actual));

        //then
        assertTrue(matched);
    }
    @Test
    public void numbersMatchingUUIDsWithUUIDAsString() throws Exception {
        //given
        UUID uuid = UUID.randomUUID();
        UUID theSame = UUID.fromString(uuid.toString());
        PreparedStatementExecution expected = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(uuid)
                .build();
        PreparedStatementExecution actual = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(theSame.toString())
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expected);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actual));

        //then
        assertTrue(matched);
    }

    @Test
    public void matchNonNumberAgainstNumberShouldBeFalse() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0)
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables("NaN")
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertFalse(matched);
    }

    @Test
    public void lessVariablesIsFalse() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1,2,3)
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1,2)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertFalse(matched);
    }

    @Test
    public void moreVariablesIsFalse() throws Exception {
        //given
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1,2,3)
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1,2, 3, 4)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchNullAgainstSomethingElseIsFalse() throws Exception {
        //given

        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0)
                .build();
        List<Object> variables = new ArrayList<>();
        variables.add(null);

        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(variables)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(matchAgainst);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(toMatch));

        //then
        assertFalse(matched);
    }

    @Test
    public void matchesDateWhenSentBackAsLong() throws Exception {
        //given
        Date date = new Date();
        PreparedStatementExecution actualExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(date.getTime())
                .build();
        PreparedStatementExecution expectedExecution = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(date)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(expectedExecution);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(actualExecution));

        //then
        assertTrue(matched);
    }
}