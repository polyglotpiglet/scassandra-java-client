package org.scassandra.matchers;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.scassandra.http.client.PreparedStatementExecution;

import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PreparedStatementMatcherTest {
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
        PreparedStatementExecution toMatch = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(new Byte("1"), new Short("2"), 3, 4L)
                .build();
        PreparedStatementExecution matchAgainst = PreparedStatementExecution.builder()
                .withPreparedStatementText("same query")
                .withVariables(1.0, 2.0, 3.0, 4.0)
                .build();

        PreparedStatementMatcher underTest = new PreparedStatementMatcher(toMatch);

        //when
        boolean matched = underTest.matchesSafely(Lists.newArrayList(matchAgainst));

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
}