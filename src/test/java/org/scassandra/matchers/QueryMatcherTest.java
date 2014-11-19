package org.scassandra.matchers;

import org.junit.Test;
import org.scassandra.http.client.Query;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class QueryMatcherTest {

    @Test
    public void matchesOnQuery() throws Exception {
        Query queryToMatchAgainst = Query.builder()
                .withQuery("the query")
                .build();

        Query queryWithSameText = Query.builder()
                .withQuery("the query")
                .build();

        QueryMatcher underTest = new QueryMatcher(queryToMatchAgainst);

        boolean matched = underTest.matchesSafely(Arrays.asList(queryWithSameText));

        assertTrue(matched);
    }
}