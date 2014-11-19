package org.scassandra.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.scassandra.http.client.Query;

import java.util.List;

class QueryMatcher extends TypeSafeMatcher<List<Query>> {

    private Query query;

    public QueryMatcher(Query query) {
        if (query == null) throw new IllegalArgumentException("null query");
        this.query = query;
    }

    @Override
    protected boolean matchesSafely(List<Query> queries) {
        return queries.contains(this.query);
    }

    @Override
    public void describeMismatchSafely(List<Query> actual, Description description) {
        description.appendText("the following queries were executed: ");
        for (Query query : actual) {
            description.appendText("\n" + query);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected query " + query + " to be executed");
    }
}