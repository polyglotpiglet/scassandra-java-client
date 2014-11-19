package org.scassandra.matchers;

import org.scassandra.http.client.PreparedStatementExecution;
import org.scassandra.http.client.Query;

public class Matchers {

    public static QueryMatcher containsQuery(Query query) {
        return new QueryMatcher(query);
    }

    public static PreparedStatementMatcher preparedStatementRecorded(PreparedStatementExecution query) {
        return new PreparedStatementMatcher(query);
    }

}
