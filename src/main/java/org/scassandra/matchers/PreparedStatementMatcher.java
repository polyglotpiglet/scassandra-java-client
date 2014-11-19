package org.scassandra.matchers;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.scassandra.http.client.PreparedStatementExecution;

import java.util.List;

public class PreparedStatementMatcher extends TypeSafeMatcher<List<PreparedStatementExecution>> {

    private PreparedStatementExecution expectedPreparedStatementExecution;

    public PreparedStatementMatcher(PreparedStatementExecution expectedPreparedStatementExecution) {
        if (expectedPreparedStatementExecution == null)
            throw new IllegalArgumentException("null expectedPreparedStatementExecution");
        this.expectedPreparedStatementExecution = expectedPreparedStatementExecution;
    }

    @Override
    public void describeMismatchSafely(List<PreparedStatementExecution> preparedStatementExecutions, Description description) {
        description.appendText("the following prepared statements were executed: ");
        for (PreparedStatementExecution preparedStatement : preparedStatementExecutions) {
            description.appendText("\n" + preparedStatement);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Expected prepared statement " + expectedPreparedStatementExecution + " to be executed");
    }

    @Override
    protected boolean matchesSafely(List<PreparedStatementExecution> queries) {
        for (PreparedStatementExecution query : queries) {
            if (doesPreparedStatementMatch(query)) {
                return true;
            }
        }
        return false;
    }

    private boolean doesPreparedStatementMatch(PreparedStatementExecution actualPreparedStatementExecution) {
        if (!actualPreparedStatementExecution.getConsistency().equals(expectedPreparedStatementExecution.getConsistency()))
            return false;
        if (!actualPreparedStatementExecution.getPreparedStatementText().equals(expectedPreparedStatementExecution.getPreparedStatementText()))
            return false;
        List<Object> expectedVariables = expectedPreparedStatementExecution.getVariables();
        List<Object> actualVariables = actualPreparedStatementExecution.getVariables();

        if (expectedVariables.size() != actualVariables.size()) {
            return false;
        }

        for (int index = 0; index < expectedVariables.size(); index++) {

            Object actualVariable = actualVariables.get(index);
            Object expectedVariable = expectedVariables.get(index);

            if (actualVariable instanceof Double) {
                Double castToDouble;
                try {
                    castToDouble = new Double(expectedVariable.toString());
                } catch (NumberFormatException e) {
                    return false;
                }
                if (!castToDouble.equals(actualVariable)) {
                    return false;
                }
            } else {
                if (!expectedVariable.equals(actualVariable)) {
                    return false;
                }
            }
        }
        return true;
    }
}
