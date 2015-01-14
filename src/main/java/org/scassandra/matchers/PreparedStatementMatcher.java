package org.scassandra.matchers;

import org.apache.commons.codec.binary.Hex;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PreparedStatementExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class PreparedStatementMatcher extends TypeSafeMatcher<List<PreparedStatementExecution>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PreparedStatementMatcher.class);

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
        for (int i = 0; i < queries.size(); i++) {
            PreparedStatementExecution actualPreparedStatementExecution = queries.get(i);
            try {
                if (doesPreparedStatementMatch(actualPreparedStatementExecution)) {
                    return true;
                }
            } catch (IllegalArgumentException e) {
                // if it is the last one let this out
                if (i == queries.size() - 1) {
                    throw e;
                } else {
                    LOGGER.info("Found prepared statement execution that didn't match: {}, reason: {}", actualPreparedStatementExecution, e.getMessage());
                }
            }
        }
        return false;
    }

    /*
    The server sends back all floats and doubles as strings to preserve accuracy so we convert the
    actual variable to the expected variables type
     */
    private boolean doesPreparedStatementMatch(PreparedStatementExecution actualPreparedStatementExecution) {

        List<ColumnTypes> variableTypes = actualPreparedStatementExecution.getVariableTypes();
        List<Object> actualVariables = actualPreparedStatementExecution.getVariables();
        if (variableTypes.size() != actualVariables.size()) {
            throw new IllegalArgumentException(String.format("Server has returned a different number of variables to variable types: variables %s variableTypes %s", actualVariables, variableTypes));
        }


        if (!actualPreparedStatementExecution.getConsistency().equals(expectedPreparedStatementExecution.getConsistency()))
            return false;
        if (!actualPreparedStatementExecution.getPreparedStatementText().equals(expectedPreparedStatementExecution.getPreparedStatementText()))
            return false;
        List<Object> expectedVariables = expectedPreparedStatementExecution.getVariables();

        if (expectedVariables.size() != actualVariables.size()) {
            return false;
        }


        for (int index = 0; index < expectedVariables.size(); index++) {

            Object expectedVariable = expectedVariables.get(index);
            Object actualVariable = actualVariables.get(index);
            ColumnTypes columnType = variableTypes.get(index);
            if (!columnType.equals(expectedVariable, actualVariable)) return false;

        }
        return true;
    }
}
