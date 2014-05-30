package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class PreparedStatementExecutionTest {

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(PreparedStatementExecution.class).verify();
    }

    @Test
    public void consistencyDefaultsToOne() {
        PreparedStatementExecution query = PreparedStatementExecution.builder().withPreparedStatementText("query").build();
        assertEquals("ONE", query.getConsistency());
    }

    @Test
    public void variableTypesDefaultToEmpty() {
        PreparedStatementExecution query = PreparedStatementExecution.builder().withPreparedStatementText("query").build();
        assertEquals(Collections.emptyList(), query.getVariables());
    }

    @Test(expected = IllegalStateException.class)
    public void preparedStatementTextIsMandatory() {
        PreparedStatementExecution query = PreparedStatementExecution.builder().build();
    }

    @Test
    public void setAllProperties() {
        PreparedStatementExecution query = PreparedStatementExecution.builder()
                .withPreparedStatementText("query")
                .withConsistency("TWO")
                .withVariables("four","five")
                .build();

        assertEquals("TWO", query.getConsistency());
        assertEquals("query", query.getPreparedStatementText());
        assertEquals(Arrays.asList("four", "five"), query.getVariables());
    }

}