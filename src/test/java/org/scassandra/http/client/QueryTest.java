package org.scassandra.http.client;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QueryTest {

    @Test
    public void testEqualsContract() {
        EqualsVerifier.forClass(Query.class).verify();
    }

    @Test
    public void consistencyDefaultsToOne() {
        Query query = Query.builder().withQuery("query").build();
        assertEquals("ONE", query.getConsistency());
    }
}
